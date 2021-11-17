import time

import numpy as np
import ujson
from pprint import pprint
from ong_esios import config, logger, http, LOCAL_TZ
import pandas as pd
import xml.etree.ElementTree as ET
from io import BytesIO          # Process I90DIA
from zipfile import ZipFile     # Process I90DIA
from threading import Thread
from ong_utils import OngTimer

timer = OngTimer()


def parse_zip_file(req) -> dict:
    """Parses zip files of I90DIA, I3DIA and IMES. Returns a dict with description of sheet as key
    and the data read into a pandas Dataframe as values"""
    myzipfile = ZipFile(BytesIO(req.data))
    for name in myzipfile.namelist():
        if any(name.startswith(pattern) for pattern in ("I90DIA", "I3DIA", "IMES")):
            xls = pd.ExcelFile(myzipfile.open(name))
            contents = xls.parse(xls.sheet_names[0], header=9).iloc[:, :2]
            retval = {id: pd.read_excel(xls, sheet_name=sheet_name, skiprows=2)
                      for sheet_name, id in contents.values if id != "Reservada"}
            return retval
        else:
            logger.error(f"Could not parse zip file (not implemented): {name}")
    return None


class EsiosXmlParser():

    def __init__(self, data):
        self.tree = ET.fromstring(data)
        ns = self.tree.tag[0:self.tree.tag.find("}") + 1]
        self.ns = {"": ns[1:-1]}
        file_type = ns.split("/")[-2]
        horizonte = self.tree.find("Horizonte", self.ns).attrib['v']
        fecha_ini, fecha_fin = map(pd.Timestamp, horizonte.split("/"))
        # if file_type == "P48-esios-MP":
        #     fecha_fin = fecha_fin + pd.offsets.Day(1)       # P48 includes 2 days
        self.dates = pd.date_range(fecha_ini, fecha_fin, freq="H", closed="left")
        self.values = {}

    def parse_pvpc(self):
        for serie in self.tree.findall("SeriesTemporales", self.ns):
            metric = list()
            # metric.append(serie.find(ns + "IdentificacionSeriesTemporales").attrib['v'])
            tipo_precio = serie.find("TipoPrecio", self.ns)
            termino_coste = serie.find("TerminoCosteHorario", self.ns)
            if termino_coste is None or tipo_precio is None or termino_coste.attrib['v'] not in ('TCUh', 'FEU'):
                continue
            metric.append(tipo_precio.attrib['v'])
            metric.append(termino_coste.attrib['v'])
            metric = "_".join(metric)
            periodo = serie.find("Periodo", self.ns)
            intervalos = periodo.findall("Intervalo", self.ns)
            self.values[metric] = [float(intervalo.find("Ctd", self.ns).attrib['v']) * 1000 for intervalo in
                              intervalos]
        return self.dates, self.values

    def parse_program(self, aggregate_daily=False):
        """
        Parses an esios program file.
        :param aggregate_daily: if True, aggregates data at daily level, otherwise hourly data is returned
        :return: a pandas DataFrame, with dates (in LOCAL_TZ) as indexes and Programing Units as columns
        """

        series_temporales = self.tree.findall("SeriesTemporales", self.ns)
        up_list = [list(serie)[2].get('v') for serie in series_temporales]
        df = pd.DataFrame(0.0, columns=set(up_list), index=self.dates[0::24] if aggregate_daily else self.dates)
        # Convert dates to LOCAL_TZ
        df = df.reindex(df.index.tz_convert(LOCAL_TZ))
        arr = np.zeros(df.shape)

        for unidad_programacion, serie in zip(up_list, series_temporales):
            up_elem = list(serie)[2]
            if up_elem.tag.endswith("UPEntrada"):  # Generator
                signo = 1
            elif up_elem.tag.endswith("UPSalida"):  # Demand
                signo = -1
            else:
                raise Exception("Unknown tag {}".format(up_elem.tag))
            periodo = serie.find("Periodo", self.ns)
            txt_periodo = periodo.find("IntervaloTiempo", self.ns).get('v').split("/")[0]  # Start of the interval
            period_interval = pd.Timestamp(txt_periodo)
            intervalos = periodo.findall("Intervalo", self.ns)
            base_position = self.dates.get_loc(period_interval)
            values = [signo * float(intervalo.find("Ctd", self.ns).attrib['v']) for intervalo in intervalos]
            if aggregate_daily:
                index = 0 if base_position == 0 else 1
                arr[index, df.columns.get_loc(unidad_programacion)] = sum(values)
            else:
                index = [int(intervalo.find("Pos", self.ns).attrib['v']) - 1 + base_position for intervalo in intervalos]
                arr[index, df.columns.get_loc(unidad_programacion)] = values

        df[:] = arr
        return df


def parse_xml_pvpc(data: str) -> dict:
    """
    Parses a xml response for PVPC. From xml looks for "SeriesTemporales" branch, from them look for
    those having both "TipoPrecio" field and "TerminoCosteHorario" (being either TCUh or FEU)
    :param data: request.text from a requests query
    :return: a json (dict)
    """
    parser = EsiosXmlParser(data)
    retval = parser.parse_pvpc()
    return retval


def parse_esios_p48(data: str, aggregate_daily: bool = False) -> pd.DataFrame:
    parser = EsiosXmlParser(data)
    retval = parser.parse_program(aggregate_daily)
    return retval


class EsiosApi:
    def __init__(self, token=config('esios_token'), initialize_indicators=False):
        """
        Inits esios api class
        :param token: the token to obtain from https://api.esios.ree.es, read from config file
        :param initialize_indicators: if True a thread is Launched to refresh indicators in __init__ (as it takes 40s)
        Otherwise (default) thread is not launched and indicators will be initialized when needed
        """
        self.esios_base_url = config("esios_base_url", "https://api.esios.ree.es")
        """
         curl "https://api.esios.ree.es/archives" -X GET -H "Accept: application/json; application/vnd.esios-api-v1+json" 
         -H "Content-Type: application/json" 
         -H "Host: api.esios.ree.es" 
         -H "Authorization: Token token=\"a4ff9719b840a21e0e521ae3df41df0a16b55afd5f77a12e1a54c3c7c6c88b3b\"" 
         -H "Cookie: "
        """
        self.headers = {"Accept": "application/json; application/vnd.ong_esios-api-v1+json",
                     "Content-Type": "application/json",
                     "Host": "api.esios.ree.es",
                     f"Authorization": f'Token token="{token}"',
                     "Cookie": ""}

        # Launch a thread to update indicators in background. It takes around 30 seconds
        self.df_indicators = None
        self.thread_indicators = None
        if initialize_indicators:
            t = Thread(target=self.list_indicators, daemon=True)
            t.start()
            self.thread_indicators = t

        self.df_archives = None
        self.df_archives_json = None

    def __get_list(self, url, debug=False, as_of_date=None):
        """
        Returns a json with the list of indicators/archives
        :param url: /indicators, /archives, etc
        :param debug: True to print data
        :param as_of_date: a datetime in case list m
        :return: a json with the list of files/indicators
        """
        query_params = f"?date={as_of_date.isoformat()}" if as_of_date else ""
        req = http.request("get", self.esios_base_url + url + query_params,
                           headers=self.headers)
        js = ujson.loads(req.data)
        js_retval = js[list(js.keys())[0]]
        if debug:
            for d in js_retval:
                name = d['name']
                id = d['id']
                archive_type = d.get("archive_type")
                print(u"'{}' # {},{}".format(id, archive_type, name))
        return js_retval

    def __get_list_df(self, url, debug=False, as_of_date=None):
        """Same as __get_list but returning a pd.DataFrame"""
        js = self.__get_list(url, debug, as_of_date)
        df = pd.DataFrame(js)
        if "id" in df.columns:
            df = df.set_index("id", drop=False)     # keep id in the columns
        return df

    @property
    def dfs(self):
        """Returns a list of DataFrames of downloadable files"""
        for f in self.list_archives_json, self.list_archives, self.list_indicators:
            yield f()

    def print_id_name(self):
        """Prints name, id and archive type for indicators, archives and json archives"""
        for df in self.dfs:
            cols = (c for c in df.columns if c in ('id', 'name', 'archive_type'))
            print(df.loc[:, cols])

    def list_archives(self, date=None):
        """Returns list of archives, saving list in memory for faster access"""
        if self.df_archives is None or date is not None:
            self.df_archives = self.__get_list_df("/archives", as_of_date=date)
            # print(f"{self.df_archives.columns=}")
            self.df_archives['url'] = "/archives"
        return self.df_archives

    def list_indicators(self):
        """Returns list of indicators, saving list in memory for faster access"""
        # If thread launched in __init__ is working, wait until it finishes
        if self.thread_indicators is not None:
            self.thread_indicators.join()
            self.thread_indicators = None
        if self.df_indicators is None:
            logger.info("Downloading indicators...")
            self.df_indicators = self.__get_list_df("/indicators")
            # print(f"{self.df_indicators.columns=}")
            self.df_indicators['url'] = "/indicators"
            logger.info("Indicators downloaded")
        return self.df_indicators

    def list_archives_json(self):
        """Returns list of json files, saving list in memory for faster access"""
        if self.df_archives_json is None:
            self.df_archives_json = self.__get_list_df("/archives_json")
            # print(f"{self.df_archives_json.columns=}")
            self.df_archives_json['url'] = "/archives_json"
        return self.df_archives_json

    def get_id_by_name(self, name):
        """Gets id of a download"""
        for df in self.dfs:
            if name in df.name.values:
                return df.index[df.name == name][0]
        raise ValueError("{name} not found".format(name=name))

    def is_indicator(self, id):
        """Returns true if name is an indicator so it is in df_indicators"""
        for df in self.list_archives(), self.list_archives_json():
            if id in df.id.values:
                return False
        return True

    def download_by(self, id=None, name=None, date=None, parser=None, **kwargs):
        """Downloads a file by either its id or its name for a given date"""
        id = id or self.get_id_by_name(name)
        return self.download("/archives", id, date, parser, self.is_indicator(id), **kwargs)

    def __request_esios(self, url, id, date, is_indicator=False):
        """Performs a request and returns req"""
        if date:
            fields = {"date": date.isoformat()}
        else:
            fields = None
        if url.startswith("/"):
            url = url[1:]
        if is_indicator:
            download_url = f"{self.esios_base_url}/indicators/{id}"
        else:
            download_url = f"{self.esios_base_url}/{url}/{id}/download"
        req = http.request("get", download_url, headers=self.headers, fields=fields)
        return req

    def download(self, url: str, id: int, date, parser=None, is_indicator=False, **kwargs):
        """
        Returns a json of the data for an indicator in a specific date
        :param url: either "archives", "indicators" or "archives_json"
        :param id: number of indicator
        :param date: date for reading. Must be first hour of day in local timezone
        :param parser: a function that receives request.data and parses result. If None, it will be based
        on request content_type. If json, the parsed json will be returned. If xml it will be parsed
        with "parse_xml_pvpc" and if content-type=="zip" with parse_zip_file. If not None,
        kwargs will be passed to this function
        :param is_indicator: True if it is an indicator, False (default) otherwise
        :return: a json or plain response if response cannot be parsed to json
        """
        req = self.__request_esios(url, id, date, is_indicator)
        content_type = req.headers['Content-Type'].split(";")[0]
        if parser:
            return parser(req.data, **kwargs)
        if content_type == "application/json":
            js_req = ujson.loads(req.data)
            if "message" in js_req.keys():
                logger.error(f"Error in request {url=} {id=} {date=} {js_req=}")
                return None     # There was an error
            if not is_indicator:
                return js_req
            else:   # Process indicator
                df = pd.DataFrame()
                for value in js_req['indicator']['values']:
                    df.loc[pd.Timestamp(value['datetime_utc']).tz_convert(LOCAL_TZ), value['geo_name']] = value['value']
                # df = df.reindex(df.index.tz_convert(LOCAL_TZ))
                return df.copy()
        elif content_type == "xml":
            dates, values = parse_xml_pvpc(req.data)
            return dict(dates=dates, values=values)
        elif content_type == "zip":
            return parse_zip_file(req)
        else:
            logger.error(f"Error in request {url=} {id=} {date=} {req.status=} {content_type=}")
            logger.debug(req.data)
            return None

    def get_esios_program(self, id=None, name=None, date=None, aggregate_daily=False):
        """
            Processes data from an esios XML file that includes SeriesTemporales data
            returns:
                d: dates
                v:dictionary indexed by metric name
        """
        return self.download_by(id, name, date, parse_esios_p48, aggregate_daily=aggregate_daily)

    def get_up_sm(self, date=None):
        """Returns a DataFrame with the join of UnidadesProgramacion and SujetosMercado, os UP can be linked to
        its owner"""
        # Join UnidadesProgramacion and SujetosMercado in a single df
        df_up = pd.DataFrame(next(iter(self.download_by(name="UnidadesProgramacion", date=date).values())))
        df_sujetos = pd.DataFrame(next(iter(self.download_by(name='SujetosMercado', date=date).values())))
        df_up_sujeto = pd.merge(left=df_up, right=df_sujetos, left_on='Sujeto del Mercado',
                                right_on='Código de sujeto').loc[:, ('Código de UP', 'Nombre')]
        return df_up_sujeto


if __name__ == '__main__':
    # esios = eSiosApi(background_initialize_indicators=True)   # Works ok
    # esios.print_id_name()                                     # Works OK
    esios = EsiosApi()
    print(esios.get_id_by_name("SujetosMercado"))               # Works ok
    print(esios.get_id_by_name("UnidadesProgramacion"))         # Works ok

    # Test of SujetosMercado and UnidadesProgramacion only works with date=None
    date = pd.Timestamp.now(tz=LOCAL_TZ).normalize()
    date = None
    df_sujetos = esios.download_by(name="SujetosMercado", date=date)
    print(df_sujetos)
    df_up = esios.download_by(name="UnidadesProgramacion", date=date)
    print(df_up)

    date = pd.Timestamp(2021, 6, 1).tz_localize(LOCAL_TZ)
    I90DIA = esios.download_by(id=34, date=date)
    print(I90DIA)

    # pprint(esios.list_indicators())
    pprint(esios.list_archives())
    pprint(esios.list_archives_json())
    print(esios.download("indicator", 1001, pd.Timestamp.today()))
    date = pd.Timestamp.today().normalize()
    pvpc_json = esios.download("archives", 80, date)
    if pvpc_json:
        dates = pvpc_json['dates']
        values = pvpc_json['values']
        print(values.keys())
    date = pd.Timestamp(2014, 6, 1)
    pvpc_json = esios.download("archives", 80, date)
    if pvpc_json:
        dates = pvpc_json['dates']
        values = pvpc_json['values']
    #    print(values)
        print(values.keys())
    print("done")
