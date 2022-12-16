from time import sleep
from unittest import TestCase

import pandas as pd

from ong_esios.esios_api import EsiosApi
from ong_utils import OngTimer, LOCAL_TZ


class TesteSiosApi(TestCase):
    timer = OngTimer()

    @classmethod
    def setUpClass(cls) -> None:
        cls.esios = EsiosApi()
        cls.today = pd.Timestamp.today().normalize().tz_localize(LOCAL_TZ)
        cls.date_95d_ago = cls.today - pd.offsets.Day(95)
        cls.yesterday = cls.today - pd.offsets.Day(1)

    def test_list_indicators(self):
        """Prints id and name of all (indicators+archives+archives_json).
        for an eSiosApi instance initializing indicators and without initialization.
        With initialization this process must be faster
        """
        esios_indicators = EsiosApi(initialize_indicators=True)
        self.esios = EsiosApi(initialize_indicators=False)
        sleep(5)  # <- So the thread can do its job
        with_indicators = "Esios with indicators"
        without_indicators = "Esios without indicators"
        self.timer.tic(with_indicators)
        print(esios_indicators.list_indicators().head())
        self.timer.toc(with_indicators)
        self.timer.tic(without_indicators)
        print(self.esios.list_indicators().head())
        self.timer.toc(without_indicators)
        self.assertGreater(self.timer.elapsed(without_indicators), self.timer.elapsed((with_indicators)),
                           "Pre initialization of indicators was slower than no initialize")

    def test_get_id_by_name(self):
        """Tests that get_id_by_name works properly checking against some expected output values"""
        # Force refresh so zip files can be found (I3DIA, I90DIA...)
        df_archives = self.esios.list_archives(self.date_95d_ago)  # Refresh list of archives for current as_of date
        expected_output = {
            "SujetosMercado": 83,
            "UnidadesProgramacion": 82,
            "I90DIA": 34,
        }
        for name, id in expected_output.items():
            output_id = self.esios.get_id_by_name(name)
            self.assertEqual(id, output_id,
                             f"Expected id={id} for {name}, received {output_id}")

    def test_download_by_structural_data(self):
        date = None
        # Some JS files
        js = dict()
        for name in ("SujetosMercado", "UnidadesProgramacion"):
            js[name] = self.esios.download_by(name=name, date=date)
            self.assertIsNotNone(js[name], f"Returned a None value for {name}")
            self.assertGreater(len(js[name]), 0, f"Returned an empty value for {name}")
            print(js[name])

    def test_get_p48_agent(self):
        """Test getting the p48 of only units belonging to a certain agent"""
        df_up_sm = self.esios.get_up_sm()
        print(df_up_sm)
        self.assertFalse(df_up_sm.empty, "Joined DataFrame is empty!")
        test_agent = "ENDESA"
        test_units = df_up_sm[df_up_sm.Nombre.str.contains(test_agent)]['Código de UP']
        p48cierre_id = self.esios.get_id_by_name("p48cierre")
        p48 = self.esios.get_esios_program(p48cierre_id, date=self.yesterday)
        p48_filtered = p48.filter(items=test_units)
        print(p48_filtered)
        self.assertFalse(p48_filtered.empty, f"No units found for {test_agent} for date {self.yesterday}")

    def test_download_by_zip_file(self):
        """Test download of zip files (I90DIA, I3DIA, IMES)"""
        # Some ZIP files for 95 days ago
        df_archives = self.esios.list_archives(self.date_95d_ago)  # Refresh list of archives for current as_of date
        for name in ("I90DIA", "I3DIA", "IMES"):
            js = self.esios.download_by(name=name, date=self.date_95d_ago)
            self.assertIsNotNone(js, f"Returned a None value for {name}")
            self.assertGreater(len(js), 0, f"Returned an empty value for {name}")
            print(js.keys())

    def test_get_esios_program(self):
        """Test if p48cierre can be downloaded"""
        date = self.yesterday
        p48cierre_id = self.esios.get_id_by_name("p48cierre")
        df = self.esios.get_esios_program(p48cierre_id, date=date)
        self.assertIsNotNone(df, "Received a None response")
        self.assertFalse(df.empty, "Received an empty dataframe as response")
        print(df)

    def test_precio_spot(self):
        """Downloads price of spot market (OMIE's Mercado Diario)"""

        # indics = self.esios.list_indicators()
        # precios = indics[indics.name.str.contains("Precio")]
        # precios = precios[~precios.name.str.contains("medio")]
        # id = self.esios.get_id_by_name("Precio mercado SPOT Diario")    # Should be 600
        id = 600  # Precio mercado SPOT Diario
        # df = self.esios.download_by(name="Precio mercado SPOT Diario", date=self.yesterday)
        df = self.esios.download_by(id=id, date=self.yesterday)
        print(df)

        id = 668  # Precio Gestión de Desvíos a Subir
        df = self.esios.download_by(id=id, date=self.yesterday)
        print(df)

        id = 614  # PPrecio mercado SPOT Intradiario Sesión 3'
        df = self.esios.download_by(id=id, date=self.yesterday - pd.offsets.Day(5))
        print(df)

    def test_get_esios_program_aggregated(self):
        """Test if p48cierre can be downloaded and aggregated daily"""
        date = pd.Timestamp.now(tz=LOCAL_TZ).normalize() + pd.offsets.Day(-2)

        p48cierre_id = self.esios.get_id_by_name("p48cierre")
        df = self.esios.get_esios_program(p48cierre_id, date=date, aggregate_daily=True)
        self.assertIsNotNone(df, "Received a None response")
        self.assertFalse(df.empty, "Received an empty dataframe as response")
        print(df)

    def test_pvpc(self):
        """Tesf of downloading pvpc data"""
        date = pd.Timestamp.today().normalize()
        pvpc_json = self.esios.download_by(id=80, date=date)
        if pvpc_json:
            dates = pvpc_json['dates']
            values = pvpc_json['values']
            print(values.keys())
        date = pd.Timestamp(2014, 6, 1)
        pvpc_json = self.esios.download_by(name="pvpcdesglosehorario", date=date)
        if pvpc_json:
            dates = pvpc_json['dates']
            values = pvpc_json['values']
            #    print(values)
            print(values.keys())
        print("done")

    def test_analysis_ancillary_services(self):
        """Calculates per agent quotas of ancillary services markets"""

        start_t = self.date_95d_ago
        end_t = self.date_95d_ago + pd.offsets.Day(2)

        df_archives = self.esios.list_archives(self.date_95d_ago)  # Refresh list of archives for current as_of date

        df_up_sujeto = self.esios.get_up_sm()
        mercados = ['Resultado de la Programación Horaria del Mercado de Secundaria. Valores horarios de la asignación de Banda de Regulación Secundaria',
                    'Resultado de la Programación Horaria del Mercado de Terciaria. Valores horarios de la energía asignada en el Mercado de Terciaria',
                    'desvios', 'reserva_potencia']
        asignaciones = {}
        i90_id = self.esios.get_id_by_name("I90DIA")
        for idx, date in enumerate(pd.date_range(start_t, end_t, freq="D")):
            I90DIA = self.esios.download_by(id=i90_id, date=date)
            if I90DIA is not None:
                for mercado, df_data in I90DIA.items():
                    if mercado not in mercados:
                        continue
                    if not df_data.empty:
                        df_data = df_data.loc[:, ('Unidad de Programación', 'Total')]
                        df_data['index'] = date
                        df_data = df_data.set_index('index')
                        df_data.Total = abs(df_data.Total)
                        if asignaciones.get(mercado, None) is None:
                            asignaciones[mercado] = df_data
                        else:
                            asignaciones[mercado] = asignaciones[mercado].append(df_data)
        for mercado in mercados:
            if mercado not in asignaciones:
                continue
            df_mercado = asignaciones[mercado]
            df_mercado_agente = pd.merge(left=df_mercado, right=df_up_sujeto, left_on="Unidad de Programación",
                                         right_on="Código de UP",
                                         how="left")
            df_cuotas_agente = df_mercado_agente.groupby("Nombre").sum().apply(
                lambda x: 100 * x / float(x.sum())).sort_values("Total")
            print("*" * 50)
            print("Cuotas mercado {}".format(mercado))
            # Prints just over 5%
            print(df_cuotas_agente)
            print("*" * 50)
