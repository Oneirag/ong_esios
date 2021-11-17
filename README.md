# ong_esios
Simple functions to read data from esios api ([https://api.esios.ree.es/](https://api.esios.ree.es/)).
A token is needed to be obtained from the previous URL for this to work.
Package `ong_utils` is used to read token from a config file, it can be downloaded from github in 
[https://github.com/Oneirag/ong_utils.git](https://github.com/Oneirag/ong_utils.git)

Sample uses cases can be found in tests

REE files can be either archives (such as I90DIA files), archives_json (such as SujetosMercado) 
or indicators (such as "Precio Mercado SPOT").
Samples of downloading them for a specific date are:

```Python
from ong_esios.esios_api import EsiosApi, LOCAL_TZ
import pandas as pd

# Initializing
esios = EsiosApi()  # token is read from config("esios_token") key
esios = EsiosApi(token="your token here")   # For specific token

date = pd.Timestamp(2020, 10, 10, tzinfo=LOCAL_TZ)
# Download a js archive (as a dict)
js = esios.download_by(name="UnidadesProgramacion", date=date)

date = pd.Timestamp(2020, 10, 10, tzinfo=LOCAL_TZ)
# Download a I90DIA zip file. These files are not available until 90 days after
# delivery, so we have to update list of files before trying to download
esios.list_archives(date)       # Refresh list of files available for this date
i90dia = esios.download_by(name="I90DIA", date=date)        # i90dia is a dict of DataFrames indexed by market names

# Download an indicator. There are *many* indicators making the request of list of indicators to
# esios website quite slow (>20s). Therefore is faster to hardcode id 
id = esios.get_id_by_name("Precio mercado SPOT Diario")     # Slow, over 20secs (first request)
id = 600        # Faster, just hardcode indicator id 
df = esios.download_by(id=id, date=date)        # A dataframe with markets as columns

# Download a p48cierre file. As it is a program needs to call a special method
df = esios.get_esios_program(name="p48cierre", date=date)

# Download components of PVPC price
pvpc_json = esios.download_by(name="pvpcdesglosehorario", date=date)
```

