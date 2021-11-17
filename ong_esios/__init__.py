from ong_utils import OngConfig, LOCAL_TZ, http

_util = OngConfig("ong_esios", cfg_filename="ong_config.yml")
logger = _util.logger
config = _util.config
