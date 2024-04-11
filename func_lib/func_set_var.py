from data_source.data_source import DataSource
from client_lib import c_log
from configs import vars as vr


# 设置全局log
async def set_var_log(stt, log_dir='./log/'):
    if vr.log != {}:
        return vr.log
    vr.log = c_log.Logs(log_dir=f'{log_dir}{stt}', name=stt, default_extra_name='main')


# 设置全局 data_source
async def set_data_source():
    if vr.data_source != {}:
        return vr.data_source
    vr.data_source = DataSource()
    return vr.data_source
