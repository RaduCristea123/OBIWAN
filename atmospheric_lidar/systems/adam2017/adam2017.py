from ...licel import LicelLidarMeasurement

from . import adam2017_netcdf_parameters

class ADAM2017LidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = adam2017_netcdf_parameters

    def _get_scc_channel_variables(self):
        channel_variables = \
            {'Background_Low': (('channels',), 'd'),
             'Background_High': (('channels',), 'd'),
             'LR_Input': (('channels',), 'i'),
             'DAQ_Range': (('channels',), 'd'),
             'First_Signal_Rangebin': (('channels',), 'i')
             }
        return channel_variables
