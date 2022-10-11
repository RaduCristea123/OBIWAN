from ...licel import LicelLidarMeasurement
from . import ipral_netcdf_parameters


class IpralLidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = ipral_netcdf_parameters

    def __init__(self, file_list=None, use_id_as_name=True):
        super(IpralLidarMeasurement, self).__init__(file_list, use_id_as_name)

    def set_PT(self):
        ''' Sets the pressure and temperature at station level .
        The results are stored in the info dictionary.        
        '''
    
        self.info['Temperature'] = 25.0
        self.info['Pressure'] = 1020.0