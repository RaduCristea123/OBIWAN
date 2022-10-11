from ...licel import LicelLidarMeasurement
from . import lilas_netcdf_parameters


class LilasLidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = lilas_netcdf_parameters

    def set_PT(self):
        ''' Sets the pressure and temperature at station level .
        The results are stored in the info dictionary.        
        '''
    
        self.info['Temperature'] = 25.0
        self.info['Pressure'] = 1020.0