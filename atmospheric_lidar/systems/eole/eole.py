from ...licel import LicelLidarMeasurement
from . import eole_netcdf_parameters


class EoleLidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = eole_netcdf_parameters

    def set_PT(self):
        ''' Sets the pressure and temperature at station level .
        The results are stored in the info dictionary.        
        '''
    
        self.info['Temperature'] = 25.0
        self.info['Pressure'] = 1020.0
    
           
    #def save_netcdf_extra(self, f):
    #    CHARMEX CLOUD MIN ALTITUDE 
    #    temp_v=f.createVariable('max_altitude_m_asl', 'd', ('time', 'nb_of_time_scales'))
