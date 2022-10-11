from ...licel_depol import LicelCalibrationMeasurement
from . import adam2017_depolarization_parameters

class ADAM2017CalibrationMeasurement(LicelCalibrationMeasurement):
    extra_netcdf_parameters = adam2017_depolarization_parameters
