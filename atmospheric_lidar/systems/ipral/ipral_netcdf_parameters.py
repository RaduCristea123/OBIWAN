# INSERT HERE THE SYSTEM PARAMETERS
general_parameters = \
    {'System': '\'IPRAL\'',
     'Laser_Pointing_Angle': 0,
     'Molecular_Calc': 0,  # Use US standard atmosphere
     'Latitude_degrees_north': 50.63,
     'Longitude_degrees_east': 3.07,
     'Altitude_meter_asl': 0.4,
     'Call sign': 'mb',}

# LINK YOUR LICEL CHANNELS TO SCC PARAMETERS. USE BT0, BC0 ETC AS NAMES (AS IN LICEL FILES).
channel_parameters = \
    {'BT0': {'channel_ID': 41,
             'Background_Low': 19000,
             'Background_High': 20000,
             'Laser_Shots': 1000,
             'LR_Input': 1,
             'DAQ_Range': 500.0,},
     'BC0': {'channel_ID': 42,
             'Background_Low': 19000,
             'Background_High': 20000,
             'Laser_Shots': 1000,
             'LR_Input': 1,
             'DAQ_Range': 0,},
     'BT1': {'channel_ID': 41,
             'Background_Low': 19000,
             'Background_High': 20000,
             'Laser_Shots': 1000,
             'LR_Input': 1,
             'DAQ_Range': 500.0,},
     'BC1': {'channel_ID': 42,
             'Background_Low': 19000,
             'Background_High': 20000,
             'Laser_Shots': 1000,
             'LR_Input': 1,
             'DAQ_Range': 0,},
     }
