""" Sample script to convert licel files to SCC netcdf format.

The script assumes the following things:

1. You have already installed the atmospheric_lidar module (e.g. using pip).
2. You have create a class in a file "ipral" describing your system (described the readme file).
   If you want to use it for a different system you need to change the script to use your own class

Run the script using: python convert_ipral.py <your options>

Examples
--------
# This will read all files starting with "l" from "my_dir".
# It will guess the measurement ID based on the date of the files, and will assume measurement number 00.
# For example, the new measurment id could be 20120101mb00
python convert_ipral.py my_dir l*.

# This will use the measurement id you defined.
python convert_ipral.py my_dir l*. -m 20120204mb32  # It will create the file 20120204mb32.nc

Help string
-----------
# To get this, run: python convert_ipral.py -h
usage: convert_ipral.py [-h] [-m MEASUREMENT_ID] [-n MEASUREMENT_NUMBER]
                        [directory] [searchstring]

positional arguments:
  directory             Directory with licel files.
  searchstring          Processing system id.

optional arguments:
  -h, --help            show this help message and exit
  -m MEASUREMENT_ID, --measurement_id MEASUREMENT_ID
                        The new measurement id
  -n MEASUREMENT_NUMBER, --measurement_number MEASUREMENT_NUMBER
                        The measurement number for the date, if no id is
                        provided

"""

import glob
import os
import argparse

from atmospheric_lidar import ipral


if __name__ == "__main__":

    # Define the command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", nargs='?', help="Directory with licel files.", default='.')
    parser.add_argument("searchstring", nargs='?', help="Processing system id.", default="*.*")
    parser.add_argument("-m", "--measurement_id", help="The new measurement id", default=None)
    parser.add_argument("-n", "--measurement_number", help="The measurement number for the date, if no id is provided", default="00")
    args = parser.parse_args()


    # Get a list of files to convert
    search_str = os.path.join(args.directory, args.searchstring)
    files = glob.glob(search_str)

    if files:
        # Read the files
        print("Reading {0} files from {1}".format(len(files), args.directory))
        measurement = ipral.IpralLidarMeasurement(files)

        #Save the netcdf
        print("Saving netcdf.")
        measurement.set_measurement_id(args.measurement_id, args.measurement_number)
        measurement.save_as_SCC_netcdf()
        print("Created file ", measurement.scc_filename)
    else:
        print("No files found when searching for ", search_str)