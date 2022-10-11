Overview
========

This package provides utilities to handle raw (atmospheric) lidar input data.
The main format supported are Licel binary files (including the Raymetrics modified format).

The package provides a single command line tool, called licel2scc that can convert Licel binary files to the
EARLINET's Single Calculus Chain NetCDF format.

Installation
============

The easiest way to install this module is from the python package index using ``pip``::

   pip install atmospheric-lidar

Using it as a Licel to SCC converter
====================================

Parameter file
--------------
Before converting Licel binary to SCC format, you need to create a file linking Licel channels to SCC channels.

As an example, you can start by changing the file “cf_netcdf_parameters.py” that describe such
parameters for the Clermont Ferrand  lidar.

Command line interface
----------------------
The usage of the  ``licel2scc`` program is described below::

    A program to convert Licel binary files to the SCC NetCDF format.

    positional arguments:
      parameter_file        The path to a parameter file linking licel and SCC
                            channels.
      files                 Location of licel files. Use relative path and
                            filename wildcards. (default './*.*')

    optional arguments:
      -h, --help            show this help message and exit
      -i, --id_as_name      Use transient digitizer ids as channel names, instead
                            of descriptive names
      -m MEASUREMENT_ID, --measurement_id MEASUREMENT_ID
                            The new measurement id
      -n MEASUREMENT_NUMBER, --measurement_number MEASUREMENT_NUMBER
                            The measurement number for the date from 00 to 99.
                            Used if no id is provided
      -t TEMPERATURE, --temperature TEMPERATURE
                            The temperature (in C) at lidar level, required if
                            using US Standard atmosphere
      -p PRESSURE, --pressure PRESSURE
                            The pressure (in hPa) at lidar level, required if
                            using US Standard atmosphere
      -D DARK_FILES, --dark_files DARK_FILES
                            Location of files containing dark measurements.
                            Use relative path and filename wildcars, see 'files'
                            parameter for example.
      -d, --debug           Print dubuging information.
      -s, --silent          Show only warning and error messages.
      --version             Show current version.

Similarly, the ``licel2scc-depol`` program can be used to convert
Licel files from Delta45 depolarization calibration measurements::

    A program to convert Licel binary files from depolarization calibration
    measurements to the SCC NetCDF format.

    positional arguments:
      parameter_file        The path to a parameter file linking licel and SCC
                            channels.
      plus45_string         Search string for plus 45 degree files (default '*.*')
      minus45_string        Search string for minus 45 degree files (default
                            '*.*')

    optional arguments:
      -h, --help            show this help message and exit
      -i, --id_as_name      Use transient digitizer ids as channel names, instead
                            of descriptive names
      -m MEASUREMENT_ID, --measurement_id MEASUREMENT_ID
                            The new measurement id
      -n MEASUREMENT_NUMBER, --measurement_number MEASUREMENT_NUMBER
                            The measurement number for the date from 00 to 99.
                            Used if no id is provided
      -t TEMPERATURE, --temperature TEMPERATURE
                            The temperature (in C) at lidar level, required if
                            using US Standard atmosphere
      -p PRESSURE, --pressure PRESSURE
                            The pressure (in hPa) at lidar level, required if
                            using US Standard atmosphere
      -d, --debug           Print dubuging information.
      -s, --silent          Show only warning and error messages.
      --version             Show current version.

Usage in python code
--------------------
System class
~~~~~~~~~~~~
To read data from a system, you need create a class that describes you system.
This is very simple if your lidar data are in the Licel format, as you only need to specify
the external file with the extra SCC parameters. You can use as an example the file ``cf_netcdf_parameters.py``:
   
.. code-block:: python

   from licel import LicelLidarMeasurement
   import cf_netcdf_parameters

   class CfLidarMeasurement(LicelLidarMeasurement):
       extra_netcdf_parameters = cf_netcdf_parameters

This code assumes that the ``cf_netcdf_parameters.py`` is in your python path.

Using the class
~~~~~~~~~~~~~~~

Once you have made the above setup you can start using it. The best way to understand how
it works is through an interactive shell (I suggest [ipython](http://ipython.org/)).
In the following example I use the cf_raymetrics setup:
   
.. code-block:: python

   import glob  # This is needed to read a list of filenames
   import cf_lidar

   # Go to the folder where you files are stored
   cd /path/to/lidar/files

   # Read the filenames
   files  = glob.glob("*") # The * reads all the files in the folder.

   # Read the files
   my_measurement = cf_lidar.CfLidarMeasurement(files)

   # Now the data have been read, and you have a measurement object to work with:
   # See what channels are present
   print(my_measurement.channels)

   # Quicklooks of all the channels
   my_measurements.plot()

Converting to SCC format
~~~~~~~~~~~~~~~~~~~~~~~~

There are some extra info you need to put in before converting to SCC format, "Measurement_ID", "Temperature", "Pressure":
   
.. code-block:: python

   my_measurement.info["Measurement_ID"] = "20101229op00"
   my_measurement.info["Temperature"] = "14"
   my_measurement.info["Pressure"] = "1010"

You can use standard values of temperature and pressure by just calling:
   
.. code-block:: python

    my_measurement.get_PT()

You can specify the standard values by overriding your system's ``get_PT`` method:

.. code-block:: python

   from licel import LicelLidarMeasurement
   import cf_netcdf_parameters

   class CfLidarMeasurement(LicelLidarMeasurement):
       extra_netcdf_parameters = cf_netcdf_parameters

       def get_PT():
           self.info['Temperature'] = 25.0
           self.info['Pressure'] = 1020.0

If you have an external source of temperature and pressure information (a meteorological station) you can automate
this by reading the appropriate code in the ``get_PT`` method .


After you have used this extra input, you save the file using this command:

.. code-block:: python

   my_measurement.save_as_SCC_netcdf("filename")

where you change the output filename to the filename you want to use.