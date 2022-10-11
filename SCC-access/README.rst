Overview
========

This package provides a tool for interacting with the
Single Calculus Chain through the command line. Specifically, with the script you can:

* Upload a file to the SCC for processing
* Download the processed files and graphs
* Delete an existing measurement from the SCC (with appropriate privileges)


The script provides limited feedback when a file upload fails. Before using
the script, you will need to upload some files manually and be confident that 
your SCC file format and processing settings are correct.

Please note that this is not part of the "official" SCC tools.

In principle, the main functions are implemented in a class (SCC) that you can also import
and use in your custom scripts. However, error handling has to be improved in this case. If you are interested
in such use, please contact me at ioannis@inoe.ro.

Any suggestions for improvements and new features are more than welcome.

Installation
------------

The easiest way to install this module is from the python package index using pip::
    
    pip install hg+https://repositories.imaa.cnr.it/public/scc_access#egg=scc-access

or, if you don't have mercurial on your system::

   pip install https://repositories.imaa.cnr.it/public/scc_access/archive/tip.zip

You can also use the script by cloning this mercurial repository.

Settings
--------
The required user-defined settings need to be specified in a .yaml file.

The following parameters should be specified::

   basic_credentials: ['username', 'password']     # The HTTP user name and password that is needed to access the SCC site.
   website_credentials: ['username', 'password']   # The user name and password that is needed to log in to the SCC site.
   output_dir: /path/to/files/scc_output/          # The directory to download the files
   base_url: https://scc.imaa.cnr.it/              # SCC base URL. Normally you shouldn't need to change that.


The repository includes a `settings_sample.yaml` file that you can use as a starting point. Rename the file, e.g. to
`settings.yaml` and input the required parameters. If you don't want to specify the file path every time
your run the `scc_access` script, you can name the file `.scc_access.yaml` and place it  in your home directory.

Please note that it's not a good idea to store your stations management credentials in the settings
file. The standard user has "Station Management" privileges and, if the credentials
are stolen, someone could change/delete the stations settings from the SCC database.
For this, it is better to use a user account with minimum access settings, i.e. that
can only upload files and measurements.


Usage
-----

You can upload a file specifying the username and the system id::
    
   scc_access upload-file 20110101po01.nc 125

If you want to wait for the processing to finish and download the resulting files
you can use add the `-p` or `--process` flag::
    
   scc_access upload-file 20110101po01.nc 125 -p

The two command above assume that you have placed your setting file in the default location. You can specify a
custom location using the -c flag::

   scc_access -c ./path/to/settings.yaml upload-file 20110101po01.nc 125

By default, the SCC will reject an uploaded file if the specified measurement id already exists on the server. You
can instruct the script to delete any existing measurement before uploading using the `--force_upload` flag::

   scc_access upload-file 20110101po01.nc 125 -p --force_upload

When uploading a measurement you can require that the processing is delayed by a specified number of hours. This
can be used to assure that your files are processed using model input data, that typically have a delay of more than
24 hours. You can specify the delay (in hours) using the --delay option::

   scc_access upload-file 20110101po01.nc 125 --delay 48

.. note::
   When specifying a delay, the script will not wait to download the output files.

If you want to delete an existing measurement id from the database use the `delete`
command and the measurement id::
    
    scc_access delete 20110101po01

You can list available measurements with the `list` command::

    scc_access list

.. note::
   The `list` command needs to be updated. Cross-check the results before using them.

For more information on the syntax type::
    
    scc_access -h

This will produce the following help text::

    usage: scc_access [-h] [-d] [-s] [-c CONFIG]
                      {delete,rerun-all,rerun-elpp,upload-file,list,download} ...

    positional arguments:
      {delete,rerun-all,rerun-elpp,upload-file,list,download}
        delete              Deletes a measurement.
        rerun-all           Rerun all processing steps for the provided
                            measurement IDs.
        rerun-elpp          Rerun low-resolution processing steps for the provided
                            measurement ID.
        upload-file         Submit a file and, optionally, download the output
                            products.
        list                List measurements registered on the SCC.
        download            Download selected measurements.

    optional arguments:
      -h, --help            show this help message and exit
      -d, --debug           Print debugging information.
      -s, --silent          Show only warning and error messages.
      -c CONFIG, --config CONFIG
                            Path to the config file.

You can find out more information about each command e.g. like this::

   scc_access upload-file -h

In this case, the help text will give more details about the `upload-file` command::

    usage: scc_access upload-file [-h] [-p] [--force_upload]
                                  [--radiosounding RADIOSOUNDING]
                                  [--overlap OVERLAP] [--lidarratio LIDARRATIO]
                                  filename system

    positional arguments:
      filename              Measurement file name or path.
      system                Processing system id.

    optional arguments:
      -h, --help            show this help message and exit
      --delay DELAY         Delay processing by the specified number of hours (0 to 96).
      -p, --process         Wait for the processing results.
      --force_upload        If measurement ID exists on SCC, delete before
                            uploading.
      --radiosounding RADIOSOUNDING
                            Radiosounding file name or path
      --overlap OVERLAP     Overlap file name or path
      --lidarratio LIDARRATIO
                            Lidar ratio file name or path
