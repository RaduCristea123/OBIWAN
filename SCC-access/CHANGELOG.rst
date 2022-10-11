Changelog
=========

0.10.1 - 2021-02-09
------------------
* Temporary workaround for missing "has_finished" flag from the API.

0.10.0 - 2021-02-04
------------------
* Support for new version of web interface (to be released on 2021-02-09.
* Output of module exit codes and their description.
* Support of the "delay" parameter when uploading files.

0.9.0 - 2021-01-09
------------------
* Added force_upload option
* Removed the process subcommand, added the --process option to the `upload_file` subcommand.
* Homogenised download directory names.

0.8.1 - 2019-12-19
------------------
* Correct handling of ancillary file full paths (thanks to Marc-Antoine Drouin)

0.8.0 - 2019-12-19
------------------
* Check if ancillary file is already in the SCC DB before uploading.

0.7.1 - 2019-12-05
------------------
* Fixed handling of both old- and new-style measurement ids.

0.7.0 - 2019-01-11
------------------
* Download method for HiRElPP, cloudmask, and other new datasets.
* Since 0.6.2: Restructuring of input arguments, ancillary file upload, code improvements.

0.6.2 - 2018-01-10
------------------
* Fixed bug when download optical files.
* Changes config file path to positional argument.

0.6.1 - 2017-12-15
------------------
* Converted script to python module
* Settings are now read from .yaml file.

0.5.0 - 2015-06-23
------------------
* Moved configuration settings to a separate file
* Added lincence information (MIT)
* Added version number
* Added this changelog

