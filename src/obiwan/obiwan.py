from .lidarchive import lidarchive
from atmospheric_lidar.licel import LicelLidarMeasurement

from scc_access import scc_access

import argparse
import datetime
import importlib
import logging
import os
import pytz
import re
import sys
import yaml
import shutil
import time
import pickle

from netCDF4 import Dataset

SCC_STATUS_NOT_STARTED = 0
SCC_STATUS_IN_PROGRESS = 1
SCC_STATUS_OK = 127
SCC_STATUS_FAILED = -127
LOG_FILE = "licel2scc.log"

# Set up logging
log_format = '%(asctime)s %(levelname)-8s %(scope)-12s %(message)s'

CURRENT_MEASUREMENT = "scc-access"
SWAP_FILE_NAME = "obiwan.swp"

class SwapFile:
    def __init__ ( self, file_path ):
        self.measurements = {}
        self.config = {}
        self.file_path = file_path
        
    def load ( self ):
        try:
            with open ( self.file_path, 'rb' ) as file:
                info = pickle.load(file)
                
            self.config = info["config"]
            self.measurements = info["measurements"]
            
            return len(self.config.keys()) > 0
        except:
            self.reset()
            
        return False
        
    def save ( self ):
        with open ( self.file_path, 'wb' ) as file:
            pickle.dump({
                "config": self.config,
                "measurements": self.measurements
            }, file)
            
    def reset ( self ):
        self.reset_measurements ()
        self.config = {}
        
    def reset_measurements ( self ):
        self.measurements = {}

    def update_config ( self, kvp, save = True ):
        self.config[ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
            
    def update_measurement ( self, measurement_id, kvp, save = True ):
        if measurement_id not in self.measurements.keys():
            self.measurements[ measurement_id ] = {}
            
        self.measurements[ measurement_id ][ kvp[0] ] = kvp[1]
        
        if save:
            self.save()
        
    def update_measurement_by_scc_id ( self, scc_id, kvp, save = True ):
        for key in self.measurements.keys():
            try:
                if self.measurements[ key ][ "scc_measurement_id" ] == scc_id:
                    self.measurements[ key ][ kvp[0] ] = kvp[1]
                    
                    if save:
                        self.save()
                        
                    return
            except:
                pass

class SystemLogFilter ( logging.Filter ):
    def filter ( self, record ):
        if not hasattr ( record, 'scope' ):
            record.scope = 'main'
            
        return True
        
class SCCLogFilter ( logging.Filter ):
    def filter ( self, record ):
        if not hasattr ( record, 'scope' ):
            record.scope = CURRENT_MEASUREMENT
            
        return True
        
class LidarLogFilter ( logging.Filter ):
    def filter ( self, record ):
        if not hasattr ( record, 'scope' ):
            record.scope = CURRENT_MEASUREMENT
            
        return True

logging.basicConfig (
    level = logging.INFO,
    format = log_format,
    datefmt = '%Y-%m-%d %H:%M',
    filename = LOG_FILE, # 'obiwan_%s.log' % ( datetime.datetime.now().strftime( '%Y-%m-%d_%H.%M.%S' ) ),
    filemode = 'w'
)

logger = logging.getLogger( 'obiwan' )
logger.addFilter ( SystemLogFilter() )

formatter = logging.Formatter ( log_format, '%Y-%m-%d %H:%M' )

logger.setLevel (logging.INFO)
logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.ERROR )
logging.getLogger ( 'scc_access.scc_access' ).addFilter ( SCCLogFilter() )

logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.ERROR )
logging.getLogger ( 'atmospheric_lidar.generic' ).addFilter ( LidarLogFilter() )

console = logging.StreamHandler()
console.setLevel ( logging.INFO )
console.setFormatter ( formatter )
logging.getLogger().addHandler ( console )

class ObiwanConfig:
    def __init__ ( self, file_path ):
        with open (file_path) as yaml_file:
            try:
                config = yaml.safe_load (yaml_file)
            except:
                logger.error("Could not parse YAML file (%s). Exiting..." % args.cfg)
                sys.exit (1)
                
        self.scc_settings_file = config['scc_settings_file']
        self.scc_configurations_folder = config['scc_configurations_folder']
        self.maximum_upload_retry_count = config['maximum_upload_retries']
        self.max_acceptable_gap = config['maximum_measurement_gap']
        self.min_acceptable_length = config['minimum_measurement_length']
        self.max_acceptable_length = config['maximum_measurement_length']
        self.center_type = config['measurement_center_type']
        self.netcdf_parameters_path = config['system_netcdf_parameters']
        self.time_parameter_file = config['time_parameter_file']
        self.measurements_debug = config['measurements_debug']
        self.measurements_debug_dir = config['measurements_debug_dir']
        self.netcdf_out_dir = config.get('netcdf_out_folder', os.getcwd())
        
        for key in config.keys():
            if key.startswith("test_list_"):
                self.test_lists = config[key]
            

class Channel:
    '''
    Helper class used to describe a lidar system channel.
    '''
    def __init__ (self, licel_channel):
        self.name = licel_channel.name
        self.resolution = licel_channel.resolution
        self.wavelength = licel_channel.wavelength
        self.laser_used = licel_channel.laser_used
        self.adcbits = licel_channel.adcbits
        self.analog = licel_channel.is_analog
        self.active = licel_channel.active
        
    def Equals (self, channel):
        '''
        Compares two lidar channels.
        
        Parameters
        ----------
        channel : Channel
            The channel used for comparison.
        '''
        if  (self.name == channel.name and
                self.resolution == channel.resolution and
                self.laser_used == channel.laser_used and
                self.adcbits == channel.adcbits and
                self.analog == channel.analog and
                self.active == channel.active):
            
            return True
            
        return False

class System:
    '''
    Helper class to describe a lidar system.
    '''
    def __init__ (self, file):
        self.file = None
        self.id = None
        self.channels = []
        
        self.ReadFromFile (file)
        
    def ReadFromFile (self, file):
        '''
        Reads a sample file to determine the lidar
        system configuration.
        
        Parameters
        ----------
        file : str
            Path of the raw lidar data file.
        '''
        self.file = file
        self.id = os.path.basename (file)
        measurement = LicelLidarMeasurement ([file])
        
        for channel_name, channel in measurement.channels.items():
            self.channels.append (Channel (channel))
            
        del measurement
        
    def Equals (self, system):
        '''
        Compares two lidar systems by comparing their channels.
        
        Parameters
        ----------
        system : System
            The system used for comparison.
        '''
        # Make a copy of the other system's channel list:
        other_channels = system.channels[:]
        
        if len(self.channels) != len(other_channels):
            return False
        
        for channel in self.channels:
            found = False
            for other_channel in other_channels:
                if channel.Equals (other_channel):
                    found = True
                    other_channels.remove (other_channel)
                    break
                    
            if found == False:
                return False
           
        if len (other_channels) > 0:
            return False
            
        return True
        
class SystemIndex:
    '''
    Holds an index of lidar systems.
    '''
    def __init__ (self, folder):
        self.systems = []
        
        self.ReadFolder (folder)
        
    def ReadFolder (self, folder):
        '''
        Reads an entire folder and identifies distinct lidar
        systems inside that folder by looking into the raw
        lidar data files.
        
        Parameters
        ----------
        folder : str
            Path of the folder holding the sample data files.
        '''
        files = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        
        for file in files:
            try:
                self.systems.append (System (file))
            except Exception:
                logger.warning ("File %s is not a valid sample file" % file)
                pass
        
    def GetSystemId (self, system):
        '''
        Retrieves the system ID stored in the SystemIndex for a
        given lidar system. Used to determine the system ID for a
        specific measurement.
        
        Parameters
        ----------
        system : System
            The system you need to retrieve the ID for.
        '''
        compatible_ids = []
        system_obj = System (system)
        
        for s in self.systems:
            if system_obj.Equals (s):
                compatible_ids.append (s.id)
                
        if len(compatible_ids) == 0:
            raise ValueError ( "Couldn't find a matching configuration." )
        
        if len(compatible_ids) > 1:
            raise ValueError ( "More than one configuration matches." )
            
        return compatible_ids[0]

def LogText ( text ):
    '''
    Creates an entry in the log file.
    
    Parameters
    ----------
    text : str
        Text to be written to the log file. Newline character will be
        appended automatically.
    '''
    if os.path.isfile ( LOG_FILE ) == False:
        with open ( LOG_FILE, 'w' ) as logfile:
            logfile.write ( "%s\t%s\t%s\t%s\t%s\t\n" % ( "Date", "Measurement", "PP ver.", "ELPP ver.", "ELDA ver." ) )
            logfile.write ( text + "\n" )
    else:
        with open ( LOG_FILE, 'a' ) as logfile:
            logfile.write (text + "\n")

def GetPreProcessorVersion ( file ):
    '''
    Reads the SCC preprocessor version used to process a measurement.
    
    Parameters
    ----------
    file : str
        Name or path of the NetCDF file downloaded from the SCC (SCC preprocessed file).
    
    Return values
    -------------
    String representing the SCC preprocessor version.
    '''
    dataset = Dataset ( file )
    pp_version = dataset.SCCPreprocessingVersion
    dataset.close ()
    
    return pp_version
    
def GetELPP_ELDAVersion ( file ):
    '''
    Reads the SCC processing software version used to process a measurement.
    
    Parameters
    ----------
    file : str
        Name or path of the NetCDF file downloaded from the SCC (SCC processed file).    
    Return values
    -------------
    Two strings, representing the ELPP version and the ELDA version.
    '''
    dataset = Dataset ( file )
    software_version = dataset.__AnalysisSoftwareVersion
    dataset.close ()
    
    elpp_regex = 'ELPP version: ([^;]*);'
    elda_regex = 'ELDA version: (.*)$'
    
    scc_elpp_version = re.findall ( elpp_regex, software_version )[0]
    scc_elda_version = re.findall ( elda_regex, software_version )[0]
    
    return scc_elpp_version, scc_elda_version

def GetSCCVersion ( download_folder, measurement_id ):
    '''
    Retrieves version information about the SCC chain used to process a given measurement.
    
    Parameters
    ----------
    download_folder : str
        Path to the download folder as passed to the scc-access module.
    measurement_id : str
        ID of the measurement used to retrieve the information.
    
    Return values
    -------------
    String representing the SCC version and SCC processor versions description.
    '''
    preprocessed_folder = os.path.join ( download_folder, measurement_id, 'elpp' )
    file = os.path.join ( preprocessed_folder, os.listdir(preprocessed_folder)[0] )
    
    dataset = Dataset ( file )
    
    scc_version = dataset.scc_version_description
    
    dataset.close()
    
    return scc_version
    
def TryUpload ( filename, system_id, replace, scc ):
    # try:
    upload = scc.upload_file ( filename, system_id, replace, False )
    # except Exception, e:
        # measurement_id = os.path.splitext ( os.path.basename(filename) ) [0]
        # logger.warning ( "[%s] SCC upload error: %s" % (measurement_id, str(e)))        
        # upload = False
        
    if upload != False:
        upload = True
        
    return upload

def UploadMeasurement ( filename, system_id, scc, max_retry_count, replace ):
    '''
    Upload a NetCDF file to the SCC and process it.
    
    Parameters
    ----------
    filename : str
        NetCDF file to upload to SCC.
    system_id : int
        System ID as set up in the SCC web interface.
    scc : SCC
        SCC object used for interacting with the SCC API.
    max_retry_count : int
        Maximum number of retries in case of a failed upload.
    '''
    measurement_id = os.path.splitext ( os.path.basename(filename) ) [0]
    
    retry_count = 0
    
    # Send the file to SCC and start the processing chain:
    upload = TryUpload (filename, system_id, replace, scc)
    
    # If the upload failed, retry for a given number of times.
    while upload == False and retry_count < max_retry_count:
        retry_count += 1
        logger.warning ( "[%s] Upload to SCC failed with code. Retrying (%d/%d)." % (measurement_id, retry_count, max_retry_count), extra={'scope': measurement_id} )
        
        upload = TryUpload (filename, system_id, replace, scc)
        
    return upload
    
def DownloadProducts ( measurements, scc ):
    '''
    Download products for a given set of measurements.
    
    Parameters
    ----------
    measurements : list
        list of measurement names to download
    scc : SCC
        SCC connection to use the SCC API    
    '''
    
    logger.info ( "Downloading SCC products" )
    
    for measurement_id in measurements:
        CURRENT_MEASUREMENT = measurement_id
        
        logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': measurement_id} )
        
        result = scc.monitor_processing ( measurement_id, exit_if_missing = False )
        
        if result is not None:
            logger.debug ( "Processing finished", extra={'scope': measurement_id} )
            
            try:
                scc_version = GetSCCVersion ( scc.output_dir, measurement_id )
            except Exception as e:
                if result.elpp != 127:
                    logger.error ( "No SCC products found", extra={'scope': measurement_id} )
                else:
                    logger.error ( "Unknown error in SCC products", extra={'scope': measurement_id} )
                scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                continue
                
            logger.info ( scc_version, extra={'scope': measurement_id} )
        else:
            logger.error ( "Download failed", extra={'scope': measurement_id} )
            

def Convert ( config, curr_measurement_index, licel_measurement, swap_file, processing_log ):
    logger.debug ( "Converting %d licel files to SCC NetCDF format." % len(licel_measurement.DataFiles()) )
    
    try:
        system_id = system_index.GetSystemId (licel_measurement.DataFiles()[0].Path())
    except ValueError as e:
        logger.error ("Couldn't determine system ID for measurement '%s': %s. Skipping measurement." % (licel_measurement.DataFiles()[0].Path(), str(e)))
        return None, None
    except IndexError as e:
        logger.error ( "Could not find any data files for this measurement. Skipping." )
        return None, None
        
    swap_file.update_measurement ( curr_measurement_index, ("system_id", system_id) )
        
    try:
        earlinet_station_id = nc_parameters_module.general_parameters['Call sign']
        date_str = licel_measurement.DataFiles()[0].StartDateTime().strftime('%Y%m%d')
        measurement_number = licel_measurement.NumberAsString()
        measurement_id = "{0}{1}{2}".format(date_str, earlinet_station_id, measurement_number)
    except Exception as e:
        logger.error ( "Could not determine measurement ID. Skipping..." )
        return None, None
        
    swap_file.update_measurement ( curr_measurement_index, ("scc_measurement_id", measurement_id) )
    
    CURRENT_MEASUREMENT = measurement_id
    m = { 'scope': measurement_id }
    
    processing_log[ measurement_id ] = {
        "process_start": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
        "data_folder": os.path.abspath ( args.folder ),
        "data_file": "",
        "scc_system_id": system_id,
        "uploaded": "FALSE",
        "downloaded": "FALSE",
        "scc_version": "",
        "result": "",
    }

    measurement = CustomLidarMeasurement ( [file.Path() for file in licel_measurement.DataFiles()] )
    
    if len(licel_measurement.DarkFiles()) > 0:
        measurement.dark_measurement = CustomLidarMeasurement ( [file.Path() for file in licel_measurement.DarkFiles()] )

    measurement = measurement.subset_by_scc_channels ()
    measurement.set_measurement_id(measurement_number=licel_measurement.NumberAsString())
    
    file_path = os.path.join(config.netcdf_out_dir, f'{measurement_id}.nc')
    
    measurement.save_as_SCC_netcdf (filename=file_path)
    
    swap_file.update_measurement ( curr_measurement_index, ("converted", True) )
    swap_file.update_measurement ( curr_measurement_index, ("scc_netcdf_path", file_path) )
    
    return file_path, measurement_id
    
def DebugMeasurement ( licel_measurement, measurement_path, measurements_debug_dir ):
    if measurements_debug_dir:
        debug_date_str = licel_measurement.DataFiles()[0].StartDateTime().strftime('%Y-%m-%d-%H-%M')
        debug_dir = os.path.join ( measurements_debug_dir, debug_date_str )

        if os.path.exists (debug_dir):
            i = 2
            temp_debug_dir = "%s_%d" % (debug_dir, i)
            
            while os.path.exists ( temp_debug_dir ):
                i += 1
                temp_debug_dir = "%s_%d" % (debug_dir, i)
                
            debug_dir = temp_debug_dir
        
        debug_dark_dir = os.path.join ( debug_dir, "D" )

    os.makedirs ( debug_dir )
    os.makedirs ( debug_dark_dir )
    
    logger.debug ("Raw data files:")
    for file in licel_measurement.DataFiles():
        logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dir )
        
    logger.debug ("Raw dark files:")
    for file in licel_measurement.DarkFiles():
        logger.debug ( os.path.basename(file.Path()) )
        if measurements_debug_dir:
            shutil.copy2 ( file.Path(), debug_dark_dir )
            
    logger.debug ("SCC NetCDF file: %s" % ( os.path.basename(measurement_path) ))
    if measurements_debug_dir:
        shutil.copy2 ( measurement_path, debug_dir )
            
def Upload (config, scc, curr_measurement_index, measurement_id, measurement_date, file_path, swap_file, processing_log, **kwargs):
    reprocess = kwargs.get("reprocess", True)
    replace = kwargs.get("replace", True)
    
    CURRENT_MEASUREMENT = "main"
    
    logger.info ( "Converting and uploading measurement %d." % ( curr_measurement_index + 1) )
    
    measurement_exists = False
    existing_measurement, _ = scc.get_measurement( measurement_id )
    
    swap_file.update_measurement ( curr_measurement_index, ("already_on_scc", measurement_exists) )
    
    if existing_measurement is not None:
        measurement_exists = True
    
    if measurement_exists and reprocess:
        # Reprocess the measurement and mark it for download
        logger.debug ( "Measurement already exists in the SCC, triggering reprocessing." )
        scc.rerun_all ( measurement_id, False )
        swap_file.update_measurement ( curr_measurement_index, ("uploaded", True) )
        
        if swap_file.config["last_processed_date"] is None:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > swap_file.config["last_processed_date"]:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
            
        return measurement_id
    elif measurement_exists and not replace:
        # Simply mark the measurement for download without reuploading or reprocessing
        logger.debug ( "Measurement already exists in the SCC, skipping reprocessing." )
        swap_file.update_measurement ( curr_measurement_index, ("uploaded", True) )
        
        if swap_file.config["last_processed_date"] is None:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > swap_file.config["last_processed_date"]:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
            
        return measurement_id

    processing_log[ measurement_id ][ "data_file" ] = os.path.abspath ( file_path )
    
    measurement_id = os.path.splitext ( os.path.basename(file_path) ) [0]
    CURRENT_MEASUREMENT = measurement_id
    
    can_download = UploadMeasurement ( file_path, system_id, scc, config.maximum_upload_retry_count, replace )

    if can_download == True:
        logger.debug ( "Successfully uploaded to SCC", extra=m)
        processing_log[ measurement_id ][ "uploaded" ] = "TRUE"
        
        if swap_file.config["last_processed_date"] is None:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
        elif measurement_date > swap_file.config["last_processed_date"]:
            swap_file.update_config ( ("last_processed_date", measurement_date) )
            
        swap_file.update_measurement ( curr_measurement_index, ("uploaded", True) )
        return measurement_id
    else:
        processing_log[ measurement_id ][ "result" ] = "Error uploading to SCC"
        
    return None
    
def ResumePastWork (scc, scc_settings, swap_file):    
    if not swap_file.load():
        return []
        
    logger.warning ("obiwan was interrupted during previous task. Resuming...")
    
    if not swap_file.config["convert"]:
        scc.login(scc_settings['website_credentials'])

    resume_download = [
        measurement["scc_measurement_id"] for measurement in swap_file.measurements.values()
        if not measurement["downloaded"] and swap_file.config["download"] and measurement["uploaded"]
    ]

    resume_convert = [
        measurement for measurement in swap_file.measurements.values()
        if not measurement["converted"]
    ]

    resume_upload = [
        measurement for measurement in swap_file.measurements.values()
        if not swap_file.config["convert"] and measurement["converted"] and not measurement["uploaded"]
    ]
    
    logger.info (f"Not converted: {len(resume_convert)}, not uploaded: {len(resume_upload)}, not downloaded: {len(resume_download)} ")

    if len(resume_convert):
        logger.info(f"Retrying to convert {len(resume_convert)} measurements...")
        
    for curr_measurement_index, measurement in enumerate (resume_convert):
        try:
            file_path, measurement_id = Convert ( swap_file.config["yaml"], curr_measurement_index, measurement["licel_measurement"], swap_file, processing_log )
            
            if not measurement_id:
                continue
            
            if swap_file.config["yaml"].measurements_debug:
                if swap_file.config["yaml"].measurements_debug_dir:
                    DebugMeasurement (measurement["licel_measurement"], file_path, swap_file.config["yaml"].measurements_debug_dir)
                    
            logger.info(f"{measurement_id} added to list of measurements to be uploaded.")
            resume_upload.append(measurement)
        except Exception as e:
            logger.error ( "Error processing measurement: %s" % (str(e)) )
            processing_log[ measurement["scc_measurement_id"] ][ "result" ] = "Error processing measurement: %s" % (str(e))
            
    if len(resume_upload):
        logger.info(f"Retrying to upload {len(resume_upload)} measurements...")
        
    for curr_measurement_index, measurement in enumerate (resume_upload):
        try:
            if measurement["scc_measurement_id"] not in processing_log.keys():
                processing_log[ measurement["scc_measurement_id"] ] = {
                    "process_start": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "data_folder": os.path.abspath ( swap_file.config["folder"] ),
                    "data_file": "",
                    "scc_system_id": measurement["system_id"],
                    "uploaded": "FALSE",
                    "downloaded": "FALSE",
                    "scc_version": "",
                    "result": "",
                }
    
            if not swap_file.config["convert"]:
                measurement_id = Upload (
                    swap_file.config["yaml"],
                    scc,
                    curr_measurement_index,
                    measurement["scc_measurement_id"],
                    measurement["licel_measurement"].DataFiles()[-1].EndDateTime(),
                    measurement["scc_netcdf_path"],
                    swap_file,
                    processing_log,
                    reprocess = swap_file.config["reprocess"],
                    replace = swap_file.config["replace"]
                )
                
                if measurement_id:
                    logger.info(f'{measurement["scc_measurement_id"]} added to list of measurements to be downloaded.')
                    resume_download.append(measurement["scc_measurement_id"])
                    
        except Exception as e:
            logger.error ( "Error processing measurement: %s" % (str(e)) )
            processing_log[ measurement["scc_measurement_id"] ][ "result" ] = "Error processing measurement: %s" % (str(e))
            
    for measurement_id in resume_download:
        if measurement_id not in processing_log.keys():
            processing_log[ measurement_id ] = {
                "process_start": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "data_folder": os.path.abspath ( swap_file.config["folder"] ),
                "data_file": "",
                "scc_system_id": "N/A",
                "uploaded": "FALSE",
                "downloaded": "FALSE",
                "scc_version": "",
                "result": "",
            }
            
    return resume_download

parser = argparse.ArgumentParser(description="Tool for processing Licel lidar measurements using the Single Calculus Chain.")
parser.add_argument("--folder", help="The path to the folder you want to scan.")
parser.add_argument("--datalog", help="Path of the Datalog CSV you want to save the processing log in.", default="datalog.csv")
parser.add_argument("--startdate", help="The path to the folder you want to scan.")
parser.add_argument("--enddate", help="The path to the folder you want to scan.")
parser.add_argument("--cfg", help="Configuration file for this script.", default="obiwan.yaml")
parser.add_argument("--verbose", "-v", help="Verbose output level.", action="count")
parser.add_argument("--replace", "-r", help="Replace measurements that already exist in the SCC database.", action="store_true")
parser.add_argument("--reprocess", "-p", help="Reprocess measurements that already exist in the SCC database, skipping the reupload.", action="store_true")
parser.add_argument("--download", "-d", help="Download SCC products after processing", action="store_true")
parser.add_argument("--convert", "-c", help="Convert files to SCC NetCDF without submitting", action="store_true")
parser.add_argument("--continuous", help="Use for continuous measuring systems", action="store_true")
parser.add_argument("--resume", help="Use for continuous measuring systems", action="store_true")

args = parser.parse_args ()

if args.verbose is not None:
    if args.verbose == 1:
        logger.setLevel (logging.DEBUG)
        console.setLevel ( logging.DEBUG )
    elif args.verbose == 2:
        logger.setLevel (logging.DEBUG)
        console.setLevel ( logging.DEBUG )
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.INFO )
        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.INFO )
    elif args.verbose > 2:
        logger.setLevel (logging.DEBUG)
        console.setLevel ( logging.DEBUG )
        logging.getLogger ( 'scc_access.scc_access' ).setLevel ( logging.DEBUG )
        logging.getLogger ( 'atmospheric_lidar.generic' ).setLevel ( logging.DEBUG )

if args.folder is None:
    logger.error ( "You must specify the data folder. Exiting..." )
    parser.print_help()
    sys.exit (1)

if not os.path.isfile (args.cfg):
    logger.error ( "Wrong path for configuration file (%s)" % args.cfg )
    parser.print_help()
    sys.exit (1)
    
if args.datalog is None:
    datalog = "datalog.csv"
else:
    datalog = args.datalog
    
config = ObiwanConfig ( args.cfg )

scc_settings = scc_access.settings_from_path (config.scc_settings_file)
scc = scc_access.SCC(scc_settings['basic_credentials'], scc_settings['output_dir'], scc_settings['base_url'])

swap_file_path = os.path.join ( config.netcdf_out_dir, SWAP_FILE_NAME )

if not os.path.isdir (config.netcdf_out_dir):
    os.makedirs (config.netcdf_out_dir)

test_lists = config.test_lists

sys.path.append ( os.path.dirname (config.netcdf_parameters_path) )
netcdf_parameters_filename = os.path.basename ( config.netcdf_parameters_path )
if netcdf_parameters_filename.endswith ('.py'):
    netcdf_parameters_filename = netcdf_parameters_filename[:-3]

nc_parameters_module = importlib.import_module ( netcdf_parameters_filename )

class CustomLidarMeasurement(LicelLidarMeasurement):
    extra_netcdf_parameters = nc_parameters_module

lidarchive = lidarchive.Lidarchive ()
lidarchive.SetFolder (args.folder)

if args.startdate is None:
    start_date = None
else:
    start_date = datetime.datetime.strptime( args.startdate, '%Y%m%d%H%M%S' )
    
if args.enddate is None:
    end_date = None
else:
    end_date = datetime.datetime.strptime( args.enddate, '%Y%m%d%H%M%S' )
    
to_download = []
processing_log = {}
swap_file = SwapFile ( swap_file_path )
swap_file.load()

if args.continuous:
    logger.debug(swap_file.config)
    
    last_processed_date = swap_file.config.get("last_processed_date", None)
    
    if last_processed_date is not None:
        if start_date is not None and start_date < last_processed_date:
            start_date = last_processed_date
        if start_date is None:
            start_date = last_processed_date

system_index = SystemIndex (config.scc_configurations_folder)

if not args.convert:
    scc.login(scc_settings['website_credentials'])

if args.resume:
    resume_download = ResumePastWork ( scc, scc_settings, swap_file )
    to_download += resume_download
    # swap_file.reset()

log_header_run_time = "Run started at %s" % ( datetime.datetime.now().strftime ( "%Y-%m-%d %H:%M:%S" ) )
logger.info ( log_header_run_time, extra={'scope': 'start'} )

log_header_cfg = "Configuration file = %s" % ( os.path.abspath ( args.cfg ) )
logger.info ( log_header_cfg )

log_header_folder = "Data folder = %s" % os.path.abspath ( args.folder )
logger.info ( log_header_folder )

start_time_text = "N/A" if start_date is None else start_date.strftime ( "%Y-%m-%d %H:%M:%S" )
log_header_start_time = "Minimum start time = %s" % ( start_time_text )
logger.info ( log_header_start_time )

end_time_text = "N/A" if end_date is None else end_date.strftime ( "%Y-%m-%d %H:%M:%S" )
log_header_end_time = "Maximum end time = %s" % (end_time_text)
logger.info ( log_header_end_time )

log_header_gap = "Maximum gap between measurements (seconds) = %d" % config.max_acceptable_gap
logger.info ( log_header_gap )

log_header_scc = "SCC configuration file = %s" % ( os.path.abspath ( config.scc_settings_file ) )
logger.info ( log_header_scc )

logger.debug ( "Identifying measurements. This can take a few minutes...")

lidarchive.ReadFolder (start_date, end_date)
licel_measurements = lidarchive.ContinuousMeasurements (config.time_parameter_file, config.max_acceptable_gap, config.min_acceptable_length, config.max_acceptable_length, config.center_type)

for index, licel_measurement in enumerate(licel_measurements):
    swap_file.update_measurement ( index, ("licel_measurement", licel_measurement), save=False )
    swap_file.update_measurement ( index, ("scc_netcdf_path", ""), save=False )
    swap_file.update_measurement ( index, ("converted", False), save=False )
    swap_file.update_measurement ( index, ("uploaded", False) )
    swap_file.update_measurement ( index, ("downloaded", False), save=False )
    swap_file.update_measurement ( index, ("system_id", None), save=False )
    swap_file.update_measurement ( index, ("scc_measurement_id", None), save=False )
    swap_file.update_measurement ( index, ("already_on_scc", False), save=False )
    
swap_file.update_config(("convert", args.convert), save=False)
swap_file.update_config(("reprocess", args.reprocess), save=False)
swap_file.update_config(("replace", args.replace), save=False)
swap_file.update_config(("download", args.download), save=False)
swap_file.update_config(("folder", os.path.abspath(args.folder)), save=False)
swap_file.update_config(("last_processed_date", None), save=False)
swap_file.update_config(("yaml", config))

logger.debug ( "Processed %d files" % len (lidarchive.Measurements()) )
logger.debug ( "Identified %d different continuous measurements with a maximum acceptable gap of %ds" % (len (licel_measurements), config.max_acceptable_gap) )

logger.info ( "Starting processing" )

for curr_measurement_index, licel_measurement in enumerate (licel_measurements):
    try:
        file_path, measurement_id = Convert ( config, curr_measurement_index, licel_measurement, swap_file, processing_log )

        if not measurement_id:
            continue

        if config.measurements_debug:
            if config.measurements_debug_dir:
                DebugMeasurement (licel_measurement, file_path, config.measurements_debug_dir)
                
        if not args.convert:
            measurement_id = Upload (
                config,
                scc,
                curr_measurement_index,
                measurement_id,
                licel_measurement.DataFiles()[-1].EndDateTime(),
                file_path,
                swap_file,
                processing_log,
                reprocess = args.reprocess,
                replace = args.replace
            )
            
            if measurement_id:
                to_download.append(measurement_id)
            
    except Exception as e:
        logger.error ( "Error processing measurement: %s" % (str(e)) )
        processing_log[ measurement_id ][ "result" ] = "Error processing measurement: %s" % (str(e))

if config.measurements_debug_dir:
    debug_test_dir = -1
    if lidarchive.TestFiles():
        measurements_debug_dir = os.path.join(config.measurements_debug_dir, "Tests")
        os.makedirs(measurements_debug_dir)
        test_files = lidarchive.TestFiles()
        gapped_test_files = lidarchive.FilterByGap(test_files, config.max_acceptable_gap, same_location=False)
        for test_file in gapped_test_files:
            for test in test_file:
                flag = False
                for j in range(len(test_lists)):
                    if test.Site() in test_lists[j]:
                        flag = True
                        debug_test_dir = os.path.join(measurements_debug_dir, "Test_List_%s_" % str(j+1) + test_file[0].StartDateTime().strftime('%Y-%m-%d-%H-%M'))
                        if os.path.exists(debug_test_dir):
                            shutil.copy2(test.Path(), debug_test_dir)
                            continue
                        os.makedirs(debug_test_dir)
                        shutil.copy2(test.Path(), debug_test_dir)
                if not flag:
                    debug_test_dir = os.path.join(measurements_debug_dir, "Other_Tests_" + test_file[-1].EndDateTime().strftime('%Y-%m-%d-%H-%M'))
                    if os.path.exists(debug_test_dir):
                        shutil.copy2(test.Path(), debug_test_dir)
                        continue
                    os.makedirs(debug_test_dir)
                    shutil.copy2(test.Path(), debug_test_dir)
        logger.debug('Test files:')
        for test_file in gapped_test_files:
            for test in test_file:
                logger.debug(os.path.basename(test.Path()))


if args.download:
    logger.info ( "Downloading SCC products" )
    
    to_download = list(set(to_download))
    
    for measurement_id in to_download:
        CURRENT_MEASUREMENT = measurement_id
        
        logger.debug ( "Waiting for processing to finish and downloading files...", extra={'scope': measurement_id} )
        
        try:
            result = scc.monitor_processing ( measurement_id, exit_if_missing = False )
            
            if result is not None:
                logger.debug ( "Processing finished", extra={'scope': measurement_id} )
                
                try:
                    scc_version = GetSCCVersion ( scc.output_dir, measurement_id )
                except Exception as e:
                    if result.elpp != 127:
                        logger.error ( "No SCC products found", extra={'scope': measurement_id} )
                        processing_log[ measurement_id ][ "downloaded" ] = "FALSE"
                        processing_log[ measurement_id ][ "result" ] = "No SCC products found"
                    else:
                        logger.error ( "Unknown error in SCC products", extra={'scope': measurement_id} )
                        processing_log[ measurement_id ][ "downloaded" ] = "FALSE"
                        processing_log[ measurement_id ][ "result" ] = "Unknown error in SCC products"
                    scc_version = "Unknown SCC Version! Check preprocessed NetCDF files."
                    continue
                    
                logger.info ( scc_version, extra={'scope': measurement_id} )
                swap_file.update_measurement_by_scc_id ( measurement_id, ("downloaded", True) )
                processing_log[ measurement_id ][ "downloaded" ] = "TRUE"
                processing_log[ measurement_id ][ "result" ] = os.path.abspath ( scc.output_dir )
                processing_log[ measurement_id ][ "scc_version" ] = scc_version
            else:
                logger.error ( "Download failed", extra={'scope': measurement_id} )
                processing_log[ measurement_id ][ "result" ] = "Error downloading SCC products"
        except Exception as e:
            logger.error ( f"Error downloading SCC products: {str(e)}" )
            processing_log[ measurement_id ][ "result" ] = "Error downloading SCC products"
            
if datalog is not None:
    if not os.path.isfile ( os.path.abspath ( datalog ) ):
        with open ( os.path.abspath ( datalog ), 'w' ) as csvfile:
            csvfile.write ( "Process Start,Data Folder,Data File,SCC System ID,Measurement ID,Uploaded,Downloaded,SCC Version,Result" )
            
    with open ( os.path.abspath ( datalog ), 'a' ) as csvfile:
        for measurement_id in processing_log.keys():
            csvfile.write ("\n%s,%s,%s,%s,%s,%s,%s,\"%s\",%s" % (
                processing_log[ measurement_id ][ "process_start" ],
                processing_log[ measurement_id ][ "data_folder" ],
                processing_log[ measurement_id ][ "data_file" ],
                processing_log[ measurement_id ][ "scc_system_id" ],
                measurement_id,
                processing_log[ measurement_id ][ "uploaded" ],
                processing_log[ measurement_id ][ "downloaded" ],
                processing_log[ measurement_id ][ "scc_version" ],
                processing_log[ measurement_id ][ "result" ]
            ))

# Delete swap file
swap_file.reset_measurements()
swap_file.save()
sys.exit (0)
