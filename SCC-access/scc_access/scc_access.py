import sys

import requests

try:
    import urllib.parse as urlparse  # Python 3
except ImportError:
    import urlparse  # Python 2

import argparse
import datetime
import logging
import os
import re
from io import BytesIO

import time

from zipfile import ZipFile

import yaml

import netCDF4 as netcdf

requests.packages.urllib3.disable_warnings()
logger = logging.getLogger(__name__)

# The regex to find the measurement id from the measurement page
# This should be read from the uploaded file, but would require an extra NetCDF module.
regex = "<h3>Measurement (?P<measurement_id>.{12,15}) <small>"  # {12, 15} to handle both old- and new-style measurement ids.


class SCC:
    """A simple class that will attempt to upload a file on the SCC server.

    The uploading is done by simulating a normal browser session. In the current
    version no check is performed, and no feedback is given if the upload
    was successful. If everything is setup correctly, it will work.
    """

    def __init__(self, auth, output_dir, base_url):

        self.auth = auth
        self.output_dir = output_dir
        self.base_url = base_url
        self.session = requests.Session()
        self.session.auth = auth
        self.session.verify = False

        self.login_url = urlparse.urljoin(self.base_url, 'accounts/login/')
        self.logout_url = urlparse.urljoin(self.base_url, 'accounts/logout/')
        self.list_measurements_url = urlparse.urljoin(self.base_url, 'data_processing/measurements/')

        self.upload_url = urlparse.urljoin(self.base_url, 'data_processing/measurements/quick/')
        self.measurement_page_pattern = urlparse.urljoin(self.base_url, 'data_processing/measurements/{0}/')
        self.download_hirelpp_pattern = urlparse.urljoin(self.base_url,
                                                             'data_processing/measurements/{0}/download-hirelpp/')
        self.download_cloudmask_pattern = urlparse.urljoin(self.base_url,
                                                         'data_processing/measurements/{0}/download-cloudmask/')

        self.download_elpp_pattern = urlparse.urljoin(self.base_url,
                                                              'data_processing/measurements/{0}/download-preprocessed/')
        self.download_elda_pattern = urlparse.urljoin(self.base_url,
                                                         'data_processing/measurements/{0}/download-optical/')
        self.download_plots_pattern = urlparse.urljoin(self.base_url,
                                                       'data_processing/measurements/{0}/download-plots/')
        self.download_elic_pattern = urlparse.urljoin(self.base_url,
                                                       'data_processing/measurements/{0}/download-elic/')
        self.delete_measurement_pattern = urlparse.urljoin(self.base_url, 'admin/database/measurements/{0}/delete/')

        self.api_base_url = urlparse.urljoin(self.base_url, 'api/v1/')
        self.api_measurement_pattern = urlparse.urljoin(self.api_base_url, 'measurements/{0}/')
        self.api_measurements_url = urlparse.urljoin(self.api_base_url, 'measurements')
        self.api_sounding_search_pattern = urlparse.urljoin(self.api_base_url, 'sounding_files/?filename={0}')
        self.api_lidarratio_search_pattern = urlparse.urljoin(self.api_base_url, 'lidarratio_files/?filename={0}')
        self.api_overlap_search_pattern = urlparse.urljoin(self.api_base_url, 'overlap_files/?filename={0}')

    def login(self, credentials):
        """ Login to SCC. """
        logger.debug("Attempting to login to SCC, username %s." % credentials[0])
        login_credentials = {'username': credentials[0],
                             'password': credentials[1]}

        logger.debug("Accessing login page at %s." % self.login_url)

        # Get upload form
        login_page = self.session.get(self.login_url)

        if not login_page.ok:
            raise self.PageNotAccessibleError('Could not access login pages. Status code %s' % login_page.status_code)

        logger.debug("Submitting credentials.")
        # Submit the login data
        login_submit = self.session.post(self.login_url,
                                         data=login_credentials,
                                         headers={'X-CSRFToken': login_page.cookies['csrftoken'],
                                                  'referer': self.login_url})
        return login_submit

    def logout(self):
        """ Logout from SCC """
        return self.session.get(self.logout_url, stream=True)

    def upload_file(self, filename, system_id, force_upload, delete_related, delay=0, rs_filename=None, ov_filename=None, lr_filename=None):
        """ Upload a filename for processing with a specific system. If the
        upload is successful, it returns the measurement id. """
        measurement_id = self.measurement_id_from_file(filename)

        logger.debug('Checking if a measurement with the same id already exists on the SCC server.')
        existing_measurement, _ = self.get_measurement(measurement_id)

        if existing_measurement:
            if force_upload:
                logger.info(
                    "Measurement with id {} already exists on the SCC. Trying to delete it...".format(measurement_id))
                self.delete_measurement(measurement_id, delete_related)
            else:
                logger.error(
                    "Measurement with id {} already exists on the SCC. Use --force_upload flag to overwrite it.".format(
                        measurement_id))
                # TODO: Implement handling at the proper place. This does not allow the SCC class to be used by external programs.
                sys.exit(1)

        # Get submit page
        upload_page = self.session.get(self.upload_url)

        # Submit the data
        upload_data = {'system': system_id,
                       'delay': delay}

        logger.debug("Submitted processing parameters - System: {}, Delay: {}".format(system_id, delay))

        files = {'data': open(filename, 'rb')}

        if rs_filename is not None:
            ancillary_file, _ = self.get_ancillary(rs_filename, 'sounding')

            if ancillary_file.already_on_scc:
                logger.warning("Sounding file {0.filename} already on the SCC with id {0.id}. Ignoring it.".format(ancillary_file))
            else:
                logger.debug('Adding sounding file %s' % rs_filename)
                files['sounding_file'] = open(rs_filename, 'rb')

        if ov_filename is not None:
            ancillary_file, _ = self.get_ancillary(ov_filename, 'overlap')

            if ancillary_file.already_on_scc:
                logger.warning("Overlap file {0.filename} already on the SCC with id {0.id}. Ignoring it.".format(ancillary_file))
            else:
                logger.debug('Adding overlap file %s' % ov_filename)
                files['overlap_file'] = open(ov_filename, 'rb')

        if lr_filename is not None:
            ancillary_file, _ = self.get_ancillary(lr_filename, 'lidarratio')

            if ancillary_file.already_on_scc:
                logger.warning(
                    "Lidar ratio file {0.filename} already on the SCC with id {0.id}. Ignoring it.".format(ancillary_file))
            else:
                logger.debug('Adding lidar ratio file %s' % lr_filename)
                files['lidar_ratio_file'] = open(lr_filename, 'rb')

        logger.info("Uploading of file %s started." % filename)

        upload_submit = self.session.post(self.upload_url,
                                          data=upload_data,
                                          files=files,
                                          headers={'X-CSRFToken': upload_page.cookies['csrftoken'],
                                                   'referer': self.upload_url})

        if upload_submit.status_code != 200:
            logger.warning("Connection error. Status code: %s" % upload_submit.status_code)
            return False

        # Check if there was a redirect to a new page.
        if upload_submit.url == self.upload_url:
            measurement_id = False
            logger.error("Uploaded file(s) rejected! Try to upload manually to see the error.")
        else:
            measurement_id = re.findall(regex, upload_submit.text)[0]
            logger.info("Successfully uploaded measurement with id %s." % measurement_id)
            logger.info("You can monitor the processing progress online: {}".format(self.measurement_page_pattern.format(measurement_id)))
        return measurement_id

    @staticmethod
    def measurement_id_from_file(filename):
        """ Get the measurement id from the input file. """

        if not os.path.isfile(filename):
            logger.error("File {} does not exist.".format(filename))
            sys.exit(1)

        with netcdf.Dataset(filename) as f:
            try:
                measurement_id = f.Measurement_ID
            except AttributeError:
                logger.error(
                    "Input file {} does not contain a Measurement_ID global attribute. Wrong file format?".format(
                        filename))
                sys.exit(1)

        return measurement_id

    def download_files(self, measurement_id, subdir, download_url):
        """ Downloads some files from the download_url to the specified
        subdir. This method is used to download preprocessed file, optical
        files etc.
        """
        # TODO: Make downloading more robust (e.g. in case that files do not exist on server).
        # Get the file
        request = self.session.get(download_url, stream=True)

        if not request.ok:
            raise Exception("Could not download files for measurement '%s'" % measurement_id)

        # Create the dir if it does not exist
        local_dir = os.path.join(self.output_dir, measurement_id, subdir)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # Save the file by chunk, needed if the file is big.
        memory_file = BytesIO()

        for chunk in request.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                memory_file.write(chunk)
                memory_file.flush()

        zip_file = ZipFile(memory_file)

        for ziped_name in zip_file.namelist():
            basename = os.path.basename(ziped_name)

            local_file = os.path.join(local_dir, basename)

            with open(local_file, 'wb') as f:
                f.write(zip_file.read(ziped_name))

    def download_hirelpp(self, measurement_id):
        """ Download hirelpp files for the measurement id. """
        # Construct the download url
        download_url = self.download_hirelpp_pattern.format(measurement_id)
        self.download_files(measurement_id, 'hirelpp', download_url)


    def download_cloudmask(self, measurement_id):
        """ Download cloudmask files for the measurement id. """
        # Construct the download url
        download_url = self.download_cloudmask_pattern.format(measurement_id)
        self.download_files(measurement_id, 'cloudscreen', download_url)


    def download_elpp(self, measurement_id):
        """ Download preprocessed files for the measurement id. """
        # Construct the download url
        download_url = self.download_elpp_pattern.format(measurement_id)
        self.download_files(measurement_id, 'elpp', download_url)


    def download_elda(self, measurement_id):
        """ Download optical files for the measurement id. """
        # Construct the download url
        download_url = self.download_elda_pattern.format(measurement_id)
        self.download_files(measurement_id, 'elda', download_url)


    def download_plots(self, measurement_id):
        """ Download profile graphs for the measurement id. """
        # Construct the download url
        download_url = self.download_plots_pattern.format(measurement_id)
        self.download_files(measurement_id, 'elda_plots', download_url)


    def download_elic(self, measurement_id):
        """ Download ELIC files for the measurement id. """
        # Construct the download url
        download_url = self.download_elic_pattern.format(measurement_id)
        self.download_files(measurement_id, 'elic', download_url)


    def download_eldec(self, measurement_id):
        """ Download ELDEC files for the measurement id. """
        # Construct the download url
        download_url = self.download_elda_pattern.format(measurement_id)  # ELDA patter is used for now
        self.download_files(measurement_id, 'eldec', download_url)


    def rerun_elpp(self, measurement_id, monitor=True):
        logger.debug("Started rerun_elpp procedure.")

        logger.debug("Getting measurement %s" % measurement_id)
        measurement, status = self.get_measurement(measurement_id)

        if measurement:
            logger.debug("Attempting to rerun ElPP through %s." % measurement.rerun_all_url)
            request = self.session.get(measurement.rerun_elpp_url, stream=True)

            if request.status_code != 200:
                logger.error(
                    "Could not rerun processing for %s. Status code: %s" % (measurement_id, request.status_code))
            else:
                logger.info("Rerun-elpp command submitted successfully for id {}.".format(measurement_id))

            if monitor:
                self.monitor_processing(measurement_id)

    def rerun_all(self, measurement_id, monitor=True):
        logger.debug("Started rerun_all procedure.")

        logger.debug("Getting measurement %s" % measurement_id)
        measurement, status = self.get_measurement(measurement_id)

        if measurement:
            logger.debug("Attempting to rerun all processing through %s." % measurement.rerun_all_url)

            request = self.session.get(measurement.rerun_all_url, stream=True)

            if request.status_code != 200:
                logger.error("Could not rerun pre processing for %s. Status code: %s" %
                             (measurement_id, request.status_code))
            else:
                logger.info("Rerun-all command submitted successfully for id {}.".format(measurement_id))

            if monitor:
                self.monitor_processing(measurement_id)

    def process(self, filename, system_id, monitor,  force_upload, delete_related, delay=0, rs_filename=None, lr_filename=None, ov_filename=None):
        """ Upload a file for processing and wait for the processing to finish.
        If the processing is successful, it will download all produced files.
        """
        logger.info("--- Processing started on %s. ---" % datetime.datetime.now())
        # Upload file
        logger.info("Uploading file.")
        measurement_id = self.upload_file(filename, system_id, force_upload, delete_related,
                                          delay=delay,
                                          rs_filename=rs_filename,
                                          lr_filename=lr_filename,
                                          ov_filename=ov_filename)

        if monitor and (delay > 0):
            logger.warning("Will not start monitoring, since a delay was specified: {} hours.".format(delay))
            return None

        if measurement_id and monitor:
            logger.info("Monitoring processing.")
            return self.monitor_processing(measurement_id)

        return None

    def monitor_processing(self, measurement_id):
        """ Monitor the processing progress of a measurement id"""

        # try to deal with error 404
        error_count = 0
        error_max = 3
        time_sleep = 3

        # try to wait for measurement to appear in API
        measurement = None
        logger.info("Looking for measurement %s on the SCC.", measurement_id)
        while error_count < error_max:
            time.sleep(time_sleep)
            measurement, status = self.get_measurement(measurement_id)
            if status != 200 and error_count < error_max:
                logger.error("Measurement not found. waiting %ds", time_sleep)
                error_count += 1
            else:
                break

        if error_count == error_max:
            logger.critical("Measurement %s doesn't seem to exist", measurement_id)
            sys.exit(1)

        logger.info('Measurement %s found.', measurement_id)

        if measurement is not None:
            while not measurement.has_finished:
                measurement.log_processing_status()
                time.sleep(10)
                measurement, status = self.get_measurement(measurement_id)

            logger.info("Measurement processing finished.")
            measurement.log_detailed_status()

            if measurement.hirelpp == 127:
                logger.info("Downloading HiRElPP files.")
                self.download_hirelpp(measurement_id)
            if measurement.cloudmask == 127:
                logger.info("Downloading cloud screening files.")
                self.download_cloudmask(measurement_id)
            if measurement.elpp == 127:
                logger.info("Downloading ElPP files.")
                self.download_elpp(measurement_id)
            if measurement.elda == 127:
                logger.info("Downloading ELDA files.")
                self.download_elda(measurement_id)
                logger.info("Downloading ELDA plots.")
                self.download_plots(measurement_id)
            if measurement.elic == 127:
                logger.info("Downloading ELIC files.")
                self.download_elic(measurement_id)
            if measurement.is_calibration and measurement.eldec==0:
                logger.info("Downloading ELDEC files.")
                self.download_eldec(measurement_id)
            logger.info("--- Processing finished. ---")

        return measurement

    def get_measurement(self, measurement_id):

        if measurement_id is None:  # Is this still required?
            return None

        measurement_url = self.api_measurement_pattern.format(measurement_id)
        logger.debug("Measurement API URL: %s" % measurement_url)

        response = self.session.get(measurement_url)

        response_dict = None

        if response.status_code == 200:
            response_dict = response.json()
        elif response.status_code == 404:
            logger.info("No measurement with id %s found on the SCC." % measurement_id)
        else:
            logger.error('Could not access API. Status code %s.' % response.status_code)

        # TODO: Implement better handling for status 401.

        if response_dict:
            measurement = Measurement(self.base_url, response_dict)
        else:
            measurement = None

        return measurement, response.status_code

    def delete_measurement(self, measurement_id, delete_related):
        """ Deletes a measurement with the provided measurement id. The user
        should have the appropriate permissions.

        The procedures is performed directly through the web interface and
        NOT through the API.
        """
        # Get the measurement object
        measurement, _ = self.get_measurement(measurement_id)

        # Check that it exists
        if measurement is None:
            logger.warning("Nothing to delete.")
            return None

        # Go the the page confirming the deletion
        delete_url = self.delete_measurement_pattern.format(measurement_id)

        confirm_page = self.session.get(delete_url)

        # Check that the page opened properly
        if confirm_page.status_code != 200:
            logger.warning("Could not open delete page. Status: {0}".format(confirm_page.status_code))
            return None

        # Get the delete related value
        if delete_related:
            delete_related_option = 'delete_related'
        else:
            delete_related_option = 'not_delete_related'

        # Delete the measurement
        delete_page = self.session.post(delete_url,
                                        data={'post': 'yes',
                                              'select_delete_related_measurements': delete_related_option},
                                        headers={'X-CSRFToken': confirm_page.cookies['csrftoken'],
                                                 'referer': delete_url}
                                        )
        if not delete_page.ok:
            logger.warning("Something went wrong. Delete page status: {0}".format(
                delete_page.status_code))
            return None

        logger.info("Deleted measurement {0}".format(measurement_id))
        return True

    def available_measurements(self):
        """ Get a list of available measurement on the SCC. """
        response = self.session.get(self.api_measurements_url)
        response_dict = response.json()

        if response_dict:
            measurement_list = response_dict['objects']
            measurements = [Measurement(self.base_url, measurement_dict) for measurement_dict in measurement_list]
            logger.info("Found %s measurements on the SCC." % len(measurements))
        else:
            logger.warning("No response received from the SCC when asked for available measurements.")
            measurements = None

        return measurements

    def list_measurements(self, station=None, system=None, start=None, stop=None, upload_status=None,
                          processing_status=None, optical_processing=None):

        # TODO: Change this to work through the API

        # Need to set to empty string if not specified, we won't get any results
        params = {
            "station": station if station is not None else "",
            "system": system if system is not None else "",
            "stop": stop if stop is not None else "",
            "start": start if start is not None else "",
            "upload_status": upload_status if upload_status is not None else "",
            "preprocessing_status": processing_status if processing_status is not None else "",
            "optical_processing_status": optical_processing if optical_processing is not None else ""
        }

        response_txt = self.session.get(self.list_measurements_url, params=params).text
        tbl_rgx = re.compile(r'<table id="measurements">(.*?)</table>', re.DOTALL)
        entry_rgx = re.compile(r'<tr>(.*?)</tr>', re.DOTALL)
        measurement_rgx = re.compile(
            r'.*?<td><a[^>]*>(\w+)</a>.*?<td>.*?<td>([\w-]+ [\w:]+)</td>.*<td data-order="([-]?\d+),([-]?\d+),([-]?\d+)".*',
            re.DOTALL)
        matches = tbl_rgx.findall(response_txt)
        if len(matches) != 1:
            return []

        ret = []
        for entry in entry_rgx.finditer(matches[0]):
            m = measurement_rgx.match(entry.string[entry.start(0):entry.end(0)])
            if m:
                name, date, upload, preproc, optical = m.groups()
                ret.append(
                    Measurement(self.base_url, {"id": name, "upload": int(upload), "pre_processing": int(preproc),
                                                "processing": int(optical)}))

        return ret

    def measurement_id_for_date(self, t1, call_sign, base_number=0):
        """ Give the first available measurement id on the SCC for the specific
        date.
        """
        date_str = t1.strftime('%Y%m%d')
        base_id = "%s%s" % (date_str, call_sign)
        search_url = urlparse.urljoin(self.api_base_url, 'measurements/?id__startswith=%s' % base_id)

        response = self.session.get(search_url)

        response_dict = response.json()

        measurement_id = None

        if response_dict:
            measurement_list = response_dict['objects']

            if len(measurement_list) == 100:
                raise ValueError('No available measurement id found.')

            existing_ids = [measurement_dict['id'] for measurement_dict in measurement_list]

            measurement_number = base_number
            measurement_id = "%s%02i" % (base_id, measurement_number)

            while measurement_id in existing_ids:
                measurement_number = measurement_number + 1
                measurement_id = "%s%02i" % (base_id, measurement_number)

        return measurement_id

    def get_ancillary(self, file_path, file_type):
        """
        Try to get the ancillary file data from the SCC API.

        The result will always be an API object. If the file does not exist, the .exists property is set to False.

        Parameters
        ----------
        file_path : str
           Path  of the uploaded file.
        file_type : str
           Type of ancillary file. One of 'sounding', 'overlap', 'lidarratio'.

        Returns
        : AncillaryFile
           The api object.
        """
        assert file_type in ['sounding', 'overlap', 'lidarratio']

        filename = os.path.basename(file_path)

        if file_type == 'sounding':
            file_url = self.api_sounding_search_pattern.format(filename)
        elif file_type == 'overlap':
            file_url = self.api_overlap_search_pattern.format(filename)
        else:
            file_url = self.api_lidarratio_search_pattern.format(filename)

        response = self.session.get(file_url)

        if not response.ok:
            logger.error('Could not access API. Status code %s.' % response.status_code)
            return None, response.status_code

        response_dict = response.json()
        object_list = response_dict['objects']

        logger.debug("Ancillary file JSON: {0}".format(object_list))

        if object_list:
            ancillary_file = AncillaryFile(self.api_base_url, object_list[0])  # Assume only one file is returned
        else:
            ancillary_file = AncillaryFile(self.api_base_url, None)  # Create an empty object

        return ancillary_file, response.status_code

    def __enter__(self):
        return self

    def __exit__(self, *args):
        logger.debug("Closing SCC connection session.")
        self.session.close()

    class PageNotAccessibleError(RuntimeError):
        pass


class ApiObject(object):
    """ A generic class object. """

    def __init__(self, base_url, dict_response):
        self.base_url = base_url

        if dict_response:
            # Add the dictionary key value pairs as object properties
            for key, value in dict_response.items():
                # logger.debug('Setting key {0} to value {1}'.format(key, value))
                try:
                    setattr(self, key, value)
                except:
                    logger.warning('Could not set attribute {0} to value {1}'.format(key, value))
            self.exists = True
        else:
            self.exists = False


class Measurement(ApiObject):
    """ This class represents the measurement object as returned in the SCC API.
    """

    def __init__(self, base_url, dict_response):

        # Define expected attributes to assist debugging

        self.hirelpp = None
        self.hirelpp_exit_code = None
        self.cloudmask = None
        self.cloudmask_exit_code = None
        self.elpp = None
        self.elpp_exit_code = None
        self.elda = None
        self.elda_exit_code = None
        self.elic = None
        self.elic_exit_code = None
        self.eldec = None
        self.eldec_exit_code = None
        self.elquick = None
        self.elquick_exit_code = None

        self.id = None
        self.is_calibration = None
        self.is_running = None

        self.resource_uri = None
        self.start = None
        self.stop = None
        self.system = None
        self.upload = None

        super().__init__(base_url, dict_response)

    @property
    def has_finished(self):
        """ Temporary implementation for SCC version 5.2.0, until the API returns a flag indicating if the
        processing measurement has finished. """
        if (self.is_running is False) and (self.hirelpp != 0 or self.elpp != 0):
            return True
        else:
            return False

    def log_processing_status(self):
        """ Log module status. """
        logger.info("Measurement is being processed. Status: {}, {}, {}, {}, {}, {}). Please wait.".format(
            self.upload,
            self.hirelpp,
            self.cloudmask,
            self.elpp,
            self.elda,
            self.elic))

    def log_detailed_status(self):
        """ Log module exit and status codes."""
        logger.info("Measurement exit status:".format(self.id))
        if self.is_calibration:
            self._log_module_status('ElPP', self.elpp, self.elpp_exit_code)
            self._log_module_status('ElDEC', self.eldec, self.eldec_exit_code)
        else:
            self._log_module_status('HiRElPP', self.hirelpp, self.hirelpp_exit_code)
            self._log_module_status('CloudScreen', self.cloudmask, self.cloudmask_exit_code)
            self._log_module_status('ElPP', self.elpp, self.elpp_exit_code)
            self._log_module_status('ELDA', self.elda, self.elda_exit_code)
            self._log_module_status('ELIC', self.elic, self.elic_exit_code)
            self._log_module_status('ELQuick', self.elquick, self.elquick_exit_code)

    def _log_module_status(self, name, status, exit_code):
        if exit_code:
            if exit_code['exit_code'] > 0:
                logger.warning("{0} exit code: {2[exit_code]} - {2[description]}".format(name, status, exit_code))
            else:
                logger.info("{0} exit code: {2[exit_code]} - {2[description]}".format(name, status, exit_code))
        else:
            logger.info("{0} exit code: {2}".format(name, status, exit_code))

    @property
    def rerun_elda_url(self):
        url_pattern = urlparse.urljoin(self.base_url, 'data_processing/measurements/{0}/rerun-elda/')
        return url_pattern.format(self.id)

    @property
    def rerun_elpp_url(self):
        url_pattern = urlparse.urljoin(self.base_url, 'data_processing/measurements/{0}/rerun-elpp/')
        return url_pattern.format(self.id)

    @property
    def rerun_all_url(self):
        ulr_pattern = urlparse.urljoin(self.base_url, 'data_processing/measurements/{0}/rerun-all/')
        return ulr_pattern.format(self.id)

    def __str__(self):
        return "Measurement {}".format(self.id)


class AncillaryFile(ApiObject):
    """ This class represents the ancilalry file object as returned in the SCC API.
    """
    @property
    def already_on_scc(self):
        if self.exists is False:
            return False

        return not self.status == 'missing'

    def __str__(self):
        return "%s: %s, %s" % (self.id,
                               self.filename,
                               self.status)


def process_file(filename, system_id, settings, force_upload, delete_related,
                 delay=0, monitor=True, rs_filename=None, lr_filename=None, ov_filename=None):
    """ Shortcut function to process a file to the SCC. """
    logger.info("Processing file %s, using system %s" % (filename, system_id))

    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        measurement = scc.process(filename, system_id,
                                  force_upload=force_upload,
                                  delete_related=delete_related,
                                  delay=delay,
                                  monitor=monitor,
                                  rs_filename=rs_filename,
                                  lr_filename=lr_filename,
                                  ov_filename=ov_filename)
        scc.logout()
    return measurement


def delete_measurements(measurement_ids, delete_related, settings):
    """ Shortcut function to delete measurements from the SCC. """
    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        for m_id in measurement_ids:
            logger.info("Deleting %s." % m_id)
            scc.delete_measurement(m_id, delete_related)
        scc.logout()


def rerun_all(measurement_ids, monitor, settings):
    """ Shortcut function to rerun measurements from the SCC. """

    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        for m_id in measurement_ids:
            logger.info("Rerunning all products for %s." % m_id)
            scc.rerun_all(m_id, monitor)
        scc.logout()


def rerun_processing(measurement_ids, monitor, settings):
    """ Shortcut function to delete a measurement from the SCC. """

    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        for m_id in measurement_ids:
            logger.info("Rerunning (optical) processing for %s" % m_id)
            scc.rerun_elpp(m_id, monitor)
        scc.logout()


def list_measurements(settings, station=None, system=None, start=None, stop=None, upload_status=None,
                      preprocessing_status=None,
                      optical_processing=None):
    """List all available measurements"""
    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        ret = scc.list_measurements(station=station, system=system, start=start, stop=stop, upload_status=upload_status,
                                    processing_status=preprocessing_status, optical_processing=optical_processing)
        for entry in ret:
            print("%s" % entry.id)
        scc.logout()


def download_measurements(measurement_ids, download_preproc, download_optical, download_graph, settings):
    """Download all measurements for the specified IDs"""
    with SCC(settings['basic_credentials'], settings['output_dir'], settings['base_url']) as scc:
        scc.login(settings['website_credentials'])
        for m_id in measurement_ids:
            if download_preproc:
                logger.info("Downloading preprocessed files for '%s'" % m_id)
                scc.download_elpp(m_id)
                logger.info("Complete")
            if download_optical:
                logger.info("Downloading optical files for '%s'" % m_id)
                scc.download_elda(m_id)
                logger.info("Complete")
            if download_graph:
                logger.info("Downloading profile graph files for '%s'" % m_id)
                scc.download_plots(m_id)
                logger.info("Complete")
        scc.logout()


def settings_from_path(config_file_path):
    """ Read the configuration file.

    The file should be in YAML syntax."""

    if not os.path.isfile(config_file_path):
        raise argparse.ArgumentTypeError("Wrong path for configuration file (%s)" % config_file_path)

    with open(config_file_path) as yaml_file:
        try:
            settings = yaml.safe_load(yaml_file)
            logger.debug("Read settings file(%s)" % config_file_path)
        except Exception:
            raise argparse.ArgumentTypeError("Could not parse YAML file (%s)" % config_file_path)

    # YAML limitation: does not read tuples
    settings['basic_credentials'] = tuple(settings['basic_credentials'])
    settings['website_credentials'] = tuple(settings['website_credentials'])
    return settings


# Setup for command specific parsers
def setup_delete(parser):
    def delete_from_args(parsed):
        delete_measurements(parsed.IDs,
                            delete_related=False,
                            settings=parsed.config)

    parser.add_argument("IDs", nargs="+", help="measurement IDs to delete.")
    parser.set_defaults(execute=delete_from_args)


def setup_rerun_all(parser):
    def rerun_all_from_args(parsed):
        rerun_all(parsed.IDs, parsed.process, parsed.config)

    parser.add_argument("IDs", nargs="+", help="Measurement IDs to rerun.")
    parser.add_argument("-p", "--process", help="Wait for the results of the processing.",
                        action="store_true")
    parser.set_defaults(execute=rerun_all_from_args)


def setup_rerun_elpp(parser):
    def rerun_processing_from_args(parsed):
        rerun_processing(parsed.IDs, parsed.process, parsed.config)

    parser.add_argument("IDs", nargs="+", help="Measurement IDs to rerun the processing on.")
    parser.add_argument("-p", "--process", help="Wait for the results of the processing.",
                        action="store_true")
    parser.set_defaults(execute=rerun_processing_from_args)


def setup_upload_file(parser):
    """ Upload but do not monitor processing progress. """
    def upload_file_from_args(parsed):
        process_file(parsed.filename, parsed.system, parsed.config,
                     delay=parsed.delay,
                     monitor=parsed.process,
                     force_upload=parsed.force_upload,
                     delete_related=False,  # For now, use this as default
                     rs_filename=parsed.radiosounding,
                     ov_filename=parsed.overlap,
                     lr_filename=parsed.lidarratio)

    def delay(arg):
        try:
            int_arg = int(arg)
        except ValueError:
            raise argparse.ArgumentTypeError("Could not convert delay argument {} to integer.".format(arg))

        if 0 <= int_arg <= 96:
            return int_arg
        else:
            raise argparse.ArgumentTypeError("Delay should be an integer between 0 and 96.")

    parser.add_argument("filename", help="Measurement file name or path.")
    parser.add_argument("system", help="Processing system id.")
    parser.add_argument("--delay", help="Delay processing by the specified number of hours (0 to 96).",
                        default=0, type=delay)
    parser.add_argument("-p", "--process", help="Wait for the processing results.",
                        action="store_true")
    parser.add_argument("--force_upload", help="If measurement ID exists on SCC, delete before uploading.",
                        action="store_true")
    parser.add_argument("--radiosounding", default=None, help="Radiosounding file name or path")
    parser.add_argument("--overlap", default=None, help="Overlap file name or path")
    parser.add_argument("--lidarratio", default=None, help="Lidar ratio file name or path")

    parser.set_defaults(execute=upload_file_from_args)


def setup_list_measurements(parser):
    def list_measurements_from_args(parsed):
        # TODO: Fix this
        logger.warning("This method needs to be updated. Cross-chceck any results.")

        list_measurements(parsed.config, station=parsed.station, system=parsed.system, start=parsed.start,
                          stop=parsed.stop,
                          upload_status=parsed.upload_status, preprocessing_status=parsed.preprocessing_status,
                          optical_processing=parsed.optical_processing_status)

    def status(arg):
        if -127 <= int(arg) <= 127:
            return arg
        else:
            raise argparse.ArgumentTypeError("Status must be between -127 and 127")

    def date(arg):
        if re.match(r'\d{4}-\d{2}-\d{2}', arg):
            return arg
        else:
            raise argparse.ArgumentTypeError("Date must be in format 'YYYY-MM-DD'")

    parser.add_argument("--station", help="Filter for only the selected station")
    parser.add_argument("--system", help="Filter for only the selected station")
    parser.add_argument("--start", help="Filter for only the selected station", type=date)
    parser.add_argument("--stop", help="Filter for only the selected station", type=date)
    parser.add_argument("--upload-status", help="Filter for only the selected station", type=status)
    parser.add_argument("--preprocessing-status", help="Filter for only the selected station", type=status)
    parser.add_argument("--optical-processing-status", help="Filter for only the selected station", type=status)
    parser.set_defaults(execute=list_measurements_from_args)


def setup_download_measurements(parser):
    def download_measurements_from_args(parsed):
        # TODO: Fix this
        logger.warning("This method needs to be updated. Cross-chceck any results.")

        preproc = parsed.download_elpp
        optical = parsed.download_elda
        graphs = parsed.download_profile_graphs
        if not preproc and not graphs:
            optical = True
        download_measurements(parsed.IDs, preproc, optical, graphs, parsed.config)

    parser.add_argument("IDs", help="Measurement IDs that should be downloaded.", nargs="+")
    parser.add_argument("--download-preprocessed", action="store_true", help="Download preprocessed files.")
    parser.add_argument("--download-optical", action="store_true",
                        help="Download optical files (default if no other download is used).")
    parser.add_argument("--download-profile-graphs", action="store_true", help="Download profile graph files.")
    parser.set_defaults(execute=download_measurements_from_args)


def main():
    # Define the command line arguments.
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    delete_parser = subparsers.add_parser("delete", help="Deletes a measurement.")
    rerun_all_parser = subparsers.add_parser("rerun-all", help="Rerun all processing steps for the provided measurement IDs.")
    rerun_processing_parser = subparsers.add_parser("rerun-elpp",
                                                    help="Rerun low-resolution processing steps for the provided measurement ID.")
    upload_file_parser = subparsers.add_parser("upload-file", help="Submit a file and, optionally, download the output products.")
    list_parser = subparsers.add_parser("list", help="List measurements registered on the SCC.")
    download_parser = subparsers.add_parser("download", help="Download selected measurements.")

    setup_delete(delete_parser)
    setup_rerun_all(rerun_all_parser)
    setup_rerun_elpp(rerun_processing_parser)

    setup_upload_file(upload_file_parser)
    setup_list_measurements(list_parser)
    setup_download_measurements(download_parser)

    # Verbosity settings from http://stackoverflow.com/a/20663028
    parser.add_argument('-d', '--debug', help="Print debugging information.", action="store_const",
                        dest="loglevel", const=logging.DEBUG, default=logging.INFO,
                        )
    parser.add_argument('-s', '--silent', help="Show only warning and error messages.", action="store_const",
                        dest="loglevel", const=logging.WARNING
                        )

    # Setup default config location
    home = os.path.expanduser("~")
    default_config_location = os.path.abspath(os.path.join(home, ".scc_access.yaml"))
    parser.add_argument("-c", "--config", help="Path to the config file.", type=settings_from_path,
                        default=default_config_location)

    args = parser.parse_args()

    # Get the logger with the appropriate level
    logging.basicConfig(format='%(levelname)s: %(message)s', level=args.loglevel)

    # Dispatch to appropriate function
    args.execute(args)


# When running through terminal
if __name__ == '__main__':
    main()
