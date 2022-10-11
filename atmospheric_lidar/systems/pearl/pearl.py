import datetime
import os
import glob

import numpy as np

from ...generic import BaseLidarMeasurement, LidarChannel
from ..ciao import CiaoMixin

from . import pearl_netcdf_parameters
from report_file import Report_file


repository = '/mnt/storage/lidar_data/pearl/'


class PearlLidarMeasurement(CiaoMixin, BaseLidarMeasurement):
    
    extra_netcdf_parameters = pearl_netcdf_parameters
    
    def _import_file(self, filename):
        ''' Import a pearl file. '''
        
        if filename in self.files:
            print("File has been imported already:" + filename)
        else:
            parameters, channels_dict = self.read_pearl_data(filename)
            start_time = self._get_time(parameters['Acq_date'], parameters['Acq_start_time'])
            
            for channel_info in channels_dict.values():
                
                if channel_info['name'] == '1064ALR':
                    name = '1064'
                    tm = start_time
                elif channel_info['name'] == '1064BLR':
                    name = '1064'
                    tm = start_time + datetime.timedelta(seconds = 30)
                else:
                    name = channel_info['name']
                    tm = start_time
                if name not in self.channels:
                    self.channels[name] = LidarChannel(channel_info)
                self.channels[name].data[tm] = channel_info['data']
            self.files.append(filename)
    
    def read_pearl_data(self, filename):
        '''
        Reads a pearl file.
        
        Returns:
        parameters - a dictionary of general parameters
        channels   - a dictionary with keys the channel number and values lists
                     [channel name, channel bin width, channel data].
        '''
        f = open(filename,'r') # Open the file
        s = f.read(26) # Read the first 26 bytes
        
        #Get the values in a dictionary
        parameters = {}
        parameters['Acq_date'] = s[0:10] # First 10 bytes are the acquisition date.
        parameters['Acq_start_time'] = s[10:20].strip() # Next 10 bytes are start time. Strip from trailing spaces.
        parameters['Channel_no'] = np.fromstring(s[20:22], dtype = np.int16) # Next 2 bytes are the number of channels. Short integer.
        parameters['Point_no'] = np.fromstring(s[22:26], dtype = np.int32) # Next 4 bytes are the number of points. Integer.
        p = parameters # Just for less typing
        
        # Read the channel parameters
        len = 20*p['Channel_no']    
        s = f.read(len)
        channels = {}
        for (c1,n) in zip(list(range(0,len, 20)),list(range(p['Channel_no']))):
            channels[str(n)] = {'name' : s[c1+10:c1+20].strip(),
                                'binwidth' : s[c1:c1+10].strip()}
        
        #Read the data
        data = np.fromfile(f,dtype = np.float32)
        #print filename + ': ' + str(data.size) +',' + str(p['Point_no']) +str(p['Channel_no'])
        data = data.reshape(p['Point_no'],p['Channel_no'])
        for ch in channels.keys():
            channels[ch]['data'] = data[:,int(ch)]
        #Close the file
        f.close()
        return parameters,channels
                
    def _get_time(self, date_str, time_str):
        t = datetime.datetime.strptime(date_str + time_str, '%d/%m/%Y%H.%M.%S')
        return t

def get_measurement_for_interval(start_time, stop_time):
    ''' Searches for a pearl measurement based on a time interval     
    '''
    
    correct_series = None
    day = datetime.timedelta(hours = 24)
    
    if start_time > stop_time:
            raise ValueError('Stop time should be after start time')
    
    
    
    #The list of directories based on the given time. Same, previous, Next day
    possible_paths = [get_path(t) for t in [start_time - day, start_time, start_time + day] 
                            if get_path(t) is not None]
    for path in possible_paths:
        try:
            rf = Report_file(path)
        except:
            rf = None
        
        if rf is not None:
            for serie in rf.series:
                if (start_time > serie.starttime) and (stop_time < serie.endtime):
                    correct_series = serie
    
    if correct_series:
        files = correct_series.files.get('apd', []) + correct_series.files.get('mcb', [])
        m_series = PearlLidarMeasurement(files)
        m_subset = m_series.subset_by_time(start_time, stop_time)
        return m_subset
    else:
        return None


def get_channel(tim, channel = '1064'):
    if channel =='1064':
        extension = '*.apd'
    else:
        extension = '*.mcb'
    
    dirstr = get_path(tim)
    
    if not os.path.isdir(dirstr):
        raise IOError('No measurement for that date (directory does not exist.).')
        #No measurement for that date (directory does not exist.).
    files = glob.glob(dirstr + extension)
    m = PearlLidarMeasurement(files)
    c = m.channels[channel]
    return c
    

def get_path(tim):
    dirstr = repository +  tim.strftime('%Y')+ '/' +tim.strftime('%d%m%Y') + '/'
    return dirstr
