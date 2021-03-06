import os
import sys
import requests
import re
from dateutil import parser
from dateutil.relativedelta import relativedelta as tdelta
import string
import random
from xml.etree import ElementTree

_NC_TYPES = ['CF',
    'CFMA']
   
# Node types (first 2 letters of node) that require trajectory DSG attributes and 
# variables 
_TRAJECTORY_NODE_TYPES = ['DP',
    'GL',
    'WF',
    'PG',
    'SP',
    'SF']
    
_VALID_RELATIVE_TIMEDELTA_TYPES = ('years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds')

def fetch_erddap_datasets(erddap_url, dataset_id=None, full_listing=False):
    '''Fetch the allDatasets.json request at the specified erddap_url.  Returns
    a list of dicts containing individual datasets.  A datetime object is added
    to each dataset if the dataset contains a valid minTime and/or maxTime provided
    skip_datetime is set to False <default=True>.'''
    
    datasets = []
    
    if full_listing:
        datasets_url = '{:s}/allDatasets.json'.format(erddap_url.strip('/'))
    else:
        datasets_url = '{:s}/allDatasets.json?datasetID%2CminTime%2CmaxTime%2Cclass'.format(erddap_url.strip('/'))
    
    if dataset_id:
        datasets_url = '{:s}&datasetID=%22{:s}%22'.format(datasets_url, dataset_id)
    
    try:
        r = requests.get(datasets_url)
    except requests.exceptions.MissingSchema as e:
        sys.stderr.write('{:s}\n'.format(e))
        return datasets
        
    if r.status_code != 200:
        sys.stderr.write('GET request failed: {:s}\n'.format(r.reason))
        return datasets
        
    # Get the json response
    try:
        response = r.json()
    except ValueError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e, datasets_url))
        return datasets
    
    # Regex to extract the deployment number
    deployment_regex = re.compile('d(\d{4,})$')
    
    col_count = range(len(response['table']['columnNames']))   
    # Create a list of dicts containing each dataset
    for row in response['table']['rows']:
        
        dataset = {response['table']['columnNames'][x]: row[x] for x in col_count}
        
        id_tokens = dataset['datasetID'].split('-')
        if len(id_tokens) != 7:
            #sys.stderr.write('Invalid datasetID: {:s}\n'.format(dataset['datasetID']))
            continue
        
        deployment_match = deployment_regex.search(id_tokens[6])
        if not deployment_match:
            sys.stderr.write('{:s}: Unknown deployment number\n'.format(dataset['datasetID']))
            continue
            
        reference_designator = '{:s}-{:s}-{:s}-{:s}'.format(id_tokens[0],
            id_tokens[1], 
            id_tokens[2],
            id_tokens[3])
        instrument = {'reference_designator' : reference_designator,
            'subsite' : id_tokens[0],
            'node' : id_tokens[1],
            'instrument' : '{:s}-{:s}'.format(id_tokens[2], id_tokens[3]),
            'method' : id_tokens[5],
            'stream' : id_tokens[4],
            'deployment_number' : int(deployment_match.groups()[0])}
        dataset['instrument'] = instrument
        
        # Add the erddap url
        dataset['erddap_base_url'] = erddap_url
            
        datasets.append(dataset)
        
    return datasets
    
def create_dataset_xml_filename(instrument, stream, telemetry, deployment_number):
    
    return '{:s}-{:s}-{:s}-deployment{:04.0f}.dataset.xml'.format(
        instrument,
        telemetry,
        stream,
        deployment_number)
        
def create_erddap_dataset_id(instrument, stream, telemetry, deployment_number):
    
    return '{:s}-{:s}-{:s}-d{:04.0f}'.format(
        instrument,
        stream,
        telemetry,
        deployment_number)

def create_dataset_xml(nc_dir, xml_template_file, dataset_id, title, summary, summary_append=True):
    
    # Validate input args
    if not os.path.isdir(nc_dir):
        sys.stderr.write('Invalid NetCDF directory: {:s}\n'.format(nc_dir))
        return
    if not os.path.isfile(xml_template_file):
        sys.stderr.write('Invalid dataset XML file: {:s}\n'.format(xml_template_file))
        return
        
    # Parse the xml template file
    doc = ElementTree.parse(xml_template_file)
    
    root = doc.getroot()
    # The root element must be 'dataset'
    if root.tag != 'dataset':
        sys.stderr.write('Invalid dataset XML template file (Root element must be of type=dataset): {:s}\n'.format(xml_template_file))
        return
        
    # 1. Create and set the <dataset datasetID=...> attribute
    if 'datasetID' not in root.keys():
        sys.stderr.write('dataset element is missing the datasetID attribute: {:s}\n'.format(xml_template_file))
        return
        
    # Set the attribute
    root.set('datasetID', dataset_id)
    
    # 2. Set the <fileDir> attribute to nc_dir
    file_dir_att = root.find('fileDir')
    if file_dir_att is None:
        sys.stderr.write('No <fileDir></> element found: {:s}\n'.format(xml_template_file))
        return
    file_dir_att.text = nc_dir
    
    # Select the <addAttributes></> element
    add_atts_e = root.find('addAttributes')
    if add_atts_e is None:
        sys.stderr.write('No <addAttributes></> element found: {:s}\n'.format(xml_template_file))
        return
        
    # 3. Add the <title></> attribute
    # Create the attribute element
    title_att_e = ElementTree.Element('att', {'name' : 'title'})
    title_att_e.text = title
    # Add the element to the DOM
    add_atts_e.append(title_att_e)

    # 4. Modify the <summary></>     
    # Get the list of name attribute values
    atts = add_atts_e.findall('att')
    att_names = [a.get('name') for a in atts]
    i = att_names.index('summary')
    if summary_append:
        summary = '{:s} {:s}'.format(atts[i].text, summary)
    atts[i].text = summary
    
    # Check the node type to see if we need to add trajectory DSG variables and attributes
    node_type = dataset_id.split('-')[1][:2]
    if node_type in _TRAJECTORY_NODE_TYPES:
        sys.stdout.write('{:s}: Adding Trajectory DSG elements\n'.format(dataset_id))
        
        traj_elements_dir = os.path.join(os.getenv('OOI_ERDDAP_ASYNC_HOME'),
            'config',
            'nc',
            'trajectory')
        if not os.path.isdir(traj_elements_dir):
            sys.stderr.write('Invalid trajectory elements directory: {:s}\n'.format(traj_elements_dir))
            return ElementTree.tostring(root)
        
        traj_atts_xml = os.path.join(traj_elements_dir, 'trajectory.atts.xml')
        if not os.path.isfile(traj_atts_xml):
            sys.stderr.write('Invalid trajectory attributes elements file: {:s}\n'.format(traj_atts_xml))
            return ElementTree.tostring(root)
        
        traj_var_xml = os.path.join(traj_elements_dir, 'trajectory.vars.xml')
        if not os.path.isfile(traj_var_xml):
            sys.stderr.write('Invalid trajectory variable element file: {:s}\n'.format(traj_var_xml))
            return ElementTree.tostring(root)
            
        traj_vars_doc = ElementTree.parse(traj_var_xml)
        traj_vars_root = traj_vars_doc.getroot()
        if traj_vars_root.tag != 'dataset':
            sys.stderr.write('{:s}: root tag must be a dataset element\n'.format(traj_var_xml))
            return ElementTree.tostring(root)
            
        traj_atts_doc = ElementTree.parse(traj_atts_xml)
        traj_root = traj_atts_doc.getroot()
        traj_atts = traj_root.findall('att')
        for traj_att in traj_atts:
            add_atts_e.append(traj_att)
            
        traj_var = traj_vars_root.find('dataVariable')
        source_name_e = traj_var.find('sourceName')
        source_name_e.text = '={:s}'.format(root.get('datasetID'))
        root.append(traj_var)

    
    # Return the string XML
    return ElementTree.tostring(root)

def create_frontend_dataset_xml(nc_dir, xml_template_file, dataset_id):
    
    # Validate input args
    if not os.path.isdir(nc_dir):
        sys.stderr.write('Invalid NetCDF directory: {:s}\n'.format(nc_dir))
        return
    if not os.path.isfile(xml_template_file):
        sys.stderr.write('Invalid dataset XML file: {:s}\n'.format(xml_template_file))
        return
        
    # Parse the xml template file
    doc = ElementTree.parse(xml_template_file)
    
    root = doc.getroot()
    # The root element must be 'dataset'
    if root.tag != 'dataset':
        sys.stderr.write('Invalid dataset XML template file (Root element must be of type=dataset): {:s}\n'.format(xml_template_file))
        return
        
    # 1. Create and set the <dataset datasetID=...> attribute
    if 'datasetID' not in root.keys():
        sys.stderr.write('dataset element is missing the datasetID attribute: {:s}\n'.format(xml_template_file))
        return
        
    # Set the attribute
    root.set('datasetID', dataset_id)
    
    # 2. Set the <fileDir> attribute to nc_dir
    file_dir_att = root.find('fileDir')
    if file_dir_att is None:
        sys.stderr.write('No <fileDir></> element found: {:s}\n'.format(xml_template_file))
        return
    file_dir_att.text = nc_dir
    
    # Return the string XML
    return ElementTree.tostring(root)
            
#def create_dataset_xml(reference_designator, stream, telemetry, deployment_number, nc_dir, xml_template_file, instrument_descriptions):
#    
#    # Validate input args
#    if not os.path.isdir(nc_dir):
#        sys.stderr.write('Invalid NetCDF directory: {:s}\n'.format(nc_dir))
#        return
#    if not os.path.isfile(xml_template_file):
#        sys.stderr.write('Invalid dataset XML file: {:s}\n'.format(xml_template_file))
#        return
#    if not instrument_descriptions or type(instrument_descriptions) != list:
#        sys.stderr.write('instrument_descriptions must be a non-empty array of visualocean instrument metadata\n')
#        return
#        
#    # Make sure the reference designator referers to an instrument in instrument_descriptions
#    instruments = [i['reference_designator'] for i in instrument_descriptions]
#    if reference_designator not in instruments:
#        sys.stderr.write('{:s}: No instrument metadata entry found\n'.format(reference_designator))
#        return
#        
#    instrument_meta = instrument_descriptions[instruments.index(reference_designator)]
#    
#    # Parse the xml template file
#    doc = parse(xml_template_file)
#    
#    root = doc.getroot()
#    # The root element must be 'dataset'
#    if root.tag != 'dataset':
#        sys.stderr.write('Invalid dataset XML template file (Root element must be of type=dataset): {:s}\n'.format(xml_template_file))
#        return
#        
#    # 1. Create and set the <dataset datasetID=...> attribute
#    if 'datasetID' not in root.keys():
#        sys.stderr.write('dataset element is missing the datasetID attribute: {:s}\n'.format(xml_template_file))
#        return
#    dataset_id = create_erddap_dataset_id(reference_designator,
#        stream,
#        telemetry,
#        deployment_number)
#    # Set the attribute
#    root.set('datasetID', dataset_id)
#    
#    # 2. Set the <fileDir> attribute to nc_dir
#    file_dir_att = root.find('fileDir')
#    if not file_dir_att:
#        sys.stderr.write('No <fileDir></> element found: {:s}\n'.format(xml_template_file))
#        return
#    file_dir_att = nc_dir
#    
#    # Select the <addAttributes></> element
#    add_atts_e = root.find('addAttributes')
#    if not add_atts_e:
#        sys.stderr.write('No <addAttributes></> element found: {:s}\n'.format(xml_template_file))
#        return
#        
#    # 3. Add the <title></> attribute
#    title_string = '{:s} {:s} {:s} {:s} data from the {:s} stream - Deployment {:04.0f}'.format(telemetry.capitalize(),
#        instrument_meta['site'],
#        instrument_meta['subsite'],
#        instrument_meta['name'],
#        stream,
#        deployment_number)
#    # Create the attribute element
#    title_att_e = Element('att', {'name' : 'title'})
#    # Add the element to the DOM
#    add_atts_e.append(title_att_e)
#
#    # 4. Modify the <summary></>     
#    # Get the list of name attribute values
#    atts = add_atts_e.findall('att')
#    att_names = [a.get('name') for a in atts]
#    i = att_names.index('summary')
#    atts[i].text = '{:s}. {:s}'.format(atts[i].text,
#        instrument_meta['description'])
#    
#    # Return the string XML
#    return ElementTree.tostring(root)
    
#def write_erddap_dataset_xml(dataset_xml_file, xml_template, dataset_id, nc_dest_dir, title=None, summary=None, clobber=False):
#    
#    if not os.path.isfile(xml_template):
#        sys.stderr.write('Invalid dataset XML template: {:s}\n'.format(xml_template))
#        return False
#    if not os.path.isdir(nc_dest_dir):
#        sys.stderr.write('Invalid ERDDAP NetCDF destination: {:s}\n'.format(nc_dest_dir))
#        return False
#        
#    if os.path.isfile(dataset_xml_file) and not clobber:
#        sys.stderr.write('Dataset XML file already exists (use clobber=True to overwrite): {:s}\n'.format(dataset_xml_file))
#        return False
#        
#    if not title:
#        title = dataset_id
#    if not summary:
#        summary = 'Data set gathered by the Ocean Observatories Initiative'
#        
#    dataset_xml = create_erddap_dataset_xml(xml_template,
#        dataset_id,
#        nc_dest_dir,
#        title,
#        summary)
#    if not dataset_xml:
#        return dataset_xml_file
#        
#    # Write the xml to disk
#    try:
#        with open(dataset_xml_file, 'w') as fid:
#            fid.write(dataset_xml)
#    except IOError as e:
#        sys.stderr.write('{:s}\n'.format(e))
#        return False
#        
#    return True
    
#def create_erddap_dataset_xml(template_file, dataset_id, nc_dest_dir, title, summary):
#    
#    if not os.path.isfile(template_file):
#        sys.stderr.write('Invalid XML template: {:s}\n'.format(template_file))
#        return None
#        
#    try:
#        with open(template_file, 'r') as fid:
#            template_xml = fid.read()
#    except IOError as e:
#        sys.stderr.write('{:s}\n'.format(e))
#        return None
#    
#    try:
#        dataset_xml = template_xml.format(dataset_id=dataset_id,
#            file_dir=nc_dest_dir,
#            title=title,
#            summary=summary)
#    except IndexError as e:
#        sys.stderr.write('Failed to write XML: {:s}\n'.format(e))
#        return None
#        
#    return dataset_xml
        
def get_new_erddap_datasets(erddap_datasets1, erddap_datasets2):
    '''Return the list of ERDDAP datasets present in erddap_datasets1 that are
    not present in erddap_datasets2, as determined by the absence of the datasetID'''
    
    new_datasets = []
    
    try:
        dest_dataset_ids = [d['datasetID'] for d in erddap_datasets2]
    except KeyError as e:
        sys.stderr.write('{:s}\n'.format(e))
        return new_datasets
        
    for dataset in erddap_datasets1:
        if dataset['datasetID'] not in dest_dataset_ids:
            new_datasets.append(dataset)
    
    return new_datasets
    
def get_updated_erddap_datasets(erddap_datasets1, erddap_datasets2, delta_seconds=0):
    '''Return the list of ERDDAP datasets contained in both erddap_datasets1 and 
    erddap_datasets2, provided the dataset start times and/or end times are not
    identical'''
    
    updated_datasets = []
    
    try:
        dest_dataset_ids = [d['datasetID'] for d in erddap_datasets2]
    except KeyError as e:
        sys.stderr.write('{:s}\n'.format(e))
        return updated_datasets
        
    # Loop through each dataset in erddap_datasets1.  Skip the dataset if it is
    # not present in erddap_datasets2.  If it is present, convert min/maxTime to
    # datetimes to compare them.
    for dataset in erddap_datasets1:
        if dataset['datasetID'] not in dest_dataset_ids:
            continue
            
        # Get the corresponding dataset in erddap_datasets2
        target_dataset = erddap_datasets2[dest_dataset_ids.index(dataset['datasetID'])]
        # Parse the min/maxTimes from both datasets
        try:
            min_dt1 = parser.parse(dataset['minTime'])
        except ValueError as e:
            sys.stderr.write('Date parse error ({:s}): {:s}\n'.format(e, dataset['minTime']))
            continue
            
        try:
            max_dt1 = parser.parse(dataset['maxTime'])
        except ValueError as e:
            sys.stderr.write('Date parse error ({:s}): {:s}\n'.format(e, dataset['maxTime']))
            continue
            
        try:
            min_dt2 = parser.parse(target_dataset['minTime'])
        except ValueError as e:
            sys.stderr.write('Date parse error ({:s}): {:s}\n'.format(e, target_dataset['minTime']))
            continue
            
            max_dt2 = parser.parse(target_dataset['maxTime'])
        except ValueError as e:
            sys.stderr.write('Date parse error ({:s}): {:s}\n'.format(e, target_dataset['maxTime']))
            continue
            
        delta_start = min_dt1 - min_dt2
        delta_end = max_dt1 - max_dt2
        
        # Compare the deltas to determine if the erddap_datasets1 dataset has
        # been updated
        if abs(delta_start.total_seconds()) > abs(delta_seconds):
            updated_datasets.append(dataset)
        elif abs(delta_end.total_seconds()) > abs(delta_seconds):
            updated_datasets.append(dataset)
            
    return updated_datasets
    
def download_erddap_nc(erddap_url, dataset_id, output_filename=None, nc_type=None, time_delta_type=None, time_delta_value=None, start_time=None, end_time=None, clobber=None, print_url=False):
    '''Download a flat, table-like, NetCDF-3 binary file for the specified datasetID,
    with COARDS/CF/ACDD metadata from the specified erddap_base_url.  The entire time 
    series is downloaded by default. 
    
    Params:
        erddap_base_url: base ERDDAP URL that points to the tabledap or griddap
        dataset_id: ERDDAP dataset ID
        
    Options:
        output_filename: name of the file that the NetCDF should be written to.  If
            not specified, the filename is constructed using the dataset id a
            randomly generated string and is written to the current working directory
        clobber: set to True to overwrite the file if it already exists <default=False>.
        nc_type: 'CF' or 'CFMA'.  See ERDDAP doco for the differences between the optional
            types.
        time_delta_type: Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta'
        time_delta_value: Positive integer value to subtract from the end time to get the start time for subsetting.
        start_time: string specifying the start of the time-series to download
        end_time: string specifying the end of the time-series to download
        print_url: set to True to print the request URL, but do not send the request.
    '''
    
    # Check the nc_type if specified
    if nc_type and not nc_type in _NC_TYPES:
        sys.stderr.write('Invalid nc_type parameter: {:s}\n'.format(nc_type))
        return
    
    # Fetch the ERDDAP dataset metadata to make sure the dataset exists
    dataset = fetch_erddap_datasets(erddap_url, dataset_id=dataset_id)
    if not dataset:
        sys.stderr.write('Invalid ERDDAP dataset (datasetID={:s})\n'.format(dataset_id))
        return
    dataset = dataset[0]
    
    # Create the request url base    
    request_url = '{:s}/{:s}.nc'.format(erddap_url.strip('/'),
        dataset_id)
    
    # Add the optional nc_type, if specified and valid    
    if nc_type:
        request_url = '{:s}{:s}'.format(request_url, nc_type)
    
    # The user can use request a file by either using the time_delta_type and time_delta_value
    # kwargs or specifying a begin_ts and/or end_ts date string.  
    if time_delta_type and not time_delta_value:
        sys.stderr.write('time_delta_type kwarg specified but time_delta_value kwarg is not set\n')
        return
    elif time_delta_value and not time_delta_type:
        sys.stderr.write('time_delta_value kwarg specified but time_delta_type kwarg is not set\n')
        return
    elif time_delta_type and time_delta_value:
        if time_delta_type not in _VALID_RELATIVE_TIMEDELTA_TYPES:
            sys.stderr.write('Invalid time_delta_type: {:s}\n'.format(time_delta_type))
            return
        
        # Parse the dataset['maxTime'] to datetime
        try:
            end_dt = parser.parse(dataset['maxTime'])
            start_dt = end_dt - tdelta(**dict({time_delta_type : time_delta_value}))
        except ValueError as e:
            sys.stderr.write('Time Delta Error: {:s}\n'.format(e))
            return
    else: 
        # If we don't have a end_dt and start_dt, look to see if the user specified
        # a begin_ts and/or end_ts
        if end_time:
            # Add Z to the end_time to ensure that it's parsed as a utc time
            if not end_time.endswith('Z'):
                end_time = '{:s}Z'.format(end_time)
                
            try:
                end_dt = parser.parse(end_time)
            except ValueError as e:
                sys.stderr.write('end_time parse error: {:s}\n'.format(e))
                return
        else:
            try:
                end_dt = parser.parse(dataset['maxTime'])
            except ValueError as e:
                sys.stderr.write('end_time parse error: {:s}\n'.format(e))
                return
                
        if start_time:
            # Add Z to the end_time to ensure that it's parsed as a utc time
            if not start_time.endswith('Z'):
                start_time = '{:s}Z'.format(start_time)
                
            try:
                start_dt = parser.parse(start_time)
            except ValueError as e:
                sys.stderr.write('start_time parse error: {:s}\n'.format(e))
                return
        else:
            try:
                start_dt = parser.parse(dataset['minTime'])
            except ValueError as e:
                sys.stderr.write('start_time parse error: {:s}\n'.format(e))
                return
                
    # Make sure the end_dt occurs after the start_dt
    if end_dt <= start_dt:
        sys.stderr.write('Requested end time occurs before the requested start time')
        return
                
    # We should have a start_dt and end_dt to add to the query, regardless of whether
    # the user specified time_delta* or start_time/end_time
    time_params = 'time>={:s}&time<={:s}'.format(start_dt.strftime('%Y-%m-%dT%H:%M:%SZ'),
        end_dt.strftime('%Y-%m-%dT%H:%M:%SZ'))
        
    request_url = '{:s}?&{:s}'.format(request_url, time_params)
    
    if print_url:
        sys.stderr.write('{:s}\n'.format(request_url))
        return
            
    # Create a random filename if none was specified
    if not output_filename:
        request_id = id_generator()
        if nc_type:
            output_filename = os.path.join(os.path.realpath(os.curdir), '{:s}-{:s}.nc{:s}.nc'.format(dataset_id, request_id, nc_type))
        else:
            output_filename = os.path.join(os.path.realpath(os.curdir), '{:s}-{:s}.nc'.format(dataset_id, request_id))
    
    # Make sure the filename does not exist
    if os.path.isdir(output_filename):
        if not clobber:
            sys.stderr.write('Output filename already exists (set clobber=True to overwrite): {:s}\n'.format(output_filename))
            return
    
    # Send the download request        
    r = requests.get(request_url, stream=True)
    if r.status_code != 200:
        sys.stderr.write('Download failed: {:s} (Reason={:s})\n'.format(request_url, r.reason))
        return
    
    # Chunk the response and write the NetCDF file
    try:    
        with open(output_filename, 'wb') as fid:
            for chunk in r.iter_content(chunk_size=1024):
                fid.write(chunk)
                fid.flush()
    except IOError as e:
        sys.stderr.write('{:s} (Query produced no matching rows?)\n'.format(e))
        r.close()
        return None
            
    return output_filename
            
def id_generator(size=16, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
    
