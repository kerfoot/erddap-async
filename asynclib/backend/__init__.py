import os
import sys
import json
import requests
import glob
import datetime
from dateutil import parser
from UFrame import UFrame

#def write_active_deployments_catalog(uframe, dest_dir=None, clobber=False):
#    '''Fetch and write the list of active instrument deployments and associated
#    metadata to a JSON formatted text file.  The file is written to the current
#    working directory by default.'''
#    
#    # Write the JSON object to file
#    if not dest_dir:
#        dest_dir = os.path.realpath(os.curdir)
#
#    # Validate that dest_dir exists
#    if not os.path.isdir(dest_dir):
#        sys.stderr.write('Invalid active deployments catalog destination specified: {:s}\n'.format(dest_dir))
#        return
#        
#    ## Create an instance of the uframe client
#    #uframe = UFrame(base_url=uframe_base_url)
#    
#    # Create the catalog name we will write to
#    catalog = os.path.join(dest_dir, '{:s}-active-catalog.json'.format(uframe.base_url[7:]))
#    if os.path.isfile(catalog):
#        if clobber:
#            sys.stderr.write('Clobbering existing catalog: {:s}\n'.format(catalog))
#        else:
#            sys.stderr.write('Catalog already exists: {:s} (Set clobber=True to overwrite)\n'.format(catalog))
#            return catalog
#    
#    # Get the list of all actively deployed instruments
#    events = uframe.get_active_deployments()
#    
#    # Write the events to the catalog
#    try:
#        with open(catalog, 'w') as fid:
#            json.dump(events, fid)    
#    except IOError as e:
#        sys.stderr.write('{:s}\n'.format(e))
#        return
#    
#    # Return the filename if it was successfully written
#    return catalog
    
#def fetch_erddap_datasets(erddap_url, full_listing=False, skip_datetime=True):
#    '''Fetch the allDatasets.json request at the specified erddap_url.  Returns
#    a list of dicts containing individual datasets.  A datetime object is added
#    to each dataset if the dataset contains a valid minTime and/or maxTime provided
#    skip_datetime is set to False <default=True>.'''
#    
#    if full_listing:
#        datasets_url = '{:s}/allDatasets.json'.format(erddap_url.strip('/'))
#    else:
#        datasets_url = '{:s}/allDatasets.json?datasetID%2CminTime%2CmaxTime'.format(erddap_url.strip('/'))
#    
#    try:
#        r = requests.get(datasets_url)
#    except requests.exceptions.MissingSchema as e:
#        sys.stderr.write('{:s}\n'.format(e))
#        return []
#        
#    if r.status_code != 200:
#        sys.stderr.write('GET request failed: {:s}\n'.format(r.reason))
#        return []
#        
#    # Get the json response
#    try:
#        response = r.json()
#    except ValueError as e:
#        sys.stderr.write('{:s}: {:s}\n'.format(e, datasets_url))
#        return []
#    
#    datasets = [] 
#    col_count = range(len(response['table']['columnNames']))   
#    # Create a list of dicts containing each dataset
#    for row in response['table']['rows']:
#        
#        dataset = {response['table']['columnNames'][x]: row[x] for x in col_count}
#        
#        id_tokens = dataset['datasetID'].split('-')
#        if len(id_tokens) != 6:
#            #sys.stderr.write('Invalid datasetID: {:s}\n'.format(dataset['datasetID']))
#            continue
#            
#        dataset['reference_designator'] = '{:s}-{:s}-{:s}-{:s}'.format(id_tokens[0],
#            id_tokens[1], 
#            id_tokens[2],
#            id_tokens[3])
#        dataset['telemetry'] = id_tokens[4]
#        dataset['stream'] = id_tokens[5]
#        
#        if not skip_datetime:
#            dataset['minTimeDt'] = None
#            dataset['maxTimeDt'] = None
#            
#            if dataset['minTime']:
#                dataset['minTimeDt'] = parser.parse(dataset['minTime'])
#                
#            if dataset['maxTime']:
#                dataset['maxTimeDt'] = parser.parse(dataset['maxTime'])
#            
#        datasets.append(dataset)
#        
#    return datasets
    
def get_updated_datasets(uframe, uframe_catalog_json, erddap_catalog_json, skip_csv=None):
    
    updated_datasets = []
    
    if skip_csv:
        sys.stderr.write('Write code to parse skip_csv for streams to skip\n')
        return
    
    # Create a list of ERDDAP datasetIDs
    erddap_dataset_ids = [d['datasetID'] for d in erddap_catalog_json]

    for instrument in uframe_catalog_json:
        
        # Create datetimes for the instrument deployment start and end times
        #deployment_dt0 = parser.parse(instrument['event_start_ts'])
        deployment_dt1 = None
        if instrument['event_stop_ts']:
            deployment_dt1 = parser.parse(instrument['event_stop_ts'])
        
        # Get the list of all streams produced by this instrument
        streams = uframe.instrument_to_streams(instrument['instrument']['reference_designator'])
        if not streams:
            sys.stderr.write('{:s}: No streams found\n'.format(instrument['instrument']['reference_designator']))
            continue
            
        # Check each stream in streams to see if an ERDDAP dataset exists
        for stream in streams:
            
            stream_dataset_id = '{:s}-{:s}-{:s}'.format(stream['reference_designator'],
                stream['method'],
                stream['stream'])
                
            # If the stream_dataset_id does not exist, this is a new stream, so 
            # we can skip it
            if stream_dataset_id not in erddap_dataset_ids:
                continue
                
            # Find the list index for the ERDDAP dataset
            i = erddap_dataset_ids.index(stream_dataset_id)
            
            # Create datetimes for the stream start and end times
            #stream_dt0 = parser.parse(stream['beginTime'])
            stream_dt1 = parser.parse(stream['endTime'])
            
            # Create datetimes for the erddap dataset minTime and maxTime
            #dataset_dt0 = parser.parse(erddap_catalog_json[i]['minTime'])
            # Add the stream_dt1 microseconds to the dataset_dt1 datetime object to 
            # account for the fact that UFrame keeps track of microseconds, but
            # ERDDAP dataset minTime values do not
            dataset_dt1 = parser.parse(erddap_catalog_json[i]['maxTime']) + datetime.timedelta(0,0,stream_dt1.microsecond)
            
            # Compare the stream end time to the ERDDAP dataset end time to see if
            # the stream has been updated
            stream_dt = stream_dt1 - dataset_dt1
            if stream_dt.seconds == 0:
                # stream has not been updated
                continue
                
            # Dataset needs to be updated: use the stream_dt1 if the instrument deployment
            # end date is None or is >= stream_dt1.  Use the deployment end date if the 
            # deployment end date is not None and is < stream_dt1
            query_params = {'uframe_url' : uframe.base_url,
                'ts0' : dataset_dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ'),
                'ts1' : stream_dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')}
            if deployment_dt1 and stream_dt1 > deployment_dt1:
                query_params['ts1'] = deployment_dt1.strftime('%Y-%m-%dT%H:%M:%S.%sZ')
                
            entry = {'stream' : None,
                'deployment' : None,
                'request_times' : None}
            # Create a copy of the stream
            entry['stream'] = stream.copy()
            # Add the new request times
            entry['request_params'] = query_params
            # Add the instrument deployment metadata
            entry['deployment'] = instrument
            updated_datasets.append(entry)
    
    return updated_datasets
    
def get_new_datasets(uframe, uframe_catalog_json, erddap_catalog_json, skip_csv=None):
    
    new_datasets = []
    
    if skip_csv:
        sys.stderr.write('Write code to parse skip_csv for streams to skip\n')
        return
        
    # Create the list of ERDDAP dataset Ids
    if erddap_catalog_json:
        erddap_dataset_ids = [d['datasetID'] for d in erddap_catalog_json]
    else:
        erddap_dataset_ids = []

    for instrument in uframe_catalog_json:
        
        # Get the list of all streams produced by this instrument
        streams = uframe.instrument_to_streams(instrument['instrument']['reference_designator'])
        if not streams:
            sys.stderr.write('{:s}: No streams found\n'.format(instrument['instrument']['reference_designator']))
            continue
            
        # Check each stream in streams to see if an ERDDAP dataset exists
        for stream in streams:
            
            stream_dataset_id = '{:s}-{:s}-{:s}'.format(stream['reference_designator'],
                stream['method'],
                stream['stream'])
                
            # If the stream_dataset_id does not exist, this is a new stream
            if stream_dataset_id not in erddap_dataset_ids:
                entry = {'stream' : None,
                    'deployment' : None,
                    'request_params' : None}
                # Create a copy of the stream
                entry['stream'] = stream.copy()
                # Create the query request times
                query_params = {'uframe_url' : uframe.base_url,
                    'ts0' : instrument['event_start_ts'],
                    'ts1' : instrument['event_stop_ts']}
                entry['request_params'] = query_params
                # Add the instrument deployment metadata
                entry['deployment'] = instrument
                new_datasets.append(entry)
                continue
    
    return new_datasets