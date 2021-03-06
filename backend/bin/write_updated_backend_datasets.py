#!/usr/bin/env python

import os
import sys
import json
import re
import argparse
from dateutil import parser
from datetime import timedelta
from UFrame import UFrame
from asynclib.util import csv2json
from asynclib.erddap import create_dataset_xml_filename, create_erddap_dataset_id
from asynclib.erddap import fetch_erddap_datasets
#from asynclib.backend import get_new_datasets

def main(args):
    '''Compare one or more deployment_json_files with the existing datasets on an
    ERDDAP server to determine if any new datasets need to be created.  Deployment
    objects are created for any instrument/stream that has new data to fetch from
    UFrame and written to disk'''
    
    # We must have OOI_ERDDAP_DATA_HOME defined
    async_data_home = os.getenv('OOI_ERDDAP_DATA_HOME')
    if not async_data_home:
        sys.stderr.write('Environment variable OOI_ERDDAP_DATA_HOME not defined\n')
        return 1;
    elif not os.path.isdir(async_data_home):
        sys.stderr.write('Invalid OOI_ERDDAP_DATA_HOME directory: {:s}\n'.format(async_data_home))
        return 1
    
    # We must have OOI_ERDDAP_BACKEND_BASE_URL defined
    erddap_base_url = args.erddap_base_url
    if not erddap_base_url:
        sys.stdout.write('No erddap_backend_base_url not specified...Checking environment\n')
        if not os.getenv('OOI_ERDDAP_BACKEND_BASE_URL'):
            sys.stderr.write('Environment variable OOI_ERDDAP_BACKEND_BASE_URL not defined\n')
            return 1
        erddap_base_url = os.getenv('OOI_ERDDAP_BACKEND_BASE_URL')
    # Make sure the url begins with http
    if not erddap_base_url.startswith('http'):
        sys.stderr.write('ERDDAP base url must start with \'http\'\n')
        return 1

    sys.stdout.write('ERDDAP base url: {:s}\n'.format(erddap_base_url))
        
    # If the user did not specify the location of the subsites csv cable type file,
    # try to find it using the OOI_ERDDAP_ASYNC_HOME environment variable
    subsite_csv = args.subsite_csv
    if not subsite_csv:
        async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        subsite_csv = os.path.join(async_home, 'config', 'subsites.csv')
    if not os.path.isfile(subsite_csv):
        sys.stderr.write('Invalid subsites cable type csv file specified: {:s}\n'.format(subsite_csv))
        return 1
        
    # Use the current working directory as the dest_dir if not specified via -d
    dest_dir = args.destination
    if not dest_dir:
        async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        if not async_home:
            sys.stderr.write('Environment variable OOI_ERDDAP_ASYNC_HOME not defined\n')
            return 1
        dest_dir = os.path.join(async_home, 'backend', 'datasets-updates')
    # Make sure dest_dir exists
    if not os.path.isdir(dest_dir):
        sys.stderr.write('Catalog destination does not exist: {:s}\n'.format(dest_dir))
        return 1
    
    # Convert the subsite_csv to a list of sites and cable types
    subsites = csv2json(subsite_csv)
    if not subsites:
        return 1
    
    try:
        subsite_names = [s['subsite'] for s in subsites]
    except KeyError as e:
        sys.stderr.write('Failed to get subsite names from: {:s} ({:s})\n'.format(subsite_csv, e))
        return 1
        
    # Create the UFrame instance
    uframe = UFrame(base_url=args.base_url, timeout=args.timeout)
    
    # Create a regex for pulling out the datatype from the ERDDAP xml file provided
    # it exists
    erddap_type_regex = re.compile('type="EDD(Table|Grid)From')
        
    for deployment_json_file in args.deployment_json_files:
        
        catalog_file = os.path.basename(deployment_json_file)   
        (fname, ext) = os.path.splitext(catalog_file)
        # Create the updated datasets catalog filename and see if it exists
        new_datasets_catalog = os.path.join(dest_dir, '{:s}.new.json'.format(fname))
        if os.path.isfile(new_datasets_catalog):
            if args.clobber:
                sys.stdout.write('Clobbering existing updated deployments catalog: {:s}\n'.format(new_datasets_catalog))
            else:
                sys.stderr.write('Skipping existing updated deployments catalog: {:s} (Use --clobber to overwrite)\n'.format(new_datasets_catalog))
                continue
        
        # Read in the specified deployment_json_file for deployed instruments
        try:
            with open(deployment_json_file, 'r') as fid:
                deployments = json.load(fid)
        except (IOError, ValueError) as e:
            sys.stderr.write('{:s}\n'.format(e))
            return 1
                
        if type(deployments) != list:
            sys.stderr.write('{:s}: Deployment json file does not contain a top level list\n'.format(deployment_json_file))
            return 1
        elif len(deployments) == 0:
            sys.stderr.write('{:s}: Deployment json file contains no deployments\n'.format(deployment_json_file))
            return 1

        new_datasets = []
        for deployment in deployments:
            
            # Make sure the instrument subsite name exists in subsites
            if deployment['instrument']['subsite'] not in subsite_names:
                sys.stderr.write('Unknown subsite: {:s}\n'.format(deployment['instrument']['subsite']))
                continue
            
            # Pull the subsite element out to create the ERDDAP stream-xml directory    
            subsite = subsites[subsite_names.index(deployment['instrument']['subsite'])]
            erddap_instance = None
            if 'cabled_type' not in subsite:
                sys.stderr.write('{:s}: Missing subsite cabled_type key ({:s})\n'.format(subsite['subsite'], subsite_csv))
                continue
            elif subsite['cabled_type'] == 'cabled':
                erddap_instance = 'erddap-12-1'
            elif subsite['cabled_type'] == 'uncabled':
                erddap_instance = 'erddap-12-2'
            if not erddap_instance:
                sys.stderr.write('{:s}: Unknown subsite cable type\n'.format(subsite['subsite']))
                continue
            # Create the stream-xml location and validate it's existence
            stream_xml_dir = os.path.join(async_data_home, erddap_instance, 'stream-xml')
            if not os.path.isdir(stream_xml_dir):
                sys.stderr.write('Invalid ERDDAP stream-xml directory: {:s}\n'.format(stream_xml_dir))
                continue
            
            # Get the list of all streams produced by this instrument
            streams = uframe.instrument_to_streams(deployment['instrument']['reference_designator'])
            if not streams:
                sys.stderr.write('{:s}: No streams found\n'.format(deployment['instrument']['reference_designator']))
                continue
                
            # Check each stream in streams to see if an ERDDAP dataset exists
            for stream in streams:
                
                # instrument, telemetry, stream, deployment_number
                dataset_xml_filename = create_dataset_xml_filename(deployment['instrument']['reference_designator'],
                    stream['method'],
                    stream['stream'],
                    deployment['deployment_number'])
                    
                # Fully-qualified dataset xml file location
                dataset_xml_file = os.path.join(stream_xml_dir, dataset_xml_filename)
                    
                # If the stream_dataset_id does not exist, this is a new stream and 
                # we're not interested in it
                if not os.path.isfile(dataset_xml_file):
                    continue
                    
                # If the stream dataset_xml_file does exist, parse it and grab the type
                # attribute from the top level <dataset></dataset> element
                # Read in the entire file
                try:
                    with open(dataset_xml_file, 'r') as xml_fid:
                        string_xml = xml_fid.read()
                except IOError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    continue
                    
                match = erddap_type_regex.search(string_xml)
                if not match:
                    sys.stderr.write('Unable to determine ERDDAP dataset type: {:s}\n'.format(dataset_xml_file))
                    continue
                    
                dataset_type = match.groups()[0]
                erddap_dataset_type = None
                if dataset_type == 'Table':
                    erddap_dataset_type = 'tabledap'
                elif dataset_type == 'Grid':
                    erddap_dataset_type = 'griddap'
                else:
                    sys.stderr.write('Unknown ERDDAP dataset type: {:s}\n'.format(dataset_type))
                    continue
                
                # Create the datasetID so that we can query the ERDDAP server for it's metadata
                dataset_id = create_erddap_dataset_id(deployment['instrument']['reference_designator'],
                    stream['stream'],
                    stream['method'],
                    deployment['deployment_number'])
                # Create the all datasets URL that we'll need to hit to search for the 
                # dataset's metadata    
                erddap_daptype_url = '{:s}/{:s}/erddap/{:s}'.format(erddap_base_url, subsite['cabled_type'], erddap_dataset_type)
                # Send the request for the specified datasetID
                datasets = fetch_erddap_datasets(erddap_daptype_url, dataset_id=dataset_id)
                if not datasets:
                    continue    
                    
                dataset = datasets[0]
                    
                sys.stdout.write('Dataset found: {:s}\n'.format(dataset_id))
                sys.stdout.write('Checking for UPDATES...\n')
                
                # Start with the deployment event_stop_ts
                event_stop_ts = deployment['event_stop_ts']
                # If event_stop_ts is None, the deployment is active.  Use the stream
                # endTime as the event_stop_time
                if not event_stop_ts:
                    event_stop_ts = stream['endTime']
                # Convert event_stop_ts to a datetime
                event_stop_dt = parser.parse(event_stop_ts)
                
                # Convert the dataset['maxTime'] to a datetime and add the event_stop_dt.microsecond
                # value to account for the fact that UFrame keeps track of microseconds but
                # ERDDAP does not.  This will allow comparison of event_stop_ts and dataset_stop_dt
                # while ignoring the microseconds
                dataset_stop_dt = parser.parse(dataset['maxTime']) + timedelta(0, 0, event_stop_dt.microsecond)
                if event_stop_dt < dataset_stop_dt:
                    continue
                else:
                    sys.stdout.write('{:s}: New data available from UFrame.{:s}\n'.format(dataset_id, uframe.base_url))
    
                    entry = {'stream' : None,
                        'deployment' : None,
                        'request_params' : None}
                    # Create a copy of the stream
                    entry['stream'] = stream.copy()
                    # Create the query request times
                    query_params = {'uframe_url' : uframe.base_url,
                        'ts0' : dataset_stop_dt.strftime('%Y-%m-%dT%H:%M:%S.%sZ'),
                        'ts1' : event_stop_dt.strftime('%Y-%m-%dT%H:%M:%S.%sZ')}
                    entry['request_params'] = query_params
                    # Add the instrument deployment metadata
                    entry['deployment'] = deployment
                    new_datasets.append(entry)
            
            
        if not new_datasets:
            sys.stdout.write('{:s}: No updates needed\n'.format(deployment_json_file))
            continue
        
        # Write the resulting datasets that need creating to file    
        sys.stdout.write('Writing new datasets file: {:s}\n'.format(new_datasets_catalog))
        try:
            with open(new_datasets_catalog, 'w') as fid:
                json.dump(new_datasets, fid)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            return 1
    
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('deployment_json_files',
        nargs='+',
        help='Instrument deployment JSON catalog file')
    arg_parser.add_argument('-d', '--destination',
        dest='destination',
        help='Specify the location for the catalog file, which must exist')
    arg_parser.add_argument('-s', '--subsite_csv',
        dest='subsite_csv',
        help='Location of the subsites csv cable type file')
    arg_parser.add_argument('-c', '--clobber',
        dest='clobber',
        action='store_true',
        help='Clobber the updated dataset catalog file if it exists.')
    arg_parser.add_argument('-e', '--erddap_backend_base_url',
        dest='erddap_base_url',
        help='Alternate ERDDAP address.  Must start with \'http://\'.  Taken from OOI_ERDDAP_BACKEND_BASE_URL if not specified, provided it is set.')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')

    parsed_args = arg_parser.parse_args()
    #print parsed_args
    #sys.exit(3)
    
    sys.exit(main(parsed_args))
