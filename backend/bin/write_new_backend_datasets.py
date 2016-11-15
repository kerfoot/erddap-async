#!/usr/bin/env python

import os
import sys
import json
import argparse
from UFrame import UFrame
from asynclib.util import csv2json
from asynclib.erddap import create_dataset_xml_filename#, create_erddap_dataset_id

def main(args):
    '''Compare one or more subsite instrument deployment catalogs with the existing 
    datasets on an ERDDAP server to determine if any new datasets need to be created.
    New instrument deployment JSON objects are written to the default datasets-new
    directory.
    '''
    
    # We must have OOI_ERDDAP_DATA_HOME defined
    async_data_home = os.getenv('OOI_ERDDAP_DATA_HOME')
    if not async_data_home:
        sys.stderr.write('Environment variable OOI_ERDDAP_DATA_HOME not defined\n')
        return 1;
    elif not os.path.isdir(async_data_home):
        sys.stderr.write('Invalid OOI_ERDDAP_DATA_HOME directory: {:s}\n'.format(async_data_home))
        return 1
    
    # If the user did not specify the location of the subsites csv cable type file,
    # try to find it using the OOI_ERDDAP_ASYNC_HOME environment variable
    subsite_csv = args.subsite_csv
    if not subsite_csv:
        async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        subsite_csv = os.path.join(async_home,
            'config',
            'subsites.csv')
    if not os.path.isfile(subsite_csv):
        sys.stderr.write('Invalid subsites cable type csv file specified: {:s}\n'.format(subsite_csv))
        return 1

    # Create the default destination for the new datasets files if not specified with -d
    dest_dir = args.destination
    if not dest_dir:
        async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        if not async_home:
            sys.stderr.write('Environment variable OOI_ERDDAP_ASYNC_HOME not defined\n')
            return 1
        dest_dir = os.path.join(async_home,
            'backend',
            'datasets-new')
    # Make sure dest_dir exists
    if not os.path.isdir(dest_dir):
        sys.stderr.write('Catalog destination does not exist: {:s}\n'.format(dest_dir))
        return 1

    # Convert the subsite_csv to a list of sites and cable types
    subsites = csv2json(subsite_csv)
    if not subsites:
        return 1
        
    for subsite_deployments_json_catalog in args.subsite_deployments_json_catalogs:
        
        # Read in the specified subsite_deployments_json_catalog for deployed instruments
        try:
            with open(subsite_deployments_json_catalog, 'r') as fid:
                deployments = json.load(fid)
        except (IOError, ValueError) as e:
            sys.stderr.write('{:s}\n'.format(e))
            return 1
                
        if type(deployments) != list:
            sys.stderr.write('{:s}: Deployment json file does not contain a top level list\n'.format(subsite_deployments_json_catalog))
            return 1
        elif not deployments:
            sys.stderr.write('{:s}: Deployment json file contains no deployments\n'.format(subsite_deployments_json_catalog))
            return 1
            
        catalog_file = os.path.basename(subsite_deployments_json_catalog)   
        (fname, ext) = os.path.splitext(catalog_file)
        # Write the resulting datasets that need creating to file
        new_datasets_catalog = os.path.join(dest_dir, '{:s}.new.json'.format(fname))
        if os.path.isfile(new_datasets_catalog):
            if not args.clobber:
                sys.stderr.write('Skipping existing catalog: {:s} (Use --clobber to overwrite or delete manually)\n'.format(new_datasets_catalog))
                continue
        
        try:
            subsite_names = [s['subsite'] for s in subsites]
        except KeyError as e:
            sys.stderr.write('Failed to get subsite names from: {:s} ({:s})\n'.format(subsite_csv, e))
            return 1
            
        # Create the UFrame instance
        uframe = UFrame(base_url=args.base_url, timeout=args.timeout)
        
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
                sys.stdout.write('Creating ERDDAP stream-xml directory: {:s}\n'.format(stream_xml_dir))
                try:
                    os.makedirs(stream_xml_dir)
                    sys.stdout.write('ERDDAP stream-xml directory created\n')
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e))
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
                    
                # If the stream_dataset_id does not exist, this is a new stream
                if not os.path.isfile(dataset_xml_file):
                    #dataset_id = create_erddap_dataset_id(deployment['instrument']['reference_designator'],
                    #    stream['method'],
                    #    stream['stream'],
                    #    deployment['deployment_number'])
                    #sys.stdout.write('New dataset found: {:s}\n'.format(dataset_id))
                    entry = {'stream' : None,
                        'deployment' : None,
                        'request_params' : None}
                    # Create a copy of the stream
                    entry['stream'] = stream.copy()
                    # Create the query request times
                    query_params = {'uframe_url' : uframe.base_url,
                        'ts0' : deployment['event_start_ts'],
                        'ts1' : deployment['event_stop_ts']}
                    entry['request_params'] = query_params
                    # Add the instrument deployment metadata
                    entry['deployment'] = deployment
                    new_datasets.append(entry)
            
        if not new_datasets:
            return 0
                
        try:
            with open(new_datasets_catalog, 'w') as fid:
                json.dump(new_datasets, fid)
                sys.stdout.write('{:s}\n'.format(new_datasets_catalog))
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            return 1
    
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('subsite_deployments_json_catalogs',
        nargs='+',
        help='Subsite instrument deployment JSON catalog file')
    arg_parser.add_argument('-d', '--destination',
        dest='destination',
        help='Specify the location for the catalog file(s), which must exist')
    arg_parser.add_argument('-s', '--subsite_csv',
        dest='subsite_csv',
        help='Location of the subsites csv cable type file')
    arg_parser.add_argument('-c', '--clobber',
        dest='clobber',
        action='store_true',
        help='Clobber if the catalog file already exists')
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
