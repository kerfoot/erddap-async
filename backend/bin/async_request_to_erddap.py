#!/usr/bin/env python

import os
import sys
import json
import glob
import argparse
import shutil
import re
import datetime
from dateutil import parser
from asynclib.util import csv2json
from asynclib.templating import get_valid_dataset_template
from asynclib.erddap import create_dataset_xml_filename, create_erddap_dataset_id, create_dataset_xml
from asynclib.filesystem import build_nc_dest

def main(args):
    '''Creates or updates backend cabled and uncabled ERDDAP datasets for each 
    response_file in response_files'''
    
    # Check the environment
    required_environment_dirs = ['OOI_ERDDAP_UFRAME_NC_ROOT',
        'OOI_ERDDAP_DATA_HOME',
        'OOI_ERDDAP_ASYNC_HOME']
    env_ok = True
    for d in required_environment_dirs:
        if not os.getenv(d):
            sys.stderr.write('Unset environment variable: {:s}\n'.format(d))
            env_ok = False
        elif not os.path.isdir(os.getenv(d)):
            sys.stderr.write('{:s}: Directory does not exist\n'.format(os.getenv(d)))
            env_ok = False
            
    if not env_ok:
        return 1

    # Use OOI_ERDDAP_ASYNC_HOME to create the erddap backend dataset templates location
    template_dir = os.path.join(os.getenv('OOI_ERDDAP_ASYNC_HOME'),
        'backend',
        'templating',
        'xml',
        'production')
    if not os.path.isdir(template_dir):
        sys.stderr.write('Invalid ERDDAP XML template directory: {:s}\n'.format(template_dir))
        return 1
        
    # Make sure the subsites csv file exists
    subsite_csv = os.path.join(os.getenv('OOI_ERDDAP_ASYNC_HOME'),
        'config',
        'subsites.csv')
    if not os.path.isfile(subsite_csv):
        sys.stderr.write('Invalid subsites cable type csv file specified: {:s}\n'.format(subsite_csv))
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
    
    # Make sure the instrument metadata json file exists
    instruments_metadata_file = os.path.join(os.getenv('OOI_ERDDAP_ASYNC_HOME'),
        'config',
        'visualocean-instruments-metadata.json')
    if not os.path.isfile(instruments_metadata_file):
        sys.stderr.write('Invalid instrument metadata JSON file specified: {:s}\n'.format(instruments_metadata_file))
        return 1
    # Load the file
    try:
        with open(instruments_metadata_file, 'r') as fid:
            instrument_descriptions = json.load(fid)
    except (IOError, ValueError) as e:
        sys.stderr.write('{:s}: {:s}\n'.format(e, instruments_metadata_file))
        return 1
        
    # Create the list of known reference designators from the instruments metadata
    instruments = [i['reference_designator'] for i in instrument_descriptions]
        
    # Create the regex for pulling out the start and end times for a NetCDF filenae
    nc_ts_regex = re.compile('(\d{8}T\d{6}\.\d{1,})\-(\d{8}T\d{6}\.\d{1,})\.nc')

    # Each response file
    for response_file in args.response_files:
        
        if args.debug:
            sys.stdout.write('==> DEBUG MODE: No file operations performed! <==\n')
        
        sys.stdout.write('Processing response file: {:s}\n'.format(response_file))
        
        try:
            with open(response_file, 'r') as fid:
                response = json.load(fid)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
            
        # Make sure the response dict exists
        if 'response' not in response.keys():
            sys.stderr.write('Invalid UFrame response object: {:s}\n'.format(response_file))
            continue
        # response code must == 200 to be ok    
        if response['response']['status_code'] != 200:
            sys.stderr.write('Invalid UFrame request (Reason: {:s}\n'.format(response['reason']))
            continue
        
        # See if a template exists for this request
        dataset_template = get_valid_dataset_template(response['stream']['stream'],
            response['stream']['method'],
            template_dir=template_dir)
        if not dataset_template:
            continue
            
        # Make sure the response has the proper number of allURLs items
        if len(response['response']['response']['allURLs']) < 2:
            sys.stderr.write('allURLs contains < 2 urls\n'.format(response_file))
            continue
            
        # Get the user product directory from response['response']['response']['allURLs'][1]
        d_tokens = response['response']['response']['allURLs'][1].split('/')
        if len(d_tokens) != 6:
            sys.stderr.write('Badly formatted UFrame async destination url: {:s}\n'.format(response['response']['response']['allURLs'][1]))
            continue
        async_nc_dir = os.path.join(os.getenv('OOI_ERDDAP_UFRAME_NC_ROOT'), d_tokens[4], d_tokens[5])
        if not os.path.isdir(async_nc_dir):
            sys.stdout.write('UFrame async destination does not exist: {:s}\n'.format(async_nc_dir))
            continue
            
        # Check for the existence and contents of the status.txt file in async_nc_dir
        # If it's there and it contains the work 'complete', the request is ready for processing
        status_file = os.path.join(async_nc_dir, 'status.txt')
        if not os.path.isfile(status_file):
            sys.stdout.write('Production creation incomplete: {:s}\n'.format(async_nc_dir))
            continue
        try:
            with open(status_file, 'r') as fid:
                status = fid.readline().strip()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue

        if status != 'complete':
            sys.stdout.write('Production creation incomplete: {:s}\n'.format(status_file))
            continue
            
        # Create the NetCDF filename glob string to search for created files
        nc_filename_template = 'deployment{:04.0f}_{:s}-{:s}-{:s}*.nc'.format(
            response['deployment']['deployment_number'],
            response['deployment']['instrument']['reference_designator'],
            response['stream']['method'],
            response['stream']['stream'])
        source_nc_files = glob.glob(os.path.join(async_nc_dir, nc_filename_template))
        # Skip the rest if no NetCDF files were found in the UFrame product directory
        if not source_nc_files:
            sys.stderr.write('No source NetCDF files found: {:s}\n'.format(async_nc_dir))
            continue
            
        # Create the ERDDAP dataset xml filename
        xml_filename = create_dataset_xml_filename(response['deployment']['instrument']['reference_designator'],
            response['stream']['method'],
            response['stream']['stream'],
            response['deployment']['deployment_number'])
        
        # Make sure the instrument subsite name exists in subsites
        if response['response']['instrument']['subsite'] not in subsite_names:
            sys.stderr.write('Unknown subsite: {:s}\n'.format(response['response']['instrument']['subsite']))
            continue
            
        # Pull the subsite element out to create the ERDDAP stream-xml directory    
        subsite = subsites[subsite_names.index(response['response']['instrument']['subsite'])]
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
        datasets_xml_dir = os.path.join(os.getenv('OOI_ERDDAP_DATA_HOME'), erddap_instance, 'stream-xml')
        if not os.path.isdir(datasets_xml_dir):
            sys.stderr.write('Invalid ERDDAP stream-xml directory: {:s}\n'.format(datasets_xml_dir))
            continue
        
        # Create the ERDDAP product directory
        nc_dest_product_dir = build_nc_dest(response['deployment']['instrument']['reference_designator'],
            response['stream']['method'],
            response['stream']['stream'],
            response['deployment']['deployment_number'])
        dest_nc_dir = os.path.join(os.getenv('OOI_ERDDAP_DATA_HOME'),
            erddap_instance,
            'nc',
            nc_dest_product_dir)
        # Create the ERDDAP product directory if it does not exist
        if not os.path.isdir(dest_nc_dir):
            sys.stdout.write('ERDDAP destination does not exist: {:s}\n'.format(dest_nc_dir))
            if not args.debug:
                sys.stdout.write('Creating ERDDAP destination directory: {:s}\n'.format(dest_nc_dir))
                try:
                    os.makedirs(dest_nc_dir)
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    continue
        
        # Create the ERDDAP dataset id
        dataset_id = create_erddap_dataset_id(response['deployment']['instrument']['reference_designator'],
            response['stream']['stream'],
            response['stream']['method'],
            response['deployment']['deployment_number'])
            
        # Fully-qualified path to the dataset XML file, provided it exists    
        dataset_xml_file = os.path.join(datasets_xml_dir, xml_filename)
        # If not debugging and -f option, remove the dataset_xml_file if it exists
        if not args.debug and args.force and os.path.isfile(dataset_xml_file):
            sys.stdout.write('Clobbering existing dataset XML file: {:s}\n'.format(dataset_xml_file))
            os.unlink(dataset_xml_file)
            
        # Does the dataset_xml_file exist? If it does exist, we need to UPDATE the 
        # dataset.  If it doesn't exist, we need to create it to add it to the ERDDAP
        # instance
        if os.path.isfile(dataset_xml_file):
            sys.stdout.write('UPDATING existing dataset\n')
            sys.stdout.write('ERDDAP dataset ID: {:s}\n'.format(dataset_id))
            sys.stdout.write('ERDDAP XML file  : {:s}\n'.format(dataset_xml_file)) 
            sys.stdout.write('UFrame directory : {:s}\n'.format(async_nc_dir))
            sys.stdout.write('ERDDAP directory : {:s}\n'.format(dest_nc_dir))
            
            if args.debug:
                for nc in source_nc_files:
                    sys.stdout.write('\tUFrame NetCDF file: {:s}\n'.format(nc))
                continue
                
            # Get this list of NetCDF files that are already in dest_nc_dir
            last_nc_dt = None
            destination_nc_files = glob.glob(os.path.join(dest_nc_dir, nc_filename_template))
            if destination_nc_files:
                # Take the last file, extract the end time and convert it to a datetime
                last_nc = destination_nc_files[-1]
                (ncp, ncf) = os.path.split(last_nc)
                # See if we can pull out the NetCDF start and end times
                match = nc_ts_regex.search(ncf)
                if match:
                    try:
                        last_nc_dt = parser.parse('{:s}Z'.format(match.groups()[-1]))
                    except ValueError as e:
                        sys.stderr.write('{:s}\n'.format(e))
                        last_nc_dt = None
                        continue
            
            copy_count = 0
            nc_status = True
            for source_nc in source_nc_files:
                
                (ncp, ncf) = os.path.split(source_nc)
                
                # Skip the copy if the file already exists
                dest_nc = os.path.join(dest_nc_dir, ncf)
                # Clobber existing NetCDF files if the user specified -f (force).  Otherwise
                # only copy files that don't already exist at the destination
                if not args.force and os.path.isfile(dest_nc):
                    sys.stderr.write('Skipping (Destination NetCDF exists): {:s}\n'.format(dest_nc))
                    continue
                    
                # If we were able to pull out a last_nc_dt to compare file start times,
                # see if we can pull out the file start time to compare with it.  Only
                # source_nc NetCDF files with start times > last_nc_dt will be copied
                # See if we can pull out the NetCDF start and end times
                match = nc_ts_regex.search(ncf)
                nc_start_dt = None
                if match:
                    try:
                        nc_start_dt = parser.parse('{:s}Z'.format(match.groups()[0]))
                    except ValueError as e:
                        sys.stderr.write('{:s}\n'.format(e))
                        nc_status = False
                        
                if nc_start_dt < last_nc_dt:
                    sys.stdout.write('Skipping earlier source file: {:s}\n'.format(source_nc))
                    continue
                    
                try:
                    sys.stdout.write('Copying file: {:s}\n'.format(source_nc))
                    shutil.copy(source_nc, dest_nc_dir)
                    copy_count = copy_count + 1
                except IOError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    nc_status = False
                    continue
                    
            # Print the number of NetCDF files copied
            sys.stdout.write('Updated dataset with {:0.0f} NetCDF files\n'.format(copy_count))
            
            if not nc_status:
                sys.stderr.write('1 or more NetCDF copy issues. Keeping response file: {:s}\n'.format(response_file))
                continue
                
            # Delete the response file ONLY if the user said to and everything looks good with the NetCDF copies
            if args.delete_on_success:
                sys.stdout.write('SUCCESS: Deleting response file: {:s}\n'.format(response_file))
                os.unlink(response_file)
                
        else:
            sys.stdout.write('CREATING new dataset\n')
            sys.stdout.write('ERDDAP dataset ID: {:s}\n'.format(dataset_id))
            sys.stdout.write('ERDDAP XML file  : {:s}\n'.format(dataset_xml_file)) 
            sys.stdout.write('UFrame directory : {:s}\n'.format(async_nc_dir))
            sys.stdout.write('ERDDAP directory : {:s}\n'.format(dest_nc_dir))
            
            if args.debug:
                for nc in source_nc_files:
                    sys.stdout.write('\tUFrame NetCDF file: {:s}\n'.format(nc))
                continue
            
            # Make sure the reference designator referers to an instrument in instrument_descriptions
            if response['deployment']['instrument']['reference_designator'] not in instruments:
                sys.stderr.write('{:s}: No instrument metadata entry found\n'.format(response['deployment']['instrument']['reference_designator']))
                return
                
            instrument_meta = instrument_descriptions[instruments.index(response['deployment']['instrument']['reference_designator'])]
    
            # Create the dataset title
            dataset_title = '{:s} {:s} {:s} {:s} {:s} - Deployment {:04.0f} ({:s})'.format(instrument_meta['site'],
                instrument_meta['subsite'],
                instrument_meta['node'],
                instrument_meta['name'],
                response['stream']['stream'],
                response['deployment']['deployment_number'],
                response['stream']['method'])
        
            # Create the dataset summary text
            summary = instrument_meta['description']
            if not summary:
                summary = ''
                
            # Write the dataset xml file
            sys.stdout.write('Creating dataset xml: {:s}\n'.format(dataset_id))
            dataset_xml = create_dataset_xml(dest_nc_dir,
                dataset_template,
                dataset_id,
                dataset_title,
                summary)

            if not dataset_xml:
                sys.stderr.write('Failed to write dataset xml for dataset ID: {:s}\n'.format(dataset_id))
                continue
            
            # Write the xml to the dataset_xml_file
            try:    
                with open(dataset_xml_file, 'w') as fid:
                    fid.write('{:s}\n'.format(dataset_xml))
            except IOError as e:
                sys.stderr.write('{:s}\n'.format(e))
                continue
                
            # If the xml file was successfully written, move any NetCDF files that 
            # don't already exist in dest_nc_dir
            copy_count = 0
            nc_status = True
            for source_nc in source_nc_files:
                
                (ncp, ncf) = os.path.split(source_nc)
                dest_nc = os.path.join(dest_nc_dir, ncf)
                # Clobber existing NetCDF files if the user specified -f (force).  Otherwise
                # only copy files that don't already exist at the destination
                if not args.force and os.path.isfile(dest_nc):
                    sys.stderr.write('Skipping (Destination NetCDF exists): {:s}\n'.format(dest_nc))
                    continue
                    
                try:
                    sys.stdout.write('Copying file: {:s}\n'.format(source_nc))
                    shutil.copy(source_nc, dest_nc_dir)
                    copy_count = copy_count + 1
                except IOError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    nc_status = False
                    continue
                    
            # Print the number of NetCDF files copied
            sys.stdout.write('Created dataset with {:0.0f} NetCDF files\n'.format(copy_count))
            
            if not nc_status:
                sys.stderr.write('1 or more NetCDF copy issues. Keeping response file: {:s}\n'.format(response_file))
                continue
                
            # Delete the response file ONLY if the user said to and everything looks good with the NetCDF copies
            if args.delete_on_success:
                sys.stdout.write('SUCCESS: Deleting response file: {:s}\n'.format(response_file))
                os.unlink(response_file)
        
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('response_files',
        nargs='+',
        help='One or more UFrame request response files')
    arg_parser.add_argument('-x',
        dest='debug',
        action='store_true',
        help='Print the status of each request, but do not create or update any ERDDAP datasets')
    arg_parser.add_argument('-d', '--delete_on_success',
        dest='delete_on_success',
        action='store_true',
        help='Delete the response file ONLY if there were no issues with NetCDF copies')
    arg_parser.add_argument('-c', '--clobber',
        dest='force',
        action='store_true',
        help='Clobber existing stream-xml and NetCDF file(s).  Always results in a new dataset being created.')

    parsed_args = arg_parser.parse_args()
    
    sys.exit(main(parsed_args))
