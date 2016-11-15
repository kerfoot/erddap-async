#!/usr/bin/env python

import os
import sys
import json
import glob
from shutil import copyfile
import argparse

def main(args):
    '''For each uframe response json file specified, the request status is checked and,
    if complete, the ERDDAP .args and erddapDatasets.xml files are created in the
    default templating destination location.  These files are used to create an XML
    template used to serve the dataset via ERDDAP'''
    
    # Set up & validate environment & directories
        
    # Must have OOI_ERDDAP_ASYNC_HOME set and valid
    async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
    if not async_home:
        sys.stderr.write('OOI_ERDDAP_ASYNC_HOME not defined\n')
        return 1
    if not os.path.isdir(async_home):
        sys.stderr.write('Invalid OOI_ERDDAP_ASYNC_HOME directory: {:s}\n'.format(async_home))
        return 1
    
    # Must have OOI_ERDDAP_UFRAME_NC_ROOT set and valid
    async_root = os.getenv('OOI_ERDDAP_UFRAME_NC_ROOT')
    if not async_root:
        sys.stderr.write('OOI_ERDDAP_UFRAME_NC_ROOT not defined\n')
        return 1
    if not os.path.isdir(async_root):
        sys.stderr.write('Invalid OOI_ERDDAP_UFRAME_NC_ROOT directory: {:s}\n'.format(async_root))
        return 1
    
    # Use the default template datasets destination if none was specified via --destination    
    tmpl_file_destination = args.destination
    if not tmpl_file_destination:
        tmpl_file_destination = os.path.join(async_home,
            'backend',
            'templating',
            'datasets')
        
    if not os.path.isdir(tmpl_file_destination):
        sys.stderr.write('Invalid template files destination: {:s}\n'.format(tmpl_file_destination))
        return 1
        
    # Location of the ERDDAP template master files    
    tmpl_masters_dir = os.path.join(async_home,
        'backend',
        'templating',
        'masters')
    if not os.path.isdir(tmpl_masters_dir):
        sys.stderr.write('Invalid template masters directory: {:s}\n'.format(tmpl_masters_dir))
        return 1
        
    # Make sure the ERDDAP args and xml datasets template files exist
    args_master = os.path.join(tmpl_masters_dir, 'generateDatasetsXmlTemplate.args')
    xml_master = os.path.join(tmpl_masters_dir, 'datasets-xml-template.xml')
    if not os.path.isfile(args_master):
        sys.stderr.write('Invalid args master file: {:s}\n'.format(args_master))
        return 1
    if not os.path.isfile(xml_master):
        sys.stderr.write('Invalid datasets xml master file: {:s}\n'.format(xml_master))
        return 1
        
    # Read in the args_template
    try:
        with open(args_master, 'r') as fid:
            args_tmpl = fid.read()
    except IOError as e:
        sys.stderr.write('{:s}\n'.format(e))
        return 1
    
    template_runs = []
    for response_json_file in args.response_json_files:
        
        try:
            with open(response_json_file, 'r') as fid:
                response = json.load(fid)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
            
        # Make sure the response dict exists
        if 'response' not in response.keys():
            sys.stderr.write('Invalid UFrame response object: {:s}\n'.format(response_json_file))
            continue
        # response code must == 200 to be ok    
        if response['status_code'] != 200:
            sys.stderr.write('Invalid UFrame request (Reason: {:s}\n'.format(response['reason']))
            continue
        
        ## See if a template exists for this request
        #dataset_template = get_valid_dataset_template(response['stream']['stream'],
        #    response['stream']['method'],
        #    template_dir=template_dir)
        #if not dataset_template:
        #    continue
            
        # Make sure the response has the proper number of allURLs items
        if len(response['response']['allURLs']) != 2:
            sys.stderr.write('allURLs does not contain 2 urls\n'.format(response_json_file))
            continue
            
        # Get the user product directory from response['response']['response']['allURLs'][1]
        d_tokens = response['response']['allURLs'][1].split('/')
        if len(d_tokens) != 6:
            sys.stderr.write('Badly formatted UFrame async destination url: {:s}\n'.format(response['response']['response']['allURLs'][1]))
            continue
        async_nc_dir = os.path.join(async_root, d_tokens[4], d_tokens[5])
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
        nc_filename_template = 'deployment*{:s}-{:s}-{:s}*.nc'.format(
            response['reference_designator'],
            response['instrument']['telemetry'],
            response['instrument']['stream'])
        source_nc_files = glob.glob(os.path.join(async_nc_dir, nc_filename_template))
        # Skip the rest if no NetCDF files were found in the UFrame product directory
        if not source_nc_files:
            sys.stderr.write('No source NetCDF files found: {:s}\n'.format(async_nc_dir))
            continue
            
        # Create the args and xml templates if we have at least one file to use
        tmpl_dir = '{:s}-{:s}'.format(response['instrument']['stream'],
            response['instrument']['telemetry'])
        tmpl_dest_dir = os.path.join(tmpl_file_destination, tmpl_dir)
        if not os.path.isdir(tmpl_dest_dir):
            try:
                os.mkdir(tmpl_dest_dir)
            except OSError as e:
                sys.stderr.write('{:s}\n'.format(e))
                continue
        
        # Create the args filename
        args_fname = '{:s}.erddapDatasets.args'.format(tmpl_dir)
        args_file = os.path.join(tmpl_dest_dir, args_fname)
        if os.path.isfile(args_file):
            sys.stderr.write('Args file exists: {:s}\n'.format(args_file))
            if not args.clobber:
                sys.stderr.write('Delete the file manually or specify --clobber option: {:s}\n'.format(args_file))
            else:
                try:
                    os.remove(args_file)
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    continue
            
        # Create the datasets xml filename
        xml_fname = '{:s}.erddapDatasets.xml'.format(tmpl_dir)
        xml_file = os.path.join(tmpl_dest_dir, xml_fname)
        if os.path.isfile(xml_file):
            sys.stderr.write('XML template file exists: {:s}\n'.format(xml_file))
            if not args.clobber:
                sys.stderr.write('Delete the file manually or specify --clobber option: {:s}\n'.format(xml_file))
            else:
                try:
                    os.remove(xml_file)
                except OSError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    continue
        
        (nc_path, nc_file) = os.path.split(source_nc_files[0])
        # Write the args to args_file
        args_params = {'source_dir' : nc_path,
            'netcdf_filename' : source_nc_files[0]}
        args_data = args_tmpl.format(**args_params)
    
        # Create a copy of the args template file using nc_file and write the args
        # to it
        try:
            with open(args_file, 'w') as fid:
                fid.write(args_data)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
            
        # Make a copy the datasets_xml_template for the nc_file
        try:
            copyfile(xml_master, xml_file)
        except IOError as e:
            sys.stderr.write('Error creating stream datasets.xml file: {:s}\n'.format(e))
            continue
            
        template_runs.append(tmpl_dest_dir)
    
    # Print the names of the directories in which the args and xml files were successfully
    # created    
    for template_run in template_runs:
        sys.stdout.write('{:s}\n'.format(template_run))
        
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('response_json_files',
        nargs='+',
        help='One or more JSON files containing a UFrame asynchronous request response')
    arg_parser.add_argument('-d', '--destination',
        help='Alternate template preparation destination directory')
    arg_parser.add_argument('-c', '--clobber',
        action='store_true',
        help='Overwrite any existing args and xml files present in the dataset directory')
        
    parsed_args = arg_parser.parse_args()
    
    #print vars(parsed_args)
    #sys.exit(13)

    sys.exit(main(parsed_args))

