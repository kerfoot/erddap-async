#!/usr/bin/env python

import os
import sys
import json
import glob
from shutil import copyfile
import argparse
from dateutil import parser
from datetime import timedelta
from asynclib.erddap import fetch_erddap_datasets, download_erddap_nc

def main(args):
    '''Download a small NetCDF file, create the .args and .xml files necessary
    for creating an ERDDAP XML template to be used for serving frontent stream
    datasets.  Files are created for each dataset found on the ERDDAP server
    located at the URL specified in erddap_url, which must end in either
    tabledap or griddap.
    '''

    # Fetch the erddap datasets listing
    erddap_datasets = fetch_erddap_datasets(args.erddap_url)
    if not erddap_datasets:
        sys.stderr.write('No datasets found on ERDDAP instance: {:s}\n'.format(args.erddap_url))
        return 1
        
    # Must have OOI_ERDDAP_ASYNC_HOME set and valid
    async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
    if not async_home:
        sys.stderr.write('OOI_ERDDAP_ASYNC_HOME not defined\n')
        return 1
    if not os.path.isdir(async_home):
        sys.stderr.write('Invalid OOI_ERDDAP_ASYNC_HOME directory: {:s}\n'.format(async_home))
        return 1
    
    # Use the default template datasets destination if none was specified via --destination    
    tmpl_file_destination = args.destination
    if not tmpl_file_destination:
        tmpl_file_destination = os.path.join(async_home,
            'frontend',
            'templating',
            'datasets')
        
    if not os.path.isdir(tmpl_file_destination):
        sys.stderr.write('Invalid template files destination: {:s}\n'.format(tmpl_file_destination))
        return 1
        
    # Location of the ERDDAP template master files    
    tmpl_masters_dir = os.path.join(async_home,
        'frontend',
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
    for dataset in erddap_datasets:
        
        # Create the template dataset destination directory name and see if it exists
        stream = '{:s}-{:s}'.format(dataset['instrument']['stream'],
            dataset['instrument']['method'])
        tmpl_dest_dir = os.path.join(tmpl_file_destination, stream)
        if not os.path.isdir(tmpl_dest_dir):
            sys.stdout.write('Creating stream template directory: {:s}\n'.format(tmpl_dest_dir))
            try:
                os.mkdir(tmpl_dest_dir)
            except OSError as e:
                sys.stderr.write('{:s}\n'.format(e))
                continue
        
        # Look for the template NetCDF file in tmpl_dest_dir
        nc_file = os.path.join(tmpl_dest_dir, '{:s}.nc'.format(stream)) 
        if os.path.isfile(nc_file):
            sys.stderr.write('Template NetCDF file already exists: {:s}\n'.format(nc_file))
            if not args.clobber:
                sys.stderr.write('Skipping stream: {:s} (Use --clobber to overwrite)\n'.format(stream))
                continue
            else:
                sys.stdout.write('Re-downloading stream NetCDF file: {:s}\n'.format(nc_file))
                
#        # Download a very small file to use as the template NetCDF file.  
#        dt1 = parser.parse(dataset['maxTime'])
#        # Subtract 5 minutes from dt1 to get the starting timestamp of the dataset we 
#        # want to download
#        dt0 = dt1 - timedelta(0, 3600)

        # Try to download the file
        sys.stdout.write('Downloading: {:s}\n'.format(nc_file))
        nc_file = download_erddap_nc(args.erddap_url,
            dataset['datasetID'],
            output_filename=nc_file,
            time_delta_type='hours',
            time_delta_value=1,
            clobber=True,
            nc_type='CF',
            print_url=args.url_only);

        if not nc_file:
            continue

        # Create the args filename
        args_fname = '{:s}.erddapDatasets.args'.format(stream)
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
        xml_fname = '{:s}.erddapDatasets.xml'.format(stream)
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
        
        (nc_path, nc_fname) = os.path.split(nc_file)
        # Write the args to args_file
        args_params = {'source_dir' : nc_path,
            'netcdf_filename' : nc_file}
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
    arg_parser.add_argument('erddap_url',
        help='The ERDDAP url containing the datasets of interest')
    arg_parser.add_argument('-d', '--destination',
        help='Alternate template preparation destination directory')
    arg_parser.add_argument('-c', '--clobber',
        action='store_true',
        help='Overwrite any existing args and xml files present in the dataset directory')
    arg_parser.add_argument('-u', '--url_only',
        action='store_true',
        help='Print the request URL, but do not send the request.')
        
    parsed_args = arg_parser.parse_args()
    
    #print vars(parsed_args)
    #sys.exit(13)

    sys.exit(main(parsed_args))

