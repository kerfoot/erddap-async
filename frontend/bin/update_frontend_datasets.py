#!/usr/bin/env python

import os
import sys
import glob
import argparse
from subprocess import check_output, CalledProcessError
from dateutil import parser
from asynclib.erddap import *
from asynclib.templating import get_valid_dataset_template
from asynclib.filesystem import build_nc_dest

def main(args):
    '''Compare the existing uncabled tabledap ERDDAP frontend datasets with the 
    corresponding ERDDAP backend datasets.  New datasets are created and existing 
    datasets are updated if the start and/or end times differ'''
    
    if args.debug:
        sys.stdout.write('==> DEBUG MODE: No file operations performed! <==\n')
        
    # Map of cable types to erddap instance
    _CABLE_TYPES = {'uncabled' : 'erddap-11-2',
        'cabled' : 'erddap-11-1'}

    erddap_backend_base_url = os.getenv('OOI_ERDDAP_BACKEND_BASE_URL')
    if not erddap_backend_base_url:
        sys.stderr.write('OOI_ERDDAP_BACKEND_BASE_URL not set\n')
        return 1
    elif not erddap_backend_base_url.startswith('http'):
        sys.stderr.write('OOI_ERDDAP_BACKEND_BASE_URL must begin with http\n')
        return 1
    erddap_frontend_base_url = os.getenv('OOI_ERDDAP_FRONTEND_BASE_URL')
    if not erddap_frontend_base_url:
        sys.stderr.write('OOI_ERDDAP_FRONTEND_BASE_URL not set\n')
        return 1
    elif not erddap_frontend_base_url.startswith('http'):
        sys.stderr.write('OOI_ERDDAP_FRONTEND_BASE_URL must begin with http\n')
        return 1
    
    # Location of ERDDAP dataset xml templates
    async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
    if not async_home:
        sys.stderr.write('OOI_ERDDAP_ASYNC_HOME not set\n')
        return 1
    elif not os.path.isdir(async_home):
        sys.stderr.write('Invalid OOI_ERDDAP_ASYNC_HOME directory: {:s}\n'.format(async_home))
        return 1
    template_dir = os.path.join(async_home,
        'frontend',
        'templating',
        'xml',
        'production')
        
    data_home = os.getenv('OOI_ERDDAP_DATA_HOME')
    if not data_home:
        sys.stderr.write('OOI_ERDDAP_DATA_HOME not set\n')
        return 1
    elif not os.path.isdir(data_home):
        sys.stderr.write('Invalid OOI_ERDDAP_DATA_HOME directory: {:s}\n'.format(data_home))
        return 1
        
    # Get the list of available backend datasets
    backend_erddap_url = '{:s}/{:s}/erddap/{:s}'.format(erddap_backend_base_url,
        args.cable_type,
        args.dap_type)
    backend_datasets = fetch_erddap_datasets(backend_erddap_url)
    
    # Get the list of available frontend datasets
    frontend_erddap_url = '{:s}/{:s}/erddap/{:s}'.format(erddap_frontend_base_url,
        args.cable_type,
        args.dap_type)
    frontend_datasets = fetch_erddap_datasets(frontend_erddap_url)
    # Create the list of frontend dataset ids
    frontend_dataset_ids = [d['datasetID'] for d in frontend_datasets]
    
    for dataset in backend_datasets:
        
        sys.stdout.write('\nChecking backend dataset: {:s}\n'.format(dataset['datasetID']))
        
        if dataset['datasetID'] in frontend_dataset_ids:
            
            sys.stdout.write('Existing frontend dataset: {:s}\n'.format(dataset['datasetID']))
            
            # Find the array element
            i = frontend_dataset_ids.index(dataset['datasetID'])
            
            # Convert dataset times to datetimes
            frontend_dt0 = parser.parse(frontend_datasets[i]['minTime'])
            frontend_dt1 = parser.parse(frontend_datasets[i]['maxTime'])
            backend_dt0 = parser.parse(dataset['minTime'])
            backend_dt1 = parser.parse(dataset['maxTime'])
    
            updated = False
            if frontend_dt0 != backend_dt0:
                sys.stdout.write('Backend dataset start time has changed: {:s}\n'.format(dataset['datasetID']))
                updated = True
            if frontend_dt1 != backend_dt1:
                sys.stdout.write('Backend dataset end time has changed: {:s}\n'.format(dataset['datasetID']))
                updated = True
                
            if not updated:
                continue
                
            sys.stdout.write('Updating frontend dataset: {:s}\n'.format(dataset['datasetID']))
            
            # Create the ERDDAP product directory
            nc_dest_product_dir = build_nc_dest(dataset['instrument']['reference_designator'],
                dataset['instrument']['method'],
                dataset['instrument']['stream'],
                dataset['instrument']['deployment_number'])
            dest_nc_dir = os.path.join(data_home,
                _CABLE_TYPES[args.cable_type],
                'nc',
                nc_dest_product_dir)
                
            # Create the ERDDAP product directory if it does not exist
            sys.stdout.write('ERDDAP destination: {:s}\n'.format(dest_nc_dir))
            if not os.path.isdir(dest_nc_dir):
                if not args.debug:
                    try:
                        sys.stdout.write('Creating ERDDAP destination directory\n')
                        os.makedirs(dest_nc_dir)
                    except OSError as e:
                        sys.stderr.write('{:s}\n'.format(e))
                        continue
                        
            # Download the complete dataset as a single NetCDF file
            nc_fname = '{:s}.ncCF-3.nc.tmp'.format(dataset['datasetID'])
            nc_file = os.path.join(dest_nc_dir, nc_fname)
            if not args.debug:
                sys.stdout.write('Requesting UPDATED dataset: {:s}\n'.format(dataset['datasetID']))
                downloaded_nc_file = download_erddap_nc(backend_erddap_url,
                    dataset['datasetID'],
                    output_filename=nc_file,
                    nc_type='CF')
                # Skip this dataset if the file was not downloaded
                if not downloaded_nc_file:
                    continue
                    
                sys.stdout.write('Temp NetCDF-3 file written: {:s}\n'.format(downloaded_nc_file))
                
            # Delete existing NetCDF files since we'll replace them with a new updated
            # version
            old_nc_files = glob.glob(os.path.join(dest_nc_dir, '*.nc'))
            for nc in old_nc_files:
                if args.debug:
                    sys.stdout.write('Found existing NetCDF file: {:s}\n'.format(nc))
                else:
                    sys.stdout.write('Deleting existing NetCDF file: {:s}\n'.format(nc))
                    try:
                        os.unlink(nc)
                    except OSError as e:
                        sys.stderr.write('{:s}\n'.format(e))
            
            if not args.debug:
                # convert the .ncCF-3.nc.tmp file to NetCDF 4 using ncks
                nc4_file = '{:s}.ncCF-4.nc'.format(downloaded_nc_file.split('.')[0])
                try:
                    sys.stdout.write('Converting to NetCDF-4 and compressing: {:s}\n'.format(nc4_file))
                    check_output(['ncks', '-4', '-L 1', downloaded_nc_file, nc4_file])
                    os.unlink(downloaded_nc_file)
                except (CalledProcessError, OSError) as e:
                    sys.stderr.write('NetCDF-3 to NetCDF-4 conversion or delete failed: {:s}\n'.format(e))
    
        else:
            
            sys.stdout.write('New frontend dataset: {:s}\n'.format(dataset['datasetID']))
            
            # See if a dataset.xml file already exists for this deployment
            datasets_xml_dir = os.path.join(data_home,
                _CABLE_TYPES[args.cable_type],
                'stream-xml')
            if not os.path.isdir(datasets_xml_dir):
                sys.stderr.write('Invalid stream XML directory: {:s}\n'.format(datasets_xml_dir))
                continue
                
            # Create the name of the dataset.xml file
            xml_filename = '{:s}.dataset.xml'.format(dataset['datasetID'])
            # Fully-qualified path to the dataset XML file, provided it exists    
            dataset_xml_file = os.path.join(datasets_xml_dir, xml_filename)
            if os.path.isfile(dataset_xml_file):
                sys.stderr.write('Skipping existing dataset xml file: {:s}\n'.format(dataset_xml_file))
                continue
                
            # See if a template exists for this request
            dataset_template = get_valid_dataset_template(dataset['instrument']['stream'],
                dataset['instrument']['method'],
                template_dir=template_dir)
            if not dataset_template:
                continue
                
            # Create the ERDDAP product directory
            nc_dest_product_dir = build_nc_dest(dataset['instrument']['reference_designator'],
                dataset['instrument']['method'],
                dataset['instrument']['stream'],
                dataset['instrument']['deployment_number'])
            dest_nc_dir = os.path.join(data_home,
                _CABLE_TYPES[args.cable_type],
                'nc',
                nc_dest_product_dir)
            # Create the ERDDAP product directory if it does not exist
            sys.stdout.write('ERDDAP destination: {:s}\n'.format(dest_nc_dir))
            if not os.path.isdir(dest_nc_dir):
                if not args.debug:
                    try:
                        sys.stdout.write('Creating ERDDAP destination directory\n')
                        os.makedirs(dest_nc_dir)
                    except OSError as e:
                        sys.stderr.write('{:s}\n'.format(e))
                        continue
                        
            # Download the complete dataset as a single NetCDF file
            nc_fname = '{:s}.ncCF-3.nc.tmp'.format(dataset['datasetID'])
            nc_file = os.path.join(dest_nc_dir, nc_fname)
            if not args.debug:
                sys.stdout.write('Requesting NEW dataset: {:s}\n'.format(dataset['datasetID']))
                downloaded_nc_file = download_erddap_nc(backend_erddap_url,
                    dataset['datasetID'],
                    output_filename=nc_file,
                    nc_type='CF')
                # Skip this dataset if the file was not downloaded
                if not downloaded_nc_file:
                    continue
                    
                sys.stdout.write('Temp NetCDF-3 file written: {:s}\n'.format(downloaded_nc_file))
                
            # There shouldn't be NetCDF files, but check and remove any if they are present
            old_nc_files = glob.glob(os.path.join(dest_nc_dir, '*.nc'))
            if old_nc_files:
                for nc in old_nc_files:
                    if args.debug:
                        sys.stdout.write('Found existing NetCDF file: {:s}\n'.format(nc))
                    else:
                        sys.stdout.write('Deleting existing NetCDF file: {:s}\n'.format(nc))
                        try:
                            os.unlink(nc)
                        except OSError as e:
                            sys.stderr.write('{:s}\n'.format(e))
                            
            if not args.debug:
                try:
                    # convert the .ncCF-3.nc.tmp file to NetCDF 4 using ncks
                    nc4_file = '{:s}.ncCF-4.nc'.format(downloaded_nc_file.split('.')[0])
                    sys.stdout.write('Converting to NetCDF-4 and compressing: {:s}\n'.format(nc4_file))
                    check_output(['ncks', '-4', '-L 1', downloaded_nc_file, nc4_file])
                    os.unlink(downloaded_nc_file)
                except (CalledProcessError, OSError) as e:
                    sys.stderr.write('NetCDF-3 to NetCDF-4 conversion or delete failed: {:s}\n'.format(e))
            
                # Create the XML    
                dataset_xml = create_frontend_dataset_xml(os.path.dirname(downloaded_nc_file),
                    dataset_template,
                    dataset['datasetID'])
                    
                if not dataset_xml:
                    sys.stderr.write('Failed to write dataset xml for dataset ID: {:s}\n'.format(dataset['datasetID']))
                    continue
        
                # Write the XML file
                try:
                    with open(dataset_xml_file, 'w') as fid:
                        sys.stdout.write('Writing XML: {:s}\n'.format(dataset_xml_file))
                        fid.write('{:s}\n'.format(dataset_xml))
                except IOError as e:
                    sys.stderr.write('{:s}\n'.format(e))
                    continue
                    
    if args.debug:
        sys.stdout.write('\n==> DEBUG MODE: No file operations performed! <==\n')

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-c', '--cabletype',
        choices=['cabled', 'uncabled'],
        dest='cable_type',
        default='uncabled',
        help='Specify the asset type.  <Default=uncabled>')
    arg_parser.add_argument('-d', '--daptype',
        choices=['tabledap', 'griddap'],
        dest='dap_type',
        default='tabledap',
        help='Specify the ERDDAP data type.  <Default=tabledap>')
    arg_parser.add_argument('-x',
        dest='debug',
        action='store_true',
        help='Print the status of each dataset, but do not perform any file/dataset operations')

    parsed_args = arg_parser.parse_args()
    #print(parsed_args)
    #sys.exit(13)
    
    sys.exit(main(parsed_args))
