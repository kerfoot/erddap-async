#!/usr/bin/env python

import os
import sys
from shutil import copyfile
import argparse

def main(args):
    '''Write the generateDatasetsXml args and xml template files suitable for
    automating the generation of the ERDDAP dataset xml template creation'''

    status = 0

#    has_nc_files = False
#    for nc in args.nc_files:
#        if os.path.isfile(nc):
#            has_nc_files = True
#            break
#    if not has_nc_files:
#        sys.stderr.write('No valid NetCDF files found\n')
#        return 1

    # Find and validate the args template
    args_template = args.args_template
    if not args.args_template:
        DATA_HOME = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        if not DATA_HOME:
            sys.stderr.write('Location of args template not specified and OOI_ERDDAP_ASYNC_HOME environment variable not set')
            return 1
        elif not os.path.isdir(DATA_HOME):
            sys.stderr.write('Invalid OOI_ERDDAP_ASYNC_HOME directory: {:s}\n'.format(DATA_HOME))
            return 1

        # Fully-qualified path to the args template file
        args_template = os.path.join(DATA_HOME,
            'frontend',
            'templating',
            'masters',
            'generateDatasetsXmlTemplate.args')

    # Validate the args template file
    if not os.path.isfile(args_template):
        sys.stderr.write('Invalid GenerateDatasetsXml args template: {:s}\n'.format(args_template))
        return 1

    sys.stdout.write('args template: {:s}\n'.format(args_template))

    # Find and validate the xml template
    datasets_xml_template = args.xml_template
    if not args.xml_template:
        DATA_HOME = os.getenv('OOI_ERDDAP_ASYNC_HOME')
        if not DATA_HOME:
            sys.stderr.write('Location of xml template not specified and OOI_ERDDAP_ADMIN_HOME environment variable not set')
            return 1
        elif not os.path.isdir(DATA_HOME):
            sys.stderr.write('Invalid OOI_ERDDAP_ADMIN_HOME directory: {:s}\n'.format(DATA_HOME))
            return 1

        # Fully-qualified path to the args template file
        datasets_xml_template = os.path.join(DATA_HOME,
            'frontend',
            'templating',
            'masters',
            'datasets-xml-template.xml')

    # Validate the datasets xml template file
    if not os.path.isfile(datasets_xml_template):
        sys.stderr.write('Invalid  datasets.xml template: {:s}\n'.format(datasets_xml_template))
        return 1

    sys.stdout.write('xml template : {:s}\n'.format(datasets_xml_template))
            
    # Read in the args_template
    fid = open(args_template, 'r')
    args_tmpl = fid.read()
    fid.close()
    
    # Tasks:
    # 1. Create a args file, using args_template, for each NetCDF file that we 
    #   want  to generate an ERDDAP datasets.xml snippet
    # 2. Create a datasets.xml file that we'll use to write the xml output of
    #   GenerateDatasetsXml.sh to
    for nc in args.nc_files:

        if not os.path.isfile(nc):
            sys.stderr.write('File does not exist: {:s}\n'.format(nc))
            continue

        (nc_path, nc_file) = os.path.split(nc)
        
        sys.stdout.write('Stream template NetCDF: {:s}\n'.format(nc))
 
        # Strip the file extension off
        (template_string, ext) = os.path.splitext(nc)

        # Create and make sure the stream xml template and args file don't
        # already exist as we do not want to overwrite anything already there
        stream_xml_template = os.path.join(
                nc_path,
                '{:s}.template.xml'.format(template_string))
        sys.stdout.write('Stream datasets.xml file: {:s}\n'.format(stream_xml_template))
        if os.path.isfile(stream_xml_template):
            if not args.force:
                sys.stderr.write('Stream xml template already exists (Use -f to clobber)\n')
                continue
            else:
                sys.stdout.write('Clobbering existing xml template\n')

        args_file = os.path.join(nc_path, '{:s}.args'.format(template_string))
        sys.stdout.write('Stream args file: {:s}\n'.format(args_file))
        if os.path.isfile(args_file):
            if not args.force:
                sys.stderr.write('args file already exists (Use -f to clobber)\n')
                continue
            else:
                sys.stdout.write('Clobbering existing args\n')

        # Make a copy the datasets_xml_template for the nc_file
        try:
            copyfile(datasets_xml_template, stream_xml_template)
        except IOError as e:
            sys.stderr.write('Error creating stream datasets.xml file: {:s}\n'.format(e))
            continue
    
        arguments = {'source_dir' : nc_path,
                'netcdf_filename' : nc}
        args_data = args_tmpl.format(**arguments)
    
        # Create a copy of the args template file using nc_file and write the args
        # to it
        try:
            fid = open(args_file, 'w')
            fid.write(args_data)
            fid.close()
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
            
    return status

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('nc_files',
        nargs="*",
        help='One or more NetCDF files used to build the ERDDAP templates')
    arg_parser.add_argument('-a', '--args_template',
        dest="args_template",
        help='Location of the args template.  If not specified, this location is built using the OOI_ERDDAP_ADMIN_HOME environment variable')
    arg_parser.add_argument('-x', '--xml_template',
        dest="xml_template",
        help='Location of the datasets.xml template.  If not specified, this location is built using the OOI_ERDDAP_ADMIN_HOME environment variable')
    arg_parser.add_argument('-f', '--force',
        dest='force',
        action='store_true',
        help='Clobber existing xml and args files if they already exist')

    parsed_args = arg_parser.parse_args()

    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))
