#!/usr/bin/env python

import os
import sys
import argparse
import glob
from xml.etree.ElementTree import parse, Element, ElementTree

xml_file = '/Users/kerfoot/code/erddap-admin/templating/xml/alpha/ctdgv_m_glider_instrument-telemetered.template.xml'

def main(args):
    '''Extract the <dataset /> element from the <erddapDatasets /> xml file and
    write the element to a .template.xml file in the same location as the source
    file. The obs dataVariable is removed and additional global attributes are
    added to the XML.  By default, the new xml files are written to the same
    directory as they source files.'''
    
    # See if the OOI_ERDDAP_ASYNC_HOME/config/nc/attributes directory exists.
    # This directory may contain .txt files that should be added as global
    # attributes to the dataset XML template files
    new_global_attributes = {}
    async_home = os.getenv('OOI_ERDDAP_ASYNC_HOME')
    if not os.path.isdir(async_home):
        sys.stderr.write('OOI_ERDDAP_ASYNC_HOME is not set: No additional global attributes will be added\n')
    else:
        nc_atts_dir = os.path.join(async_home,
            'config',
            'nc',
            'attributes')
        if not os.path.isdir(nc_atts_dir):
            sys.stderr.write('NetCDF additional global attributes directory does not exist: {:s}\n'.format(nc_atts_dir))
            sys.stderr.write('No additional global attributes will be added\n')
        else:
            # Search for additional global attributes files
            att_txt_files = glob.glob(os.path.join(nc_atts_dir, '*.txt'))
            for f in att_txt_files:
                try:
                    with open(f, 'r') as fid:
                        att_text = fid.read()
                        if not att_text:
                            att_text = 'null'
                        (fp, fn) = os.path.split(f)
                        (k,ext) = os.path.splitext(fn)
                        new_global_attributes[k] = att_text
                except IOError as e:
                    sys.stderr.write('{:s}\n'.format(e))
            
    for xml_file in args.erddap_datasets_xml_files:
    
        (xml_path, xml_fname) = os.path.split(xml_file)
        xml_dest = args.destination
        if not xml_dest:
            xml_dest = os.path.realpath(xml_path)
    
        if not os.path.isdir(xml_dest):
            sys.stderr.write('Invalid XML destination: {:s}\n'.format(xml_dest))
            continue
       
        # split the filename on '.'.  If there are 3 tokens and the 2nd element
        # is erddapDatasets, replace it with dataset
        xml_tokens = xml_fname.split('.')
        if xml_tokens[1] == 'erddapDatasets':
            xml_tokens[1] = 'dataset'
            xml_fname = '.'.join(xml_tokens)
            
        (xml_fname, ext) = os.path.splitext(xml_fname)
        # Append 'template' to the output file
        out_xml_file = os.path.join(xml_dest, '{:s}.template.xml'.format(xml_fname))
        if os.path.isfile(out_xml_file) and not args.clobber:
            sys.stderr.write('Skipping file (Output file already exists - remove the file or use the --clobber option to overwrite): {:s}\n'.format(out_xml_file))
            continue
            
        # Validate the input xml file
        if not os.path.isfile(xml_file):
            sys.stderr.write('Invalid file: {:s}\n'.format(xml_file))
            continue
        
        # Parse the xml file
        doc = parse(xml_file)
        
        # Get the root element
        root = doc.getroot()
    
        if root.tag != 'erddapDatasets':
            sys.stderr.write('Root element is not of type <erddapDatasets> ({:s})\n'.format(root.tag))
            continue
            
        # Get the first dataset tag (should only be one)
        dataset = root.find('dataset')
        if dataset is None:
            sys.stderr.write('No <dataset /> element found: {:s}\n'.format(xml_file))
            continue
            
        # Add additional global attributes if there are files (att_txt_file)
        if new_global_attributes:
            add_attributes_e = dataset.find('addAttributes')
            if add_attributes_e is not None:
                for (k,v) in new_global_attributes.items():
                    g_att = Element('att', {'name' : k})
                    g_att.text = v
                    add_attributes_e.append(g_att)
            
        # Find and remove the obs data variable provided it exists
        data_vars = dataset.findall('dataVariable')
        obs_vars = [d for d in data_vars if d.find('sourceName').text == 'obs']
        if obs_vars:
            for obs_var in obs_vars:
                dataset.remove(obs_var)

        tree = ElementTree(dataset)
        
        try:
            with open(out_xml_file, 'w') as fid:
                tree.write(fid)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            
        sys.stdout.write('{:s}\n'.format(out_xml_file))
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('erddap_datasets_xml_files',
        nargs='+',
        help='One or more ERDDAP datasets.xml file(s) containing a single dataset element')
    arg_parser.add_argument('-d', '--destination',
        help='Alternate output xml destination')
    arg_parser.add_argument('-c', '--clobber',
        action='store_true',
        help='Overwrite existing output template files if they exist')
        
    parsed_args = arg_parser.parse_args()
    
    sys.exit(main(parsed_args))
    
