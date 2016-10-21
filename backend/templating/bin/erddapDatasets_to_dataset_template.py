#!/usr/bin/env python

import os
import sys
import argparse
from xml.etree.ElementTree import parse, Element, ElementTree

xml_file = '/Users/kerfoot/code/erddap-admin/templating/xml/alpha/ctdgv_m_glider_instrument-telemetered.template.xml'

def main(args):
    '''Extract the <dataset /> element from the <erddapDatasets /> xml file and
    write the element to a .template.xml file in the same location as the source
    file'''
    
    for xml_file in args.erddap_datasets_xml_files:
        
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
            
        tree = ElementTree(dataset)
        
        (xml_path, xml_fname) = os.path.split(xml_file)
        xml_dest = args.destination
        if not xml_dest:
            xml_dest = xml_path
        
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
        
    parsed_args = arg_parser.parse_args()
    
    sys.exit(main(parsed_args))
    