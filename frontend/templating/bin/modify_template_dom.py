#!/usr/bin/env python

from xml.dom import minidom
import sys
import os
import argparse

def main(args):
    
    for orig_xml_file in args.xml_files:
        
        dataset_node = modify_template_dom(orig_xml_file)
        if not dataset_node:
            return 1
        
        # split the file path and name
        (xml_path, xml_file) = os.path.split(orig_xml_file)
        # split the file name and extension
        (xml_file, xml_ext) = os.path.splitext(xml_file)
        
        if args.stdout:
            sys.stdout.write('stdout')
            sys.stdout.write('{:s}'.format(dataset_node.toxml()))
            return 0
            
        # Create the output filename to write the <dataset /> DOM element to
        xml_filename = '{:s}.dataset.xml'.format(xml_file)
        if args.xml_dest:
            xml_path = args.xml_dest
            
        xml_out = os.path.join(xml_path, xml_filename)

        sys.stdout.write('Output XML file: {:s}\n'.format(xml_out))
        if os.path.isfile(xml_out):
            if not args.force:
                sys.stderr.write('XML template exists (Use -f to clobber the existing file\n');
                continue
            else:
                sys.stdout.write('Clobbering existing XML template: {:s}\n'.format(xml_out))

        sys.stdout.write('Writing XML DOM: {:s}\n'.format(xml_out))
        
        # Open the output file and write the <dataset /> element    
        fid = open(xml_out, 'w')
        fid.write(dataset_node.toxml())
        fid.close()
    
    return 0
    
def modify_template_dom(orig_xml_file):
    
    # List of dataVariables that we want to either delete from the DOM
    bad_atts = {'obs',
        'driver_timestamp',
        'internal_timestamp',
        'ingestion_timestamp',
        'port_timestamp',
        'provenance'}
    # List of attributes we want to either add, if not already present, or modify
    # if present
    add_attributes = {'title' : None,
        'summary' : None,
        'institution' : 'Ocean Observatories Initiative'}
    
    if not os.path.isfile(orig_xml_file):
        sys.stderr.write('Invalid file specified: {:s}\n'.format(orig_xml_file))
        return None

    # Parse the DOM
    dom = minidom.parse(orig_xml_file)
    
    # Make sure the DOM has a <erddapDatasets /> node
    erddap = dom.getElementsByTagName('erddapDatasets')
    if not erddap:
        sys.stderr.write('Invalid erddap datasets.xml file: {:s}\n'.format(orig_xml_file))
        return None
    elif len(erddap) != 1:
        sys.stderr.write('XML file contains more than one top level DOM node (<erddapDatasets />): {:s}\n'.format(orig_xml_file))
        return None
    
    # Select the <dataset></dataset> element from the children of the erddap node
    datasets = erddap[0].getElementsByTagName('dataset')
    
    if len(datasets) != 1:
        sys.stderr.write('DOM contains more than one dataset element: {:s}\n'.format(orig_xml_file))
        return None
        
    dataset = datasets[0]
        
    # Set the datasetID
    dataset.setAttribute('datasetID', '{dataset_id}')
    
    # Find and set the <fileDir></fileDir>
    fileDirs = dataset.getElementsByTagName('fileDir')
    if fileDirs and len(fileDirs) == 1:
        fileDir = fileDirs[0]
        fileDir.firstChild.replaceWholeText('{file_dir}')
        
    # 2016-09-09: kerfoot@marine - bug in ERDDAP v1.72[3] displaying bad time values (year=2086)
    # Fix from Bob Simons is to set <updateEveryNMillis /> to -1 and
    # <reloadEveryNMinutes /> to 1
    update_nodes = dataset.getElementsByTagName('updateEveryNMillis')
    if len(update_nodes) == 1:
        update_nodes[0].firstChild.replaceWholeText('-1')
    reload_nodes = dataset.getElementsByTagName('reloadEveryNMinutes')
    if len(reload_nodes) == 1:
        reload_nodes[0].firstChild.replaceWholeText('1')
        
    # Get the first <addAttributes /> node
    addAtts = dataset.getElementsByTagName('addAttributes')[0]
    # Get all <att /> children
    attributes = addAtts.getElementsByTagName('att')
    # Create a list of the attribute 'name' attribute
    att_names = [a.getAttribute('name') for a in attributes]
    # Update or add (if it doesn't exist) the add_attributes
    for (k,v) in add_attributes.items():
        if k not in att_names:
            
            # Create and append the new node
            #sys.stdout.write('Adding global attribute: {:s}\n'.format(k))
            new_att = dom.createElement('att')
            new_att.setAttribute('name', k)
            if v:
                new_att.appendChild(dom.createTextNode('{:s}'.format(v)))
            else:
                new_att.appendChild(dom.createTextNode('{{{:s}}}'.format(k)))
                
            # Append the node
            addAtts.appendChild(new_att)
            # Append newline text node
            addAtts.appendChild(dom.createTextNode('\n'))
        else:
            
            # Modify the existing node
            #sys.stdout.write('Modifying global attribute: {:s}\n'.format(k))
            i = att_names.index(k)
            if v:
                attributes[i].firstChild.replaceWholeText('{:s}'.format(v))
            else:
                attributes[i].firstChild.replaceWholeText('{{{:s}}}'.format(k))
                
    # Delete bad_atts from DOM
    dataset_vars = dataset.getElementsByTagName('dataVariable')
    for dataset_var in dataset_vars:
        source_name = dataset_var.getElementsByTagName('sourceName')[0].firstChild.data
        if source_name in bad_atts:
#            sys.stdout.write('Removing variable: {:s}\n'.format(source_name))
            dataset.removeChild(dataset_var)
            continue
        
    return dataset
            

if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('xml_files',
        nargs="*",
        help='One or more ERDDAP datasets.xml template(s) files')
    arg_parser.add_argument('-f', '--force',
        action='store_true',
        help='Overwrite existing XML template(s)')
    arg_parser.add_argument('-d', '--dest',
        dest='xml_dest',
        help='Alternate destination for writing the output xml file.  Default is to write to the same directory as the source file')
    arg_parser.add_argument('-s', '--stdout',
        dest='stdout',
        action='store_true',
        help='Print xml to STDOUT')

    parsed_args = arg_parser.parse_args()

    #print parsed_args
    #sys.exit(13)

    sys.exit(main(parsed_args))        
