import os
import sys
import json
from xml.etree import parse, Element, ElementTree
from asynclib.erddap import create_dataset_xml_filename, create_erddap_dataset_id

xml_template_file = '/Users/kerfoot/code/erddap-async/backend/templating/xml/production/ctdbp_cdef_dcl_instrument-telemetered.dataset.template.xml'
response_json_file = '/Users/kerfoot/code/erddap-async/backend/stream-queue/CE01ISSM-MFD37-03-CTDBPC000-ctdbp_cdef_instrument_recovered-recovered_inst-20161019T182858.1476916138.request.json'
instruments_meta_file = '/Users/kerfoot/code/erddap-async/config/visualocean-instruments-metadata.json'

def create_dataset_xml(reference_designator, stream, telemetry, deployment_number, nc_dir, xml_template_file, instrument_descriptions):
    
    dataset_xml_file = None
    
    # Validate input args
    if not os.path.isdir(nc_dir):
        sys.stderr.write('Invalid NetCDF directory: {:s}\n'.format(nc_dir))
        return
    if not os.path.isfile(xml_template_file):
        sys.stderr.write('Invalid dataset XML file: {:s}\n'.format(xml_template_file))
        return
    if not instrument_descriptions or type(instrument_descriptions) != list:
        sys.stderr.write('instrument_descriptions must be a non-empty array of visualocean instrument metadata\n')
        return
        
    # Make sure the reference designator referers to an instrument in instrument_descriptions
    instruments = [i['reference_designator'] for i in instrument_descriptions]
    if reference_designator not in instruments:
        sys.stderr.write('{:s}: No instrument metadata entry found\n'.format(reference_designator))
        return
        
    instrument_meta = instrument_descriptions[instruments.index(reference_designator)]
    
    # Parse the xml template file
    doc = parse(xml_template_file)
    
    root = doc.getroot()
    # The root element must be 'dataset'
    if root.tag != 'dataset':
        sys.stderr.write('Invalid dataset XML template file (Root element must be of type=dataset): {:s}\n'.format(xml_template_file))
        return
        
    # 1. Create and set the <dataset datasetID=...> attribute
    if 'datasetID' not in root.keys():
        sys.stderr.write('dataset element is missing the datasetID attribute: {:s}\n'.format(xml_template_file))
        return
    dataset_id = create_erddap_dataset_id(reference_designator,
        stream,
        telemetry,
        deployment_number)
    # Set the attribute
    root.set('datasetID', dataset_id)
    
    # 2. Set the <fileDir> attribute to nc_dir
    file_dir_att = root.find('fileDir')
    if not file_dir_att:
        sys.stderr.write('No <fileDir></> element found: {:s}\n'.format(xml_template_file))
        return
    file_dir_att = nc_dir
    
    # Select the <addAttributes></> element
    add_atts_e = root.find('addAttributes')
    if not add_atts_e:
        sys.stderr.write('No <addAttributes></> element found: {:s}\n'.format(xml_template_file))
        return
        
    # 3. Add the <title></> attribute
    title_string = '{:s} {:s} {:s} {:s} data from the {:s} stream - Deployment {:04.0f}'.format(telemetry.capitalize(),
        instrument_meta['site'],
        instrument_meta['subsite'],
        instrument_meta['name'],
        stream,
        deployment_number)
    # Create the attribute element
    title_att_e = Element('att', {'name' : 'title'})
    # Add the element to the DOM
    add_atts_e.append(title_att_e)

    # 4. Modify the <summary></>     
    # Get the list of name attribute values
    atts = add_atts_e.findall('att')
    att_names = [a.get('name') for a in atts]
    i = att_names.index('summary')
    atts[i].text = '{:s}. {:s}'.format(atts[i].text,
        instrument_meta['description'])
    
    # Return the string XML
    return ElementTree.tostring(root)
    