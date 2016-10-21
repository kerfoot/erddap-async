import sys
import os

def get_valid_dataset_template(stream, telemetry, template_dir=None):
    '''Checks for the existence of an ERDDAP dataset xml template for the specified
    stream and telemetry type.  The filename is returned if the template exists.'''
    
    if not template_dir:
        template_root = os.getenv('OOI_ERDDAP_TEMPLATE_DIR')
        if not template_root:
            sys.stderr.write('No template_dir specified and OOI_ERDDAP_TEMPLATE_DIR environment variable not set\n')
            return
        template_dir = template_root
        
    if not os.path.isdir(template_dir):
        sys.stderr.write('Invalid ERDDAP dataset template directory: {:s}\n'.format(template_dir))
        return
        
    # Create the template file name and see if it exists
    template_xml = os.path.join(template_dir,
        '{:s}-{:s}.dataset.template.xml'.format(stream, telemetry))
    
    if not os.path.isfile(template_xml):
        sys.stderr.write('ERDDAP dataset template does not exist: {:s}\n'.format(template_xml))
        return
        
    return template_xml
    
def write_erddap_dataset_xml(request_response, nc_dir, title=None, summary=None, xml_dest=None, clobber=False):
    
    dataset_xml_file = None
    
    if type(request_response) != dict:
        sys.stderr.write('Invalid JSON response object\n')
        return dataset_xml_file
    elif 'stream' not in request_response.keys():
        sys.stderr.write('request_response is missing the fully qualified stream name\n')
        return dataset_xml_file
    elif not os.path.isdir(nc_dir):
        sys.stderr.write('Invalid NetCDF destination: {:s}\n'.format(nc_dir))
        return dataset_xml_file
    elif xml_dest and not os.path.isdir(xml_dest):
        sys.stderr.write('Invalid XML destination: {:s}\n'.format(xml_dest))
        return dataset_xml_file
        
    # Make sure we have an xml template for this stream
    xml_template_filename = '{:s}-{:s}.erddap.dataset.xml'.format(request_response['instrument']['stream'],
        request_response['instrument']['telemetry'])
    template_file = os.path.join(os.getenv('OOI_ERDDAP_TEMPLATE_HOME'), xml_template_filename)
    if not os.path.isfile(template_file):
        sys.stderr.write('Template does not exist: {:s}\n'.format(template_file))
        return dataset_xml_file
        
    # Create the output xml file name and see if it already exists
    erddap_telemetry_dir = 'erddap-12-2'
    if request_response['instrument']['telemetry'] == 'streamed':
        erddap_telemetry_dir = 'erddap-12-1'
    xml_filename = '{:s}.erddap.dataset.xml'.format(request_response['stream'])
    dataset_xml_file = os.path.join(os.getenv('OOI_ERDDAP_DATA_HOME'),
        erddap_telemetry_dir,
        'stream-xml',
        xml_filename)
    if os.path.isfile(dataset_xml_file) and not clobber:
        sys.stderr.write('Dataset XML file already exists (use clobber=True to overwrite): {:s}\n'.format(dataset_xml_file))
        return None
        
    dataset_id = request_response['stream']
    if not title:
        title = request_response['stream']
    if not summary:
        summary = '{:s} data set gathered as part of the Ocean Observatories Initiative'
        
    dataset_xml = create_erddap_dataset_xml(template_file,
        dataset_id,
        nc_dir,
        title,
        summary)
    if not dataset_xml:
        return dataset_xml_file
        
    # Write the xml to disk
    fid = open(dataset_xml_file, 'w')
    fid.write(dataset_xml)
    fid.close()
        
    return dataset_xml_file
    
def create_erddap_dataset_xml(template_file, dataset_id, nc_dir, title, summary):
    
    dataset_xml = None
    
    if not os.path.isfile(template_file):
        sys.stderr.write('Invalid XML template: {:s}\n'.format(template_file))
        return dataset_xml
        
    fid = open(template_file, 'r')
    template_xml = fid.read()
    fid.close()
    
    try:
        dataset_xml = template_xml.format(dataset_id=dataset_id,
            file_dir=nc_dir,
            title=title,
            summary=summary)
    except IndexError as e:
        sys.stderr.write('Failed to write XML: {:s}\n'.format(e))
        return None
        
    return dataset_xml