import os
import sys

OOI_ARRAYS = {'CP' : 'Coastal_Pioneer',
    'CE' : 'Coastal_Endurance',
    'GI' : 'Global_Irminger_Sea',
    'GS' : 'Global_Southern_Ocean',
    'GA' : 'Global_Argentine_Basin',
    'RS' : 'Cabled_Array',
    'GP' : 'Global_Station_Papa'}
    
def build_nc_dest(instrument, telemetry, stream, deployment_number):
        
    # Create the directory
    # Map the first 2 characters of the subsite to OOI_ARRAYS
    if instrument[:2] not in OOI_ARRAYS.keys():
        sys.stderr.write('No Array name found for instrument: {:s}\n'.format(instrument))
        return None
    
    i_tokens = instrument.split('-')
    if len(i_tokens) != 4:
        sys.stderr.write('Invalid instrument reference designator: {:s}\n'.format(instrument))
        return None
        
    return os.path.join(OOI_ARRAYS[instrument[:2]],
        i_tokens[0],
        i_tokens[1],
        '{:s}-{:s}'.format(i_tokens[2], i_tokens[3]),
        stream,
        telemetry,
        'deployment{:04.0f}'.format(deployment_number))
