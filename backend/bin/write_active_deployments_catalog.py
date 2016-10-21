#!/usr/bin/env python

import os
import sys
import json
import argparse
from UFrame import UFrame

def main(args):
    '''Write the list of actively deployment instruments from the default UFrame
    instance.  The default UFrame instance is taken from the UFRAME_BASE_URL
    environment.  Active deployments are queried from the UFrame asset management 
    API.  If no destination is specified, the resulting JSON catalog is printed
    to STDOUT.'''
    
    if args.destination and not os.path.isdir(args.destination):
            sys.stderr.write('Catalog destination does not exist: {:s}\n'.format(args.destination))
            return 1
            
    # Create the UFrame instance
    uframe = UFrame(base_url=args.base_url, timeout=args.timeout)
    
    # Write the entire catalog as JSON
    if args.destination:
        catalog = os.path.join(args.destination, '{:s}-active-catalog.json'.format(uframe.base_url[7:]))
        if os.path.isfile(catalog) and not args.clobber:
            sys.stderr.write('Catalog already exists (Use --clobber to overwrite): {:s}\n'.format(catalog))
            return 1
        
    # Fetch the list of all actively deployment instruments
    deployments = uframe.get_active_deployments()
    if not deployments:
        sys.stderr.write('No active deployments found at UFrame: {:s}\n'.format(uframe.base_url))
        return 1
        
    # Print the catalog to stdout if no destination specified
    if not args.destination:
        sys.stdout.write('{:s}\n'.format(json.dumps(deployments)))
        return 0
    
    # Write the events to the catalog if a destination was specified
    try:
        with open(catalog, 'w') as fid:
            json.dump(deployments, fid)   
            sys.stdout.write('{:s}\n'.format(catalog)) 
    except IOError as e:
        sys.stderr.write('{:s}\n'.format(e))
        return 1
    
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('-d', '--destination',
        dest='destination',
        help='Specify the location for the catalog file, which must exist.')
    arg_parser.add_argument('-c', '--clobber',
        dest='clobber',
        action='store_true',
        help='Clobber if the catalog file already exists')
    arg_parser.add_argument('-b', '--baseurl',
        dest='base_url',
        help='Specify an alternate uFrame server URL. Must start with \'http://\'.  Value is taken from the UFRAME_BASE_URL environment variable, if set')
    arg_parser.add_argument('-t', '--timeout',
        type=int,
        default=120,
        help='Specify the timeout, in seconds (Default is 120 seconds).')

    parsed_args = arg_parser.parse_args()
    
    sys.exit(main(parsed_args))
