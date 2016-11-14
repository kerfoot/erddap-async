#!/usr/bin/env python

import os
import sys
import json
import argparse
from UFrame import UFrame

def main(args):
    '''Write the list of actively deployment instruments for each registered subsite
    in the default UFrame instance.  The default UFrame instance is taken from the 
    UFRAME_BASE_URL environment, if set.  Active deployments are queried from the UFrame 
    asset management API.  The resulting JSON catalogs are written the the current
    working directory, one file for each subsite.'''
    
    dest_dir = args.destination
    # Use the current working directory if no alternate destination was specified
    # via -d or --destination
    if not dest_dir:
        dest_dir = os.path.realpath(os.curdir)

    if not os.path.isdir(dest_dir):
            sys.stderr.write('Catalog destination does not exist: {:s}\n'.format(dest_dir))
            return 1
    
    # If the user did not specify a UFrame base url (--baseurl), use the UFRAME_BASE_URL
    # environment variable, if it exists.  If not, exit
    uframe_base_url = args.base_url
    if not uframe_base_url:
        uframe_base_url = os.getenv('UFRAME_BASE_URL')
        
    if not uframe_base_url:
        sys.stderr.write('No UFrame instance specified\n')
        return 1
                
    # Create the UFrame instance
    uframe = UFrame(base_url=uframe_base_url, timeout=args.timeout)
    sys.stdout.write('UFrame: {:s}\n'.format(uframe.base_url))
        
    # Fetch the list of all actively deployment instruments
    sys.stdout.write('Fetching actively deployment instruments...\n')
    deployments = uframe.get_active_deployments()
    if not deployments:
        sys.stderr.write('No active deployments found at UFrame: {:s}\n'.format(uframe.base_url))
        return 1
    
    master_catalog_file = os.path.join(dest_dir, 'subsites-active-catalog.json')
    try:
        with open(master_catalog_file, 'w') as fid:
            sys.stdout.write('Writing master catalog: {:s}\n'.format(master_catalog_file))
            json.dump(deployments, fid)
    except IOError as e:
        sys.stderr.write('{:s}\n'.format(e))
        return 1
        
    for subsite in uframe.arrays:
        
        catalog_file = os.path.join(dest_dir, '{:s}-subsite-active-catalog.json'.format(subsite, uframe.base_url[7:]))
        if os.path.isfile(catalog_file) and not args.clobber:
            sys.stderr.write('Catalog already exists (Use --clobber to overwrite): {:s}\n'.format(catalog_file))
            continue
            
        subsite_deployments = [d for d in deployments if d['instrument']['subsite'] == subsite]
        if not subsite_deployments:
            continue
        
        # Write the events to the catalog if a destination was specified
        try:
            with open(catalog_file, 'w') as fid:
                json.dump(subsite_deployments, fid)   
                sys.stdout.write('{:s}\n'.format(catalog_file)) 
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
