#!/usr/bin/env python

import argparse
import sys
from asynclib.erddap import fetch_erddap_datasets
import csv
import json

def main(args):
    '''Fetch the allDatasets.json request at the specified erddap_url.  Prints the
    results STDOUT as comma-separated value records'''
    
    metadata = fetch_erddap_datasets(args.erddap_url,
        dataset_id=args.dataset_id,
        full_listing=args.full)
        
    if args.json:
        sys.stdout.write('{:s}\n'.format(json.dumps(metadata)))
    else:
        if metadata:
            csv_writer = csv.writer(sys.stdout)
            cols = metadata[0].keys()
            cols.pop(cols.index('instrument'))
            csv_writer.writerow(cols)
            for m in metadata:
                csv_writer.writerow([m[k] for k in cols])
                
    return 0
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('erddap_url',
        help='ERDDAP tabledap or griddap server URL')
    arg_parser.add_argument('-j', '--json',
        action='store_true',
        help='Print a valid JSON object instead of csv')
    arg_parser.add_argument('-d', '--dataset_id',
        help='Valid ERDDAP dataset ID for the specified ERDDAP server.  Information about this dataset is returned only.')
    arg_parser.add_argument('-f', '--full',
        action='store_true',
        help='Set to True to get the full dataset metadata listing <Default=False>')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
