#!/usr/bin/env python

import argparse
import sys
from asynclib.erddap import download_erddap_nc

def main(args):
    '''Download a flat, table-like, NetCDF-3 binary file for the specified datasetID,
    with COARDS/CF/ACDD metadata from the specified erddap_base_url.  The entire time 
    series is downloaded by default.  Unless specified via the -o option, the NetCDF
    file is written to the current working directory using a unique, automatically
    generated filename.
    '''
    
    nc_file = download_erddap_nc(args.erddap_url,
        args.dataset_id,
        output_filename=args.output_file,
        nc_type=args.type,
        time_delta_type=args.time_delta_type,
        time_delta_value=args.time_delta_value,
        start_time=args.start_date,
        end_time=args.end_date,
        clobber=args.clobber,
        print_url=args.url_only)
        
    if nc_file:
        sys.stdout.write('{:s}\n'.format(nc_file))
        return 0
    else:
        return 1
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('erddap_url',
        help='ERDDAP tabledap or griddap server URL')
    arg_parser.add_argument('dataset_id',
        help='Valid ERDDAP dataset ID for the specified ERDDAP server')
    arg_parser.add_argument('-o', '--output_file',
        help='Filename for the downloaded file')
    arg_parser.add_argument('-t', '--type',
        help='Type of NetCDF file desired if different from the default.  See the ERDDAP documentation for available NetCDF types',
        choices=['CF', 'CFMA'])
    arg_parser.add_argument('--time_delta_type',
        help='Type for calculating the subset start time, i.e.: years, months, weeks, days.  Must be a type kwarg accepted by dateutil.relativedelta')
    arg_parser.add_argument('--time_delta_value',
        type=int,
        help='Positive integer value to subtract from the end time to get the start time for subsetting.')
    arg_parser.add_argument('-s', '--start_date',
        help='An ISO-8601 formatted string specifying the start time/date for the data set')
    arg_parser.add_argument('-e', '--end_date',
        help='An ISO-8601 formatted string specifying the end time/data for the data set')
    arg_parser.add_argument('-c', '--clobber',
        action='store_true',
        help='Clobber existing file with the same name')
    arg_parser.add_argument('-u', '--url_only',
        action='store_true',
        help='Print the request URL, but do not send the request.')

    parsed_args = arg_parser.parse_args()

    sys.exit(main(parsed_args))
