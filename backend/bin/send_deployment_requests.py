#!/usr/bin/env python

import os
import sys
import json
import argparse
from UFrame import UFrame
from asynclib.templating import get_valid_dataset_template

def main(args):
    '''Create and send the request urls for all deployment objects contained in 
    the specified deployment JSON files.'''
    
    status = 0
   
    # Make sure the user specified .json files to look at
    if not args.deployment_json_files:
        sys.stderr.write('No deployment JSON files specified\n')
        return 1
    
    # Set up the user.  A user is required
    if args.user:
        user = args.user
    else:
        user = os.getenv('OOI_ERDDAP_USER')
    if not user:
        sys.stderr.write('No user specified and OOI_ERDDAP_USER environment variable not set\n')
        return 1
        
    # Must have a valid ERDDAP template dir
    template_dir = args.template_dir
    if not template_dir:
        if not os.getenv('OOI_ERDDAP_ASYNC_HOME'):
            sys.stderr.write('No ERDDAP template directory specified and OOI_ERDDAP_ASYNC_HOME environment variable not set\n')
            return 1
        template_dir = os.path.join(os.getenv('OOI_ERDDAP_ASYNC_HOME'),
            'backend',
            'templating',
            'xml',
            'production')
    if not os.path.isdir(template_dir):
        sys.stderr.write('Invalid ERDDAP XML template directory: {:s}\n'.format(template_dir))
        return 1
   
    # Send the requests from each .json file specified on the command line
    for deployment_file in args.deployment_json_files:
        
        try:
            with open(deployment_file, 'r') as fid:
                deployments = json.load(fid)
        except IOError as e:
            sys.stderr.write('{:s}\n'.format(e))
            continue
        
        if not deployments:
            sys.stderr.write('JSON file contains no deployment event(s): {:s}\n'.format(deployment_file))
            
        # Create a UFrame instance
        uframe = UFrame(base_url=deployments[0]['request_params']['uframe_url'])    
        
        data_requests = []        
        for deployment in deployments:
            
            if not deployment['deployment']['valid']:
                continue
                
            deployment['request_url'] = None
            
            # See if an ERDDAP dataset template exists
            dataset_template = get_valid_dataset_template(deployment['stream']['stream'],
                deployment['stream']['method'],
                template_dir=template_dir)
            if not dataset_template:
                continue
                
            # Check the current UFrame instance.  If the base url is different, create
            # a new instance with the deployment['request_params']['uframe_url']
            if uframe.base_url != deployment['request_params']['uframe_url']:
                sys.stderr.write('Creating new UFrame instance: {:s}\n'.format(deployment['request_params']['uframe_url']))
                uframe = UFrame(base_url=deployment['request_params']['uframe_url'])
                
            # Create some parameters to feed to the request builder
            ts0 = deployment['request_params']['ts0']
            if deployment['request_params']['ts1']:
                ts1 = deployment['request_params']['ts1']
            else:
                ts1 = None
                
            # Create the stream request
            request_urls = uframe.instrument_to_query(deployment['stream']['reference_designator'],
                stream=deployment['stream']['stream'],
                telemetry=deployment['stream']['method'],
                begin_ts=ts0,
                end_ts=ts1,
                time_check=args.time_check,
                exec_dpa=args.no_dpa,
                application_type='netcdf',
                provenance=args.provenance,
                limit=-1,
                #annotations=args.annotations,
                user=user,
                email=args.email,
                selogging=args.selogging)

            if not request_urls:
                sys.stderr.write('{:s}-{:s}-deployment{:04.0f}: No valid urls created\n'.format(
                    deployment['stream']['reference_designator'], deployment['stream']['stream'], deployment['deployment']['deployment_number']))
                continue
            elif len(request_urls) != 1:
                sys.stderr.write('{:s}-{:s}-deployment{:04.0f}: Multiple valid urls created\n')
                for url in request_urls:
                    sys.stderr.write('URL: {:s}\n'.format(url))
                continue

            # Add the request url to the deployment object
            deployment['request_url'] = request_urls[0]
            
            data_requests.append(deployment)
            
        # If no request response destination was specified, print the request objects
        # to STDOUT, but do NOT send the requests
        if not args.destination:
            for data_request in data_requests:
                sys.stdout.write('{:s}\n'.format(data_request['request_url']))
            continue
        
        # Otherwise, send the requests, add the response to the request object and
        # write the object to a file so that we can use it to move files later
        for data_request in data_requests:
    
            deployment_request = '{:s}-{:s}-{:s}-deployment{:04.0f}'.format(
                data_request['stream']['reference_designator'],
                data_request['stream']['stream'],
                data_request['stream']['method'],
                data_request['deployment']['deployment_number'])
    
            # Create the response file and see if it exists
            response_file = os.path.join(args.destination, '{:s}.response.json'.format(deployment_request))
            if os.path.isfile(response_file):
                sys.stderr.write('Request file already exists: {:s}\n'.format(response_file))
                continue
        
            if not data_request['request_url']:
                sys.stderr.write('{:s}: No request_url specified\n'.format(deployment_request))
    
            if uframe.base_url != data_request['request_params']['uframe_url']:
                #sys.stderr.write('Creating new UFrame instance: {:s}\n'.format(deployment['request_params']['uframe_url']))
                uframe = UFrame(base_url=deployment['request_params']['uframe_url'])
            
            # send the data request
            sys.stdout.write('Sending deployment request: {:s}...\n'.format(deployment_request))
            
            get_response = uframe.send_async_requests(data_request['request_url'])
            if not get_response:
                sys.stderr.write('{:s}: Error sending request\n'.format(data_request['request_url']))
            elif len(get_response) > 1:
                sys.stderr.write('{:s}: Multiple unknown requests sent\n'.format(data_request['request_url']))
    
            data_request['response'] = get_response[0]
            response = get_response[0]
            if not response['status']:
                sys.stderr.write('{:s}: {:s}\n'.format(response['reason'], data_request['request_url']))
                continue
    
            sys.stdout.write('Writing response: {:s}\n'.format(response_file))
            with open(response_file, 'w') as fid:
                json.dump(data_request, fid)
    
    return status
    
if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description=main.__doc__)
    arg_parser.add_argument('deployment_json_files',
        nargs='+',
        help='One or more catalog deployment JSON catalog files')
    arg_parser.add_argument('-d', '--destination',
        dest='destination',
        help='Specify the destination for the resulting request files.  The request objects are printed to STDOUT if not specified (no requests are sent)')
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
    arg_parser.add_argument('--template_dir',
        type=str,
        help='Specify the ERDDAP dataset templates directory.  Derived from OOI_ERDDAP_ASYNC_HOME if not specified')
    arg_parser.add_argument('-u', '--user',
        dest='user',
        type=str,
        help='Add a user name to the query')
    arg_parser.add_argument('--no_dpa',
        action='store_false',
        help='Prevent the execution of all data product algorithms to return L1/L2 parameters')
    arg_parser.add_argument('--provenance',
        action='store_true',
        help='Include source file  provenance information in the data sets')
    arg_parser.add_argument('--no_time_check',
        dest='time_check',
        default=True,
        action='store_false',
        help='Do not replace invalid request start and end times with stream metadata values if they fall out of the stream time coverage')
    arg_parser.add_argument('--email',
        dest='email',
        type=str,
        help='Add an email address for emailing UFrame responses to the request once sent')
    arg_parser.add_argument('--selogging',
        action='store_true',
        help='Include advanced stream engine logging')
    #arg_parser.add_argument('--annotations',
    #    action='store_true',
    #    help='Prevent the inclusion of annotations in the data sets')

    parsed_args = arg_parser.parse_args()
    
    sys.exit(main(parsed_args))
