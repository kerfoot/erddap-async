#! /bin/bash --
#
# USAGE:
#
# ============================================================================
# $RCSfile$
# $Source$
# $Revision$
# $Date$
# $Author$
# $Name$
# ============================================================================
#

PATH=${PATH}:/bin:/var/local/erddap/erddap-storage/bin/uframe-api;

app=$(basename $0);

streams_csv='/var/local/erddap/erddap-storage/bin/erddap-async/backend/config/stream-list.csv';

# Usage message
USAGE="
NAME
    $app - 

SYNOPSIS
    $app [h]

DESCRIPTION
    -h
        show help message
";

# Default values for options

# Process options
while getopts "h" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

count=0;
while read row
do

    stream=$(echo $row | awk -F, '{print $2}');
    [ -z "$stream" ] && continue;

    stream_type=$(echo $row | awk -F, '{print $4}');
    [ -z "$stream_type" ] && continue;

    [ "$stream_type" != 'Science' ] && continue;

    # Get the list of instrument that produce the stream, but keep only the
    # first one
    instrument=$(search_streams.py $stream | head -1);
    if [ -z "$instrument" ]
    then
        echo "$stream: No instruments found" >&2;
        continue
    fi

    count=$(( count + 1 ));
    
    # Build the NetCDF request
    request_url=$(build_instrument_requests.py --user erddap_templater \
        --no_provenance \
        --time_delta_type days \
        --time_delta_value 1 \
        --stream $stream \
        $instrument);

    [ -z "$request_url" ] && continue;

    echo "$request_url";

done < $streams_csv;
