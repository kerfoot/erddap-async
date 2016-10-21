#! /bin/bash --
#
# Bash script to activate the python erddap virtual environment and process
# all UFrame responses found in the stream-queue.
# Also checks for the existence and validity of the following environment
# variables:
#   OOI_ERDDAP_ASYNC_HOME
#   OOI_ERDDAP_DATA_HOME
#

echo -e "\n==============================================================================";
echo "$0: $(date --utc)";
. ${HOME}/.bashrc;
#env | sort
#exit 1;

# Set up path
PATH=${PATH}:/bin:$OOI_ERDDAP_ASYNC_HOME/backend/bin;

if [ -z "$UFRAME_BASE_URL" ]
then
    echo "No UFRAME_BASE_URL environment variable set" >&2;
    exit 1;
fi

# Source the python virtual env wrapper set up script
. $VIRTUALENVWRAPPER_SCRIPT;

workon erddap
[ "$?" -ne 0 ] && exit 1;

# Make sure $OOI_ERDDAP_ASYNC_HOME is set and valid
if [ -z "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo "OOI_ERDDAP_ASYNC_HOME not set" >&2;
    exit 1;
elif [ ! -d "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo "Invalid OOI_ERDDAP_ASYNC_HOME location: $OOI_ERDDAP_ASYNC_HOME" >&2;
    exit 1;
fi

# Make sure $OOI_ERDDAP_DATA_HOME is set and valid
if [ -z "$OOI_ERDDAP_DATA_HOME" ]
then
    echo "OOI_ERDDAP_DATA_HOME not set" >&2;
    exit 1;
elif [ ! -d "$OOI_ERDDAP_DATA_HOME" ]
then
    echo "Invalid OOI_ERDDAP_DATA_HOME location: $OOI_ERDDAP_DATA_HOME" >&2;
    exit 1;
fi

# Create the stream queue location
stream_queue="${OOI_ERDDAP_ASYNC_HOME}/backend/stream-queue";
if [ ! -d "$stream_queue" ]
then
    echo "Invalid stream-queue directory: $stream_queue" >&2;
    exit 1;
fi

# See if we have any response files to process
responses=$(find $stream_queue -name '*response.json' 2>/dev/null);
if [ -z "$responses" ]
then
    echo "No responses found: $stream_queue";
    exit 1;
fi

# Process each response file separately
for response in $responses
do
    echo "Checking response file: $response";
    # Process the response file and delete it ONLY if there were no issues
    async_request_to_erddap.py --delete_on_success $response;
done
