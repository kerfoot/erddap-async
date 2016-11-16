#! /bin/bash --
#

# Source .bashrc for virtualenv stuff
. ~/.bashrc;
PATH=${OOI_ERDDAP_ASYNC_HOME}/backend/bin:${PATH};
# Set the OOI_ERDDAP_USER to the name we want to use for UFrame requests.  We
# must export it since a new shell is spawned to run the python stuff
export OOI_ERDDAP_USER=erddap_user;

app=$(basename $0);

# Set up STDOUT and STDERR logs
log_dir=${OOI_ERDDAP_ASYNC_HOME}/backend/logs;
if [ ! -d "$log_dir" ]
then
    mkdir -m 755 $log_dir;
    [ "$?" -ne 0 ] && exit 1;
fi
# Date stamped stdout and stderr log names
stdout_log="${log_dir}/${app}-$(date --utc +%Y%m%d).stdout";
stderr_log="${log_dir}/${app}-$(date --utc +%Y%m%d).stderr";

# Usage message
USAGE="
NAME
    $app - examine all OOI subsite instrument deployment catalogs and write a
    JSON catalog for newly active streams.  A stream is deemed new if it does
    not exist as a backend ERDDAP datdaset.  STDOUT and STDERR are logged to 
    daily datestamped *.stdout and *.stderr files in:

    $log_dir

    Existing new deployment catalogs are clobbered.

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

# Activate the python virtual environment
workon erddap

echo '==============================================================================' >> $stdout_log;
echo "Executing $app @ $(date --utc)" >> $stdout_log;
echo '==============================================================================' >> $stderr_log;
echo "Executing $app @ $(date --utc)" >> $stderr_log;

# Location of the new instrument deployment catalogs
new_deployments_dir="${OOI_ERDDAP_ASYNC_HOME}/backend/datasets-new";
if [ ! -d "$new_deployments_dir" ]
then
#    echo "Invalid new instrument deployments directory: $new_deployments_dir";
    echo "Invalid new instrument deployments directory: $new_deployments_dir" >> $stderr.log;
    return 1;
fi
# Stream queue write destination for each sent request
stream_queue="${OOI_ERDDAP_ASYNC_HOME}/backend/stream-queue";
if [ ! -d "$stream_queue" ]
then
#    echo "Invalid stream queue destination: $stream_queue";
    echo "Invalid stream queue destination: $stream_queue" >> $stderr.log;
    return 1;
fi

#echo "New deployments directory: $new_deployments_dir";
#echo "Stream queue destination : $stream_queue";

echo "New deployments directory: $new_deployments_dir" >> $stdout_log;
echo "Stream queue destination : $stream_queue" >> $stdout_log;

json_files=$(find $new_deployments_dir -type f -name '*new.json');
if [ -z "$json_files" ]
then
#    echo "No new deployment JSON files found";
    echo "No new deployment JSON files found" >> $stdout_log;
    return 0;
fi

for f in $json_files
do

    echo "Sending requests: $f" >> $stdout_log;

    # Send the requests logging STDOUT and STDERR
    send_deployment_requests.py -d $stream_queue \
        $f \
        >> $stdout_log \
        2>> $stderr_log;

    [ "$?" -ne 0 ] && continue;

    # Delete the input file
    echo "Deleting new deployments file: $f" >> $stdout_log;
    rm $f;

done

deactivate

