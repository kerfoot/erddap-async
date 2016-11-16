#! /bin/bash --
#

# Source .bashrc for virtualenv stuff
. ~/.bashrc;
PATH=${OOI_ERDDAP_ASYNC_HOME}/backend/bin:${PATH};

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
    $app - process UFrame async response files to create and/or update ERDDAP
    datasets.  STDOUT and STDERR are logged to daily datestamped *.stdout and
    *.stderr files in:

    $log_dir

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

# Stream queue write destination for each sent request
stream_queue="${OOI_ERDDAP_ASYNC_HOME}/backend/stream-queue";
if [ ! -d "$stream_queue" ]
then
#    echo "Invalid stream queue: $stream_queue";
    echo "Invalid stream queue: $stream_queue" >> $stderr.log;
    return 1;
fi

#echo "Stream queue: $stream_queue";
echo "Stream queue: $stream_queue" >> $stdout_log;

json_files=$(find $stream_queue -type f -name '*response.json');
if [ -z "$json_files" ]
then
#    echo "No UFrame response files found";
    echo "No UFrame response files found" >> $stdout_log;
    return 0;
fi

# Process each response file separately
for f in $json_files
do

    echo "Processing request: $f" >> $stdout_log;

    # Create/update the ERDDAP dataset for this response, delete the response
    # file on success and logSTDOUT and STDERR
    async_request_to_erddap.py --delete_on_success \
        $f \
        >> $stdout_log \
        2>> $stderr_log;

done

# Update the uncabled and cabled erddap datasets.xml files
create_uncabled_backend_catalog.sh >> $stdout_log 2>> $stderr_log;
create_cabled_backend_catalog.sh >> $stdout_log 2>> $stderr_log;

deactivate

