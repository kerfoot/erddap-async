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

# Location of the subsite deployed instrument catalogs
catalog_destination=${OOI_ERDDAP_ASYNC_HOME}/backend/catalogs;
# Write destination for the new instrument deployment subsite catalogs
new_deployments_destination=${OOI_ERDDAP_ASYNC_HOME}/backend/datasets-new;

# Find the deployment catalog files
deployment_catalogs=$(find $catalog_destination -name '*subsite-active-catalog.json');
if [ -z "$deployment_catalogs" ]
then
    echo "No subsite deployment catalogs found: $catalog_destination" >> $stdout_log;
    return 1;
fi

for catalog in $deployment_catalogs
do
	write_new_backend_datasets.py --clobber \
        -d $new_deployments_destination $catalog \
	    >> $stdout_log \
	    2>> $stderr_log;
done

deactivate

