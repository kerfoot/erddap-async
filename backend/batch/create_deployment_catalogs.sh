#! /bin/bash
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
    $app - create the instrument deployment catalogs for each OOI subsite.
    STDOUT and STDERR are logged to daily datestamped *.stdout and *.stderr files in:

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

# Write destination for the subsite catalogs
catalog_destination=${OOI_ERDDAP_ASYNC_HOME}/backend/catalogs;

# Write the active instrument subsite deployment catalogs and clobber existing
# catalogs
write_active_subsite_deployments_catalogs.py --clobber \
    -d $catalog_destination \
    >> $stdout_log \
    2>> $stderr_log;

deactivate

