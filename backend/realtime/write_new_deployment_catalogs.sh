#! /bin/bash --
#
# Bash script to activate the python erddap virtual environment and write the
# catalogs for all subsites with actively deployed instruments
# Also checks for the existence and validity of the following environment
# variables:
#   OOI_ERDDAP_ASYNC_HOME
#   OOI_ERDDAP_DATA_HOME
#

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - write new deployments catalogs for all OOI subsites with actively deployed instruments

SYNOPSIS
    $app [h]

DESCRIPTION
    -h
        show help message
";

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

echo -e "\n==============================================================================";
echo "$0: $(date --utc)";
. ${HOME}/.bashrc;
#env | sort
#exit 1;

# Set up path
PATH=/bin:${PATH}:$OOI_ERDDAP_ASYNC_HOME/backend/bin;

if [ -z "$UFRAME_BASE_URL" ]
then
    echo "No UFRAME_BASE_URL environment variable set" >&2;
    exit 1;
fi

# Source the python virtual env wrapper set up script
. $VIRTUALENVWRAPPER_SCRIPT;

# Activate the python working environment
workon erddap
[ "$?" -ne 0 ] && exit 1;

# Make sure $OOI_ERDDAP_ASYNC_HOME is set and valid
if [ -z "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo "OOI_ERDDAP_ASYNC_HOME not set" >&2;
    exit 1;
fi

# Define subsite deployments catalogs location
deployment_catalogs_dir="$OOI_ERDDAP_ASYNC_HOME/backend/catalogs";
if [ ! -d "$deployment_catalogs_dir" ]
then
    echo "Invalid subsite catalog directory: $deployment_catalogs_dir";
    exit 1;
fi

# Create the new subsite deployments catalogs location
new_deployments_dir="$OOI_ERDDAP_ASYNC_HOME/backend/datasets-new";
if [ ! -d "$new_deployments_dir" ]
then
    echo "Invalid new subsite deployments destination: $new_deployments_dir" >&2;
    exit 1;
fi

# Parse the subsites.csv file to get a list of cabled and uncabled subsites
subsites_csv="$OOI_ERDDAP_ASYNC_HOME/backend/config/subsites.csv";
if [ ! -f "$subsites_csv" ]
then
    echo "Invalid subsites file: $subsites_csv" >&2;
    exit 1;
fi

uncabled_subsites=$(grep ',uncabled$' $subsites_csv | awk -F, '{print $1}');
cabled_subsites=$(grep ',cabled$' $subsites_csv | awk -F, '{print $1}');

# Create uncabled sites new deployments catalogs
for subsite in $uncabled_subsites
do
    subsite_active_json="$deployment_catalogs_dir/$subsite-subsite-active-catalog.json";
    echo "subsite file: $subsite_active_json";
    [ ! -f "$subsite_active_json" ] && continue;
    
    # Create the output file name
    subsite_out_json="$new_deployments_dir/$subsite-uncabled.new.json";

    echo "Writing new deployments (uncabled): $subsite";
    # Write the new deployments file
    write_new_backend_datasets.py -o $subsite_out_json $subsite_active_json;
done

# Create cabled sites new deployments catalogs
for subsite in $cabled_subsites
do
    subsite_active_json="$deployment_catalogs_dir/$subsite-subsite-active-catalog.json";
    echo "subsite file: $subsite_active_json";
    [ ! -f "$subsite_active_json" ] && continue;
    
    # Create the output file name
    subsite_out_json="$new_deployments_dir/$subsite-cabled.new.json";

    echo "Writing new deployments (cabled): $subsite";
    # Write the new deployments file
    write_new_backend_datasets.py -o $subsite_out_json $subsite_active_json;
done
