#! /bin/bash --
#
# Bash script to activate the python erddap virtual environment and write the
# catalogs for all subsites with actively deployed instruments

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

# Define catalog destination
catalog_destination=$OOI_ERDDAP_ASYNC_HOME/backend/catalog;
if [ ! -d "$catalog_destination" ]
then
    echo "Invalid subsite catalog destination: $catalog_destination";
    exit 1;
fi

# Delete all existing *active-catalog.json catalog files
echo "Deleting all previous subsite catalog files: $catalog_destination";
rm $catalog_destination/*active-catalog.json 2>/dev/null;

# Write the deployed instrument catalogs for each subsite
write_active_subsite_deployments_catalogs.py -d $catalog_destination;
