#! /bin/bash --
#

PATH=${PATH}:/bin;

app=$(basename $0);

# Location of ERDDAP's GenerateDatasetsXml.sh
xml_builder=/var/local/erddap/tomcat-11-2/webapps/erddap/WEB-INF/GenerateDatasetsXml.sh;

# Usage message
USAGE="
NAME
    $app - create the ERDDAP dataset xml template

SYNOPSIS
    $app [h] dir1[ dir2 ...]

DESCRIPTION
    Attempts to create the ERDDAP dataset xml template by examining each
    specified directory for a NetCDF, .args and .xml file.  The template(s) 
    are created using ERDDAP's GenerateDatasetsXml.sh, located:

    $xml_builder

    -h
        show help message
    -x
        print configuration information
";

# Default values for options

# Process options
while getopts "hx" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "x")
            debug=1;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

# Validate environment
if [ -z "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo '$OOI_ERDDAP_ASYNC_HOME is not set' >&2;
    exit 1;
elif [ ! -d "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo "Invalid \$OOI_ERDDAP_ASYNC_HOME: $OOI_ERDDAP_ASYNC_HOME" >&2;
    exit 1;
elif [ ! -f "$xml_builder" ]
then
    echo "Invalid generateDatasetsXml.sh: $xml_builder" >&2;
    exit 1;
fi

# Name of generateDatasetsXml.sh
xml_app=$(basename $xml_builder);
xml_app_dir=$(dirname $xml_builder);

# Set up directories
log_dest="${OOI_ERDDAP_ASYNC_HOME}/frontend/templating/xml/generateDatasetsXml-logs";
xml_dest="${OOI_ERDDAP_ASYNC_HOME}/frontend/templating/xml/alpha";
# Validate existence of directories
if [ ! -d "$log_dest" ]
then
    echo "Invalid logs destination: $log_dest";
    exit 1;
elif [ ! -d "$xml_dest" ]
then
    echo "Invalid XML destination : $xml_dest";
    exit 1;
fi

if [ -n "$debug" ]
then
    echo "Logs destination: $log_dest";
    echo "XML destination : $xml_dest";
    echo "XML builder     : $xml_builder";
fi

# one or more directories must be specified
if [ "$#" -eq 0 ]
then
    echo "No NetCDF directories specified" >&2;
    exit 1;
fi

# All paths in the $xml_builder shell script are relative, so we need to 
# change to the directory containing $xml_builder before we can run it
cd $xml_app_dir;

# Process each specified directory
for d in "$@"
do

    if [ ! -d "$d" ]
    then
        echo "Invalid directory specified: $d" >&2;
        continue
    fi

    echo "Searching: $d";

    # Flag to specify whether all necessary files are present
    status=0;

    # Directory name should be a stream-telemetry type
    stream=$(basename $d);
    # There should be a NetCDF file named stream-telemtetry.nc inside $d
    nc_file="${d}/${stream}.nc";
    [ -n "$debug" ] && echo "Stream-telemetry NetCDF   : $nc_file";
    # See if the file exists
    if [ ! -f "$nc_file" ]
    then
        echo "Missing stream-telemetry NetCDF: $nc_file";
        status=1;
    fi
    # There should be a stream-telemetry.args file
    args_file="${d}/${stream}.args";
    [ -n "$debug" ] && echo "Stream-telemetry args file: $args_file";
    if [ ! -f "$args_file" ]
    then
        echo "Missing stream-telemetry args file: $args_file";
        status=1;
    fi
    # There should be a stream-telemetry-dataset.xml file
    xml_template="${d}/${stream}.template.xml";
    [ -n "$debug" ] && echo "Stream-telemetry xml file : $xml_template";
    if [ ! -f "$xml_template" ]
    then
        echo "Missing stream-telemetry xml file: $xml_template";
        status=1;
    fi

    if [ "$status" -eq 1 ]
    then
        echo "Skipping directory: $d";
        continue;
    fi

    echo "Stream looks good: $stream";

    [ -n "$debug" ] && continue;

    # Create the log files for writing STDOUT and STDERR from
    # $xml_builder
    stream_stdout="${log_dest}/${stream}-generateDatasetsXml.sh.stdout";
    stream_stderr="${log_dest}/${stream}-generateDatasetsXml.sh.stderr";

    echo "Creating XML template: $stream";
    # Execute GenerateDatasetsXml.sh with the appropriate args
    $xml_builder \
        -i${xml_template}#dataset \
        $(cat $args_file) > $stream_stdout 2> $stream_stderr;

    if [ "$?" -ne 0 ]
    then
        echo "Error creating template: see $stream_stdout" >&2;
        echo "Error creating template: see $stream_stderr" >&2;
        continue
    fi

    echo "Copying XML template to: $xml_dest";
    cp $xml_template $xml_dest;
    if [ "$?" -ne 0 ]
    then
        echo "Unknown error creating XML template" >&2;
        continue;
    fi

    echo "XML template created: $xml_dest/$(basename $xml_template)";

done
