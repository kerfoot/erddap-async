#! /bin/bash --
#

PATH=${PATH}:/bin;

app=$(basename $0);

if [ ! -d "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo '$OOI_ERDDAP_ASYNC_HOME not set' >&2;
    exit 1;
fi

data_home=$OOI_ERDDAP_DATA_HOME;
if [ ! -d "$data_home" ]
then
    echo '$OOI_ERDDAP_DATA_HOME not set' >&2;
    return 1;
fi
# Location of the individual stream.xml files
stream_xml_dir="${OOI_ERDDAP_DATA_HOME}/erddap-11-1/stream-xml";

# Location of ERDDAP datasets.xml file
datasets_xml_file='/var/local/erddap/content/content-11-1/erddap/datasets.xml';

# Usage message
USAGE="
NAME
    $app - Write the ERDDAP datasets.xml file containing individual OOI stream datasets
    to the backend cabled ERDDAP server

SYNOPSIS
    $app [h]

DESCRIPTION
    Searches:

        $stream_xml_dir

    for all dataset.xml files and creates a new ERDDAP datasets.xml file for
        serving the individual datasets.

    The resulting datasets.xml file is written to:

        $datasets_xml_file

    -h
        show help message
    
    -x
        print the names of the xml files to be added, but do not create the
        datasets.xml file
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

if [ ! -d "$stream_xml_dir" ]
then
    echo "Invalid stream XML directory: $stream_xml_dir" >&2;
    exit 1;
fi

# Files of interest
erddap_head_xml="${OOI_ERDDAP_ASYNC_HOME}/frontend/templating/masters/erddap-datasets.head.xml";
erddap_tail_xml="${OOI_ERDDAP_ASYNC_HOME}/frontend/templating/masters/erddap-datasets.tail.xml";
stream_xml_files=$(find $stream_xml_dir -maxdepth 1 -type f -name '*dataset.xml' | sort);

if [ ! -f "$datasets_xml_file" ]
then
    echo "Invalid ERDDAP datasets.xml file: $datasets_xml_file" >&2;
    exit 1;
elif [ ! -f "$erddap_head_xml" ]
then
    echo "Missing erddap head file: $erddap_head_xml" >&2;
    exit 1;
elif [ ! -f "$erddap_tail_xml" ]
then
    echo "Missing erddap tail file: $erddap_tail_xml" >&2;
    exit 1;
elif [ -z "$stream_xml_files" ]
then
    echo "No stream xml files found: $stream_xml_dir" >&2;
    exit 1;
fi

# Create the datasets.xml file
tmp_datasets_xml_file='/var/local/erddap/content/content-11-1/erddap/datasets.xml.tmp';
if [ -z "$debug" ]
then
    cat $erddap_head_xml > $tmp_datasets_xml_file;
else
    echo '==> DEBUG MODE <==';
    echo "datasets.xml: $datasets_xml_file";
    echo "Header: $erddap_head_xml";
fi

if [ "$?" -ne 0 ]
then
    rm $tmp_datasets_xml_file;
    return 1;
fi
# cat each $xml file in $stream_xml_files to the temporary datasets xml file
for xml_file in $stream_xml_files
do
    if [ -n "$debug" ]
    then
        echo "dataset XML file: $xml_file";
    else    
	    echo "Adding $xml_file";
	    cat $xml_file >> $tmp_datasets_xml_file;
		if [ "$?" -ne 0 ]
		then
	        echo "Removing temporary datasets.xml file: $tmp_datasets_xml_file" >&2;
		    rm $tmp_datasets_xml_file;
		    return 1;
		fi
    fi
done
if [ -z "$debug" ]
then
    cat $erddap_tail_xml >> $tmp_datasets_xml_file;
else
    echo "Tail  : $erddap_tail_xml";
fi
if [ "$?" -ne 0 ]
then
    rm $tmp_datasets_xml_file;
    return 1;
fi

# Move the temporary file to the datasets.xml file
if [ -z "$debug" ]
then
	echo "Writing: $datasets_xml_file...";
	mv $tmp_datasets_xml_file $datasets_xml_file;
fi

exit $?;

