#! /bin/bash
#

PATH=${PATH}:/bin;

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - print a list of available frontend UFrame stream templates used to
    serve ERDDAP datasets

SYNOPSIS
    $app [h]

DESCRIPTION
    Searches for the XML templates located in the default template destination
    and prints the stream name of the template.  The default template
    destination is built from \$OOI_ERDDAP_ASYNC_HOME and is:

    $OOI_ERDDAP_ASYNC_HOME/frontend/templating/xml/production 

    -h
        show help message

    -d
        specify an alternate template destination to search
";

# Default values for options

# Process options
while getopts "hd:f" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "d")
            templates_dir=$OPTARG;
            ;;
        "f")
            list_files=1;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

# Remove option from $@
shift $((OPTIND-1));

# Use $OOI_ERDDAP_TEMPLATE_ROOT to figure out where the XML templates are
# located if not specified via -d option
if [ -z "$templates_dir" ]
then
	templates_dir=$OOI_ERDDAP_ASYNC_HOME/frontend/templating/xml/production;
fi

if [ -z "$templates_dir" ]
then
    echo "No template directory specified and default location is invalid: $templates_dir" >&2;
    exit 1;
fi

xml_templates=$(find $templates_dir -type f -name '*dataset.xml' -exec basename '{}' \; 2>/dev/null | sort);
if [ -n "$list_files" ]
then
    xml_templates=$(find $templates_dir -type f -name '*dataset.xml' 2>/dev/null | sort);
fi

for xml_template in $xml_templates
do
    if [ -n "$list_files" ]
    then
        echo $xml_template;
    else
        echo $xml_template | awk -F- '{print $1}';
    fi
done
