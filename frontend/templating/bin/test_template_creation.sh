#! /bin/bash --
#
# USAGE:
#
# ============================================================================
# $RCSfile$
# $Source$
# $Revision$
# $Date$
# $Author$
# $Name$
# ============================================================================
#

PATH=${PATH}:/bin;

app=$(basename $0);

# Usage message
USAGE="
NAME
    $app - 

SYNOPSIS
    $app [h]

DESCRIPTION
    -h
        show help message
";

# Default values for options
write_dir=$(pwd);

# Process options
while getopts "hd:" option
do
    case "$option" in
        "h")
            echo -e "$USAGE";
            exit 0;
            ;;
        "d")
            write_dir=$OPTARG;
            ;;
        "?")
            echo -e "$USAGE" >&2;
            exit 1;
            ;;
    esac
done

if [ ! -d "$write_dir" ]
then
    echo "Invalid xml write location: $write_dir" >&2;
fi

# Remove option from $@
shift $((OPTIND-1));

if [ -z "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo '$OOI_ERDDAP_ASYNC_HOME is not set' >&2;
    exit 1;
elif [ ! -d "$OOI_ERDDAP_ASYNC_HOME" ]
then
    echo "Invalid \$OOI_ERDDAP_ASYNC_HOME directory: $OOI_ERDDAP_ASYNC_HOME" >&2;
    return 1;
fi

templates_dir="${OOI_ERDDAP_ASYNC_HOME}/frontend/templating/xml/production";
if [ ! -d "$templates_dir" ]
then
    echo "Invalid templates production directory: $tempaltes_dir" >&2;
    return 1;
fi

if [ "$#" -eq 0 ]
then
    echo 'No NetCDF files specified' >&2;
    return 1;
fi

for nc in "$@"
do
    if [ ! -f "$nc" ]
    then
        echo "Invalid file specified: $nc" >&2;
        continue
    fi

    template_name=$(basename $nc .nc);

    template_file="${templates_dir}/${template_name}.template.dataset.xml";
    if [ ! -f "$template_file" ]
    then
        echo "Template does not exist: $template_file";
        continue;
    fi

    nc_path=$(dirname $nc);
    output_xml_file="${write_dir}/${template_name}.dataset.xml";

    echo "Writing: $output_xml_file";

    sed "s|{dataset_id}|${template_name}|" $template_file | \
        sed "s|{file_dir}|${nc_path}|" | \
        sed "s|{summary}|$template_name|" | \
        sed "s|{title}|$template_name|" > $output_xml_file;

done
