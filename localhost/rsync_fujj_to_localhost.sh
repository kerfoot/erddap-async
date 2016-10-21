#! /bin/bash
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

wget -r \
    -N \
    --no-parent \
    -R index.* \
    --no-check-certificate \
    -nH \
    --cut-dirs=2 \
    -P ~/datasets/ooi/uframe/async_results/fujj \
    https://opendap-test.oceanobservatories.org/async_results/fujj/
