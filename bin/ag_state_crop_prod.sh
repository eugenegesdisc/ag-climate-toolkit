#!/usr/bin/env bash

if [ $# -lt 6 ]; then
    echo "illegal number of parameters"
    echo "Usage: bash $0 <python-src-dir> <api-key> <output-dir> <state-2-letter> <state-full-name> <crop> {start_year} {operation} {parameter-to-desc} {-force}"
    echo "Example: bash $0 ../src/agstates.py your-usda-api-key /Users/eyu2/output IN indiana corn"
    echo "Example: bash $0 ../src/agstates.py your-usda-api-key /Users/eyu2/output IN indiana corn 2000"
    echo "Example: bash $0 ../src/agstates.py your-usda-api-key /Users/eyu2/output IN indiana corn 2000 count"
    echo "Example: bash $0 ../src/agstates.py your-usda-api-key /Users/eyu2/output IN indiana corn 2000 parameter_desc unit_desc"
    echo "Example: bash $0 ../src/agstates.py your-usda-api-key /Users/eyu2/output IN indiana corn 2000 parameter_desc unit_desc -force"
    exit 2
fi
py_src=$1
api_key=$2
output_dir=$3
state_code=$4
state_name=$5
crop=$6
start_year=$7
operation=$8
parameter=$9
force=$10

lower_state_code=`echo "print('${state_code}'.lower())" | python`
lower_state_name=`echo "print('${state_name}'.lower())" | python`
lower_crop=`echo "print('${crop}'.replace(' ','_').lower())" | python`
upper_crop=`echo "print('${crop}'.upper())" | python`

SOURCE=${BASH_SOURCE[0]}
while [ -L "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
  SOURCE=$(readlink "$SOURCE")
  [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )

#api-key
if [ $# -gt 1 ] && \
    ! [[ "${api_key}" =~ ^[#[:space:]]*$ ]]; then
    api_key="${api_key}"
else
    source "${DIR}/../bash_config.config"
    api_key="${USDA_QUICKSTATS_API_KEY}"
fi

#optional arguments - skip with # or empty string (only contains whitespaces and #)
start_year_cond=""
if [ $# -gt 6 ] && \
    ! [[ "${start_year}" =~ ^[#[:space:]]*$ ]]; then
    start_year_cond="year__GE;;${start_year}"
fi

the_operation="data"
if [ $# -gt 7 ] && \
    ! [[ "${operation}" =~ ^[#[:space:]]*$ ]]; then
    the_operation="${operation}"
fi

the_parameter="unit_desc"
if [ $# -gt 8 ] && \
    ! [[ "${parameter}" =~ ^[#[:space:]]*$ ]]; then
    the_parameter="${parameter}"
fi

the_force=""
if [ $# -gt 9 ] && \
    ! [[ "${force}" =~ ^[#[:space:]]*$ ]]; then
    the_force="${force}"
fi

the_unit_desc_con="unit_desc;;BU"
# the_unit_desc="BU / ACRE"
#if [ "${upper_crop}" = "CORN" ] || \
#    [ "${upper_crop}" = "SOYBEANS" ] || \
#    [ "${upper_crop}" = "OATS" ] || \
#    [ "${upper_crop}" = "RYE" ] || \
#    [ "${upper_crop}" = "SORGHUM" ] || \
#    [ "${upper_crop}" = "BARLEY" ] || \
#    [ "${upper_crop}" = "FLAXSEED" ] || \
#    [ "${upper_crop}" = "WHEAT" ]; then
#    the_unit_desc_con="unit_desc;;BU / ACRE"
#el
if [ "${upper_crop}" = "PEANUTS" ] || \
    [ "${upper_crop}" = "SUNFLOWER" ] || \
    [ "${upper_crop}" = "SAFFLOWER" ] || \
    [ "${upper_crop}" = "TOBACCO" ] || \
    [ "${upper_crop}" = "HOPS" ] || \
    [ "${upper_crop}" = "CANOLA" ]; then
    the_unit_desc_con="unit_desc;;LB"
elif [ "${upper_crop}" = "RICE" ] || \
    [ "${upper_crop}" = "SAFFLOWER" ] || \
    [ "${upper_crop}" = "CHICKPEAS" ] || \
    [ "${upper_crop}" = "LENTILS" ]; then
    the_unit_desc_con="unit_desc;;CWT"
elif [ "${upper_crop}" = "COTTON" ]; then
    the_unit_desc_con="unit_desc;;480 LB BALES"
elif [ "${upper_crop}" = "HAY & HAYLAGE" ]; then
    the_unit_desc_con="unit_desc;;TONS, DRY BASIS"
elif [ "${upper_crop}" = "MAPLE SYRUP" ]; then
    the_unit_desc_con="unit_desc;;GALLONS"
elif [ "${upper_crop}" = "HAY" ] || \
    [ "${upper_crop}" = "SUGARBEETS" ] || \
    [ "${upper_crop}" = "SUGARCANE" ]; then
    the_unit_desc_con="unit_desc;;TONS"
fi

# force empty the unit_desc
#if [ "${parameter}" = "unit_desc" ]; then
if [[ "${the_force}" == "-force" ]]; then
    the_unit_desc_con=""
    the_statisiccat_desc_cond=""
fi

the_crop_desc_cond=""
if ! [[ "${crop}" =~ ^[#[:space:]]*$ ]]; then
    the_crop_desc_cond="commodity_desc;;${upper_crop}"
fi


the_util_practice_desc_con=""
if [ "${upper_crop}" = "SUGARCANE" ]; then
    the_util_practice_desc_con="util_practice_desc;;SUGAR"
fi

echo "upper_crop=${upper_crop}"
echo "survey-${crop}-prod-${lower_state_name}"
echo "the_unit_desc_con=${the_unit_desc_con}"

python "${py_src}" "nass_quickstats" \
    --operation ${the_operation} \
    --api-key "${api_key}" \
    --parameter "${the_parameter}" \
    --output "${output_dir}/survey_${lower_state_code}_${lower_crop}_prod.parquet" \
    --output-columns "year;Value" \
    --output-column-names "year;${lower_crop}_prod" \
    --search-conditions \
                "source_desc;;SURVEY" \
                "sector_desc;;CROPS" \
                "agg_level_desc;;STATE" \
                "${the_crop_desc_cond}" \
                "group_desc;;FIELD CROPS" \
                "class_desc;;ALL CLASSES" \
                "reference_period_desc;;YEAR" \
                "statisticcat_desc;;PRODUCTION" \
                "prodn_practice_desc;;ALL PRODUCTION PRACTICES" \
                "${the_util_practice_desc_con}" \
                "${the_unit_desc_con}" \
                "${start_year_cond}" \
                "freq_desc;;ANNUAL" \
                "state_alpha;;${state_code}"
