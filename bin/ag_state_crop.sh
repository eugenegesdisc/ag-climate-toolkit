#!/usr/bin/env bash

if [ $# -lt 6 ]; then
    echo "illegal number of parameters"
    echo "Usage: bash $0 <api-key> <prog-root-dir> <output-dir> <state-2-letter> <state-full-name> <crops> {start_year} {operation} {parameter-to-desc}"
    echo "Example: bash $0 your-usda-api-key ../ /Users/eyu2/output IN indiana \"corn;soybeans\""
    echo "Example: bash $0 your-usda-api-key ../ /Users/eyu2/output IN indiana \"corn;soybeans\" 2000"
    echo "Example: bash $0 your-usda-api-key ../ /Users/eyu2/output IN indiana \"corn;soybeans\" 2000 count"
    echo "Example: bash $0 your-usda-api-key ../ /Users/eyu2/output IN indiana \"corn;soybeans\" 2000 parameter_desc unit_desc"
    exit 2
fi
api_key=$1
py_srcdir=$2
output_dir=$3
state_code=$4
state_name=$5
crops=$6
start_year=$7
operation=$8
parameter=$9



t_array=`echo  "print('(\"'+'\" \"'.join(\"${crops}\".split(\";\"))+'\")')" | python`
#crops_array=`echo  "for item in \"corn;soybeans\".split(\";\"): print(item) " | python`

echo "$t_array"
eval "crops_array=${t_array}"
for i in ${!crops_array[@]}; do
    echo element $i is "${crops_array[$i]}"
    bash "${py_srcdir}bin/ag_state_crop_dollar.sh" "${py_srcdir}src/agstats.py" "${api_key}" "${output_dir}" "${state_code}" "${state_name}" "${crops_array[$i]}" "${start_year}" "${operation}" "${parameter}"
    if ! [ "${operation}" = "data" ] && \
       ! [[ "${operation}" =~ ^[#[:space:]]*$ ]]; then
        continue
    fi
    bash "${py_srcdir}bin/ag_state_crop_yield.sh" "${py_srcdir}src/agstats.py"  "${api_key}" "${output_dir}" "${state_code}" "${state_name}" "${crops_array[$i]}" "${start_year}" "${operation}" "${parameter}"
    bash "${py_srcdir}bin/ag_state_crop_prod.sh" "${py_srcdir}src/agstats.py"  "${api_key}" "${output_dir}" "${state_code}" "${state_name}" "${crops_array[$i]}" "${start_year}" "${operation}" "${parameter}"
    bash "${py_srcdir}bin/ag_state_crop_acres.sh" "${py_srcdir}src/agstats.py"  "${api_key}" "${output_dir}" "${state_code}" "${state_name}" "${crops_array[$i]}" "${start_year}" "${operation}" "${parameter}"

done
