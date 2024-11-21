#!/usr/bin/env bash

: <<'COMMENT'
        {
            "name": "Python Debugger: giovanni.py",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/src/parquet_ops.py",
            "args": [
                "join",
                "--output",
                "/Users/eyu2/output/in_year_join.parquet",
                "--join-field",
                "year",
                "--join-method",
                "left",
                "/Users/eyu2/output/year_combine.parquet",
                "/Users/eyu2/output/state_yield.parquet"
            ]
        }
COMMENT

if [ $# -lt 5 ]; then
    echo "illegal number of parameters"
    echo "Usage: bash $0 <python-prog-path> <input-dir> <output-dir> <state-2-letter> <state-full-name>"
    echo "Example: bash $0 ../src/parquet_ops.py /Users/eyu2/output /Users/eyu2/output IN Indiana"
    exit 2
fi
py_src=$1
input_dir=$2
output_dir=$3
state_code=$4
state_name=$5


lower_state_code=`echo "print('${state_code}'.lower())" | python`
lower_state_name=`echo "print('${state_name}'.lower())" | python`

echo "py_src=${py_src}"

python "${py_src}" \
    "join" \
    --output \
    "${output_dir}/${lower_state_code}_climate_ag_stats.parquet" \
    --join-field \
    "year" \
    --join-method \
    "left" \
    "${input_dir}/${lower_state_code}_year_combine.parquet" \
    "${input_dir}/survey_${lower_state_code}_*.parquet"

