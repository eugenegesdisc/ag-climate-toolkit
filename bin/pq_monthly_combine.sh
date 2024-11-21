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
                "/Users/eyu2/output/monthly_combine.parquet",
                "--join-field",
                "time",
                "--join-method",
                "inner",
                "/Users/eyu2/output/monthly_precipitation.parquet",
                "/Users/eyu2/output/monthly_2m_air_temperature.parquet",
                "/Users/eyu2/output/monthly_soil_wetness.parquet"
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
    "${output_dir}/${lower_state_code}_monthly_combine.parquet" \
    --join-field \
    "time" \
    --join-method \
    "inner" \
    "${input_dir}/${lower_state_code}_monthly_precipitation.parquet" \
    "${input_dir}/${lower_state_code}_monthly_temperature.parquet" \
    "${input_dir}/${lower_state_code}_monthly_soil_wetness.parquet"

