#!/usr/bin/env bash

: <<'COMMENT'
        {
            "name": "Python Debugger: parquet_ops.py",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/src/parquet_ops.py",
            "args": [
                "aggregate",
                "--input",
                "/Users/eyu2/output/monthly_combine.parquet",
                "--output",
                "/Users/eyu2/output/year_combine.parquet",
                "--time-string-field",
                "time",
                "--time-aggregate-level",
                "year",
                "--aggregate-method",
                "mean"
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
    "aggregate" \
    --input \
    "${input_dir}/${lower_state_code}_monthly_combine.parquet" \
    --output \
    "${output_dir}/${lower_state_code}_year_combine.parquet" \
    --time-string-field \
    "time" \
    --time-aggregate-level \
    "year" \
    --aggregate-method \
    "mean"

