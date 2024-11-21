#!/usr/bin/env bash

: <<'COMMENT'
        {
            "name": "Python Debugger: giovanni.py",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/src/giovanni.py",
            "args": [
                "--earthdata-login-name", "username",
                "--earthdata-login-pass", "password",
                "--plot-type", "Time Series, Area-Averaged",
                "--plot-start-date", "January 1, 2001",
                "--plot-end-date", "2023-12-31",
                "--plot-area-bbox", "-125,20,-60, 50",
                "--plot-area-shape","US States Indiana",
                "--plot-variable","total surface precipitation precipitation M2TMNXFLX",
                "--rename-column","precipitation",
                "--rename-column-index","1",
                "--save-to-csv-file","~/output/monthly_precipitation.csv",
                "--save-to-csv-file-metadata",
                "--csv-skip-signature","time, mean_",
                "--save-to-parquet-file","~/output/monthly_precipitation.parquet"
            ]
        }
COMMENT

if [ $# -lt 5 ]; then
    echo "illegal number of parameters"
    echo "Usage: bash $0 <python-prog-path> <output-dir> <state-2-letter> <state-full-name> <login-name> <login-password>"
    echo "Example: bash $0 ../src/giovanni.py /Users/eyu2/output IN Indiana \"yourusername\" \"yourpassword\""
    echo "Example: bash $0 ../src/giovanni.py /Users/eyu2/output IN Indiana \"corn;soybeans\" 2000"
    echo "Example: bash $0 ../src/giovanni.py /Users/eyu2/output IN Indiana \"corn;soybeans\" 2000 count"
    echo "Example: bash $0 ../src/giovanni.py /Users/eyu2/output IN Indiana \"corn;soybeans\" 2000 parameter_desc unit_desc"
    exit 2
fi
py_srcdir=$1
output_dir=$2
state_code=$3
state_name=$4
earthdata_username=$5
earthdata_password=$6


lower_state_code=`echo "print('${state_code}'.lower())" | python`
lower_state_name=`echo "print('${state_name}'.lower())" | python`

python "${py_src}" \
    --earthdata-login-name "${earthdata_username}" \
    --earthdata-login-pass "${earthdata_password}" \
    --plot-type "Time Series, Area-Averaged" \
    --plot-start-date "January 1, 2001" \
    --plot-end-date "2023-12-31" \
    "--plot-area-bbox", "-125,20,-60,50" \
    --plot-area-shape "US States ${state_name}" \
    --plot-variable "total surface precipitation precipitation M2TMNXFLX" \
    --rename-column "precipitation" \
    --rename-column-index "1" \
    --save-to-csv-file "${output_dir}/${lower_state_code}_monthly_precipitation.csv" \
    --save-to-csv-file-metadata \
    --csv-skip-signature "time, mean_" \
    --save-to-parquet-file "${output_dir}/${lower_state_code}_monthly_precipitation.parquet"

