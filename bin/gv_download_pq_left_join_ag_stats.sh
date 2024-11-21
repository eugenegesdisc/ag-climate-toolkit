#!/usr/bin/env bash

: <<'COMMENT'
bash ${PWD}/bin/gv_download_precpitation.sh ${PWD}/src/giovanni.py ~/output IN Indiana "username" "password"
bash ${PWD}/bin/gv_download_soil_wetness.sh ${PWD}/src/giovanni.py ~/output IN Indiana "username" "password"
bash ${PWD}/bin/gv_download_temperature.sh ${PWD}/src/giovanni.py ~/output IN Indiana "username" "password"
bash ${PWD}/bin/pq_monthly_combine.sh ${PWD}/src/parquet_ops.py ~/output ~/output IN Indiana
bash ${PWD}/bin/pq_aggregate_m2y.sh ${PWD}/src/parquet_ops.py ~/output ~/output IN Indiana
bash ${PWD}/bin/pq_left_join_ag_stats.sh ${PWD}/src/parquet_ops.py ~/output ~/output IN Indiana



COMMENT

if [ $# -lt 6 ]; then
    echo "illegal number of parameters"
    echo "Usage: bash $0 <prog-root-path> <output-dir> <state-2-letter> <state-full-name> <earthdata-username> <earthdata-password> {skip-steps}"
    echo "Example: bash $0 ${PWD} ~/output ~/output IN Indiana username password"
    echo "Example: bash $0 ${PWD} ~/output ~/output IN Indiana username password 3"
    exit 2
fi
prog_path=$1
output_dir=$2
state_code=$3
state_name=$4
earthdata_username=$5
earthdata_password=$6
skip_step=$7


lower_state_code=`echo "print('${state_code}'.lower())" | python`
lower_state_name=`echo "print('${state_name}'.lower())" | python`

the_skip_step=0
if [ $# -gt 6 ] && \
    ! [[ "${skip_step}" =~ ^[#[:space:]]*$ ]]; then
    the_skip_step="${skip_step}"
fi

if [ "1" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/gv_download_precpitation.sh" "${prog_path}/src/giovanni.py" "${output_dir}" "${state_code}" "${state_name}" "${earthdata_username}" "${earthdata_password}"
fi
if [ "2" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/gv_download_soil_wetness.sh" "${prog_path}/src/giovanni.py" "${output_dir}" "${state_code}" "${state_name}" "${earthdata_username}" "${earthdata_password}"
fi
if [ "3" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/gv_download_temperature.sh" "${prog_path}/src/giovanni.py" "${output_dir}" "${state_code}" "${state_name}" "${earthdata_username}" "${earthdata_password}"
fi
if [ "4" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/pq_monthly_combine.sh" "${prog_path}/src/parquet_ops.py" "${output_dir}" "${output_dir}" "${state_code}" "${state_name}"
fi
if [ "5" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/pq_aggregate_m2y.sh" "${prog_path}/src/parquet_ops.py" "${output_dir}" "${output_dir}" "${state_code}" "${state_name}"
fi
if [ "6" -gt "${the_skip_step}" ]; then 
    bash "${prog_path}/bin/pq_left_join_ag_stats.sh" "${prog_path}/src/parquet_ops.py" "${output_dir}" "${output_dir}" "${state_code}" "${state_name}"
fi
