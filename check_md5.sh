#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo Usage: $0 CANCER
    exit 1
fi

CANCER=$1

qsub -j oe -N ${CANCER}-md5-check -l walltime=24:00:00,nodes=1:ppn=20,mem=8gb <<EOF
cd \$PBS_O_WORKDIR
python -u ./check_md5.py ${CANCER}
EOF

