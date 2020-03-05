#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo Usage: $0 CANCER
    exit 1
fi

CANCER=$1

qsub -j oe -N ${CANCER} -l walltime=300:00:00,nodes=1:ppn=2,mem=2gb <<EOF
cd \$PBS_O_WORKDIR
python -u ./batch_download.py --num-jobs 50 --output-dir /stornext/HPCScratch/PapenfussLab/projects/gdc_download/${CANCER}/ --gdc-project-id TCGA-${CANCER} --save-query-file ${CANCER}-query.pkl --cancer ${CANCER} --run-anyway
EOF

#python -u ./batch_download.py --num-jobs 100 --output-dir /stornext/HPCScratch/PapenfussLab/projects/gdc_download/${CANCER}/ --gdc-project-id TCGA-${CANCER} --save-query-file ${CANCER}-query.pkl --cancer ${CANCER} --run-anyway --whitelist ${CANCER}-whitelist.txt

