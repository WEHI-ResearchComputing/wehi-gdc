#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo Usage: $0 CANCER
    exit 1
fi

CANCER=$1

sbatch --job-name=${CANCER} --cpus-per-task=2 --mem=2G --nodes=1 --time=300:03:03 --output=${CANCER}-leader-%j.out <<EOF
#!/bin/bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HOME/slurm/lib
python -u ./batch_download.py --num-jobs 50 --output-dir /stornext/HPCScratch/PapenfussLab/projects/gdc_download/${CANCER}/ --gdc-project-id TCGA-${CANCER} --save-query-file ${CANCER}-query.pkl --cancer ${CANCER} --run-anyway
EOF

#python -u ./batch_download.py --num-jobs 100 --output-dir /stornext/HPCScratch/PapenfussLab/projects/gdc_download/${CANCER}/ --gdc-project-id TCGA-${CANCER} --save-query-file ${CANCER}-query.pkl --cancer ${CANCER} --run-anyway --whitelist ${CANCER}-whitelist.txt

