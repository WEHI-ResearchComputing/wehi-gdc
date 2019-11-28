#!/usr/bin/env bash

#
# This runs in the batch system:
# - calls a python script to download the file
# - calls the process.sh script to actually process the file.

# Setup your python enviroment. This may be a virtual env or a conda
module load python/3.7.0

python -u single_file_download.py --output-paths $1 --file-ids $2 --md5sums $3

# This is a crude mechanism to limit the number of jobs in the queue
while [ $(qstat -u $USER|wc -l) -gt 1000 ]
do
    sleep 600
done

if [ $? == "0" ]
then
  ../process.sh $1
fi
