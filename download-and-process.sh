#!/usr/bin/env bash

#
# This runs in the batch system:
# - calls a python script to download the file
# - if the dl succeeds calls the process.sh script to actually process the file.

hostname

# Setup your python enviroment. This may be a virtual env or a conda
module load python/3.7.0

CMD="python -u single_file_download.py --output-paths $1 --file-ids $2 --md5sums $3 --sizes $4"
echo $CMD

# Stop queue from overloading
#while [ $(qstat -u $USER|wc -l) -gt 1500 ]
#do
#   sleep 600
#done

$CMD

if [ $? == "0" ]
then
  ../process.sh $1 $5 $6
fi
