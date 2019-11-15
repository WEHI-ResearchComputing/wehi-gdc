#!/usr/bin/env bash

#
# This runs in the batch system:
# - calls a python script to download the file
# - calls the process.sh script to actually process the file.

# Setup your python enviroment. This may be a virtual env or a conda
module load python/3.7.0

python single_file_download.py --output-paths $1 --file-ids $2

if [ $? == "0" ]
then
  ../process.sh $1
fi
