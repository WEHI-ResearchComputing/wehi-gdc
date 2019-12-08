# WEHI Genomic Data Commons Downloads

This a lightweight skeleton project for programmatic Genomic Data Commons (GDC) downloads in the WEHI
environment. The GDC has a (comparatively) simple REST API 
[https://docs.gdc.cancer.gov/API/Users_Guide/Getting_Started/](https://docs.gdc.cancer.gov/API/Users_Guide/Getting_Started/)

You can fork this project and modify it for your use case. Please notify us of defects or suggestions for improvements 
or, even better, provide a pull request.

## Prerequisites
This code has only been tested with Python 3.7. 
```
module load python/3.7.1
```

Additional python modules in home directory or in a virtual environment using the provided `requirements.txt`.

To run in the batch system you must also load the PBS DRMAA module
```
module load pbs-drmaa
```

## Simple downloads
The `simple.py` script can be used for simple multi threaded download based on a case -> file cascaded query. In this
example, the script queries for cases in the TCGA melanoma cohort and then queries of WXS sequence files associated
with those cases. 

See the `case_filter` and `file_filter` variables in the script. See the GDC data model and API documentation for to 
formulate other queries.

The `GDCIterator` helper class in `helpers.py` file provides a Python iterator API for GDC queries.

## Batch download and process workflows
The `batch_download.py` script allows per case files to be downloaded as batch jobs. if the downloads are successful a bash script called `../process.sh` is called. The expectation is that this repository will be a submodule in your workflow respository. The script is passed a comma seperated list of files for that case.

For example:
```
python -u batch_download.py --num-jobs 2 --output-dir /home/thomas.e/projects/gdc_download/LUAD --save-query-file query.pkl --gdc-project-id TCGA-LUAD
```

* Maintain two concurrent jobs in the batch system. Each job processes one case.
* GDC files are written to `/home/thomas.e/projects/gdc_download/LUAD`
* The query is cached in `query.pkl`. This is useful because the query can take tens of minutes
* Download files for the TCGA-LUAD (lung cancer) project
