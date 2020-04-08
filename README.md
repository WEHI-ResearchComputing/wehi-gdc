# WEHI Genomic Data Commons Downloads

This a lightweight template project for programmatic Genomic Data Commons (GDC) downloads in the WEHI environment. This codes handles GDC queries and has robust, restartable tools to downaload the required files. Once files are downloaded, your workflow is called to process them.

The GDC has a (comparatively) simple REST API 
[https://docs.gdc.cancer.gov/API/Users_Guide/Getting_Started/](https://docs.gdc.cancer.gov/API/Users_Guide/Getting_Started/)

You can fork this project and modify it for your use case. Please notify us of defects or suggestions for improvements or, even better, provide a pull request.

## Prerequisites
This code has only been tested with Python 3.7.0
```
module load python/3.7.0
```

Install additional python modules in home directory or in a virtual environment using the provided `requirements.txt`.

To run in the PBS batch system you must also load the PBS DRMAA module
```
module load pbs-drmaa
```

To run in SLURM you will need to source your own DRMAA library. This one works for me
[https://github.com/natefoo/slurm-drmaa](https://github.com/natefoo/slurm-drmaa). Please,
note, the configure script fails with recent SLURM versions and needs to manually fixed
[https://github.com/natefoo/slurm-drmaa/issues/28](https://github.com/natefoo/slurm-drmaa/issues/28)

## How to use this template
1. Fork it. 
2. Install is as a git submodule of your workflow. See this guide for git submodules [https://www.atlassian.com/git/tutorials/git-submodule](https://www.atlassian.com/git/tutorials/git-submodule)
3. Modify paths for your data locations. These are isolated to the `*.sh` wrapper scripts.
4. Modify the file query filter in `batch_download.py` to query for the file types you are interested in. Currently, it chooses WXS BAMs.
5. You may need to modify the metadata requested in the `file_fields` variable, just below the file query predicate.
6. Check the `download_and_process.sh` script. When the download completes, this calls a script, called `process.sh` in the parent directory, to process the download files. This script needs to be provided as part of your workflow. As it currently stands, the script is called with 3 arguments:
    1. A comma seperated list of the absolute path names of the downloaded files 
    2. A comma seperate list of the barcodes/submitter ids for each file
    3. The cancer type
7. If the data you have restricted access, place a GDC API token in `~/.gdc-user-token.txt`
8. Check the `slurm-run.sh` or `pbs-run.sh` scripts to see if they are suitable for your use. If so, you can launch or restart a run for a cancer type by simply running `./<batch system>-run.sh <cancer-type>`

## Robustness and Trouble Shooting 
The download script use pycurl, which in turn wrap libcurl. This is a highly robust library for making HTTP requests. HTTP itself, is a poor choice for moving large amounts data. GDC will close download connections randomly. The download scripts will keep retryng the connection until all data are downloaded. Similarly, if the job restarts the download will restart where it left off. Onnce downloaded, a checksum is calculated for the file and compared to the expected checksum. Your workflow will only run if the checksums match.

**Note:** The download script does not check that only copy is running. If more than one copy is running, all copies will write to the same file. In this case, the file will be unusable and will have to be deleted.

If your access token has expired, GDC still return an HTTP 200 code and returns the error message as part of the reponse stream. It is not easy to deterministically distinguish this from GDC simply closing the connection. There is a heuristic to try and detect this but it does not always work.

There is a script, `count_pairs.py` that checks for expected output directories. This will need to be modified for your use case. You should also write utilities that can query the state of you workflow.

## Scripts
### Simple downloads
The `simple.py` script can be used for simple multi threaded download based on a case -> file cascaded query. In this example, the script queries for cases in the TCGA melanoma cohort and then queries of WXS sequence files associated with those cases. 

See the `case_filter` and `file_filter` variables in the script. See the GDC data model and API documentation for to formulate other queries.

The `GDCIterator` helper class in `helpers.py` file provides a Python iterator API for GDC queries.

### Batch download and process workflows
The `batch_download.py` script allows per case files to be downloaded as batch jobs. if the downloads are successful a bash script called `../process.sh` is called. The expectation is that this repository will be a submodule in your workflow respository. The script is passed a comma seperated list of files for that case.

For example:
```
python -u batch_download.py --num-jobs 2 --output-dir /home/thomas.e/projects/gdc_download/LUAD --save-query-file query.pkl --gdc-project-id TCGA-LUAD
```

* Maintain two concurrent jobs in the batch system. Each job processes one case.
* GDC files are written to `/home/thomas.e/projects/gdc_download/LUAD`
* The query is cached in `query.pkl`. This is useful because the query can take tens of minutes
* Download files for the TCGA-LUAD (lung cancer) project

### Download project metadata
`list_file_metadata.py` downloads all the default metadata for a TCGA project into a JSON file.

###
`helpers.py` 