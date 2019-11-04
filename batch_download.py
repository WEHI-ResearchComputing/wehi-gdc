import os
import drmaa
from multiprocessing.pool import Pool
from helpers import GDCIterator, GDCFileAuthProvider

"""
This is a template for building a system that will list GDC files, download them and then submit jobs to the batch 
system process them. You need to
1. Edit the case and file filters to select the data and file types that you need 
2. Fill in the is_file_needed function (this determines whether the file should be downloaded
3. Adapt the get_file_list function for your use case
4. Provide a process.sh bash script that takes source endpoint file name as argument. You will need
   to know where the file is located based on your endpoint configuration.
Good luck!
"""

#-----------------------------------------------------------------------------
# Number of current jobs (start small).
NUM_JOBS = 2

# Working directory where the script is expected to be located and
# where the script will run (may not be where the files are).
# Note: Probably should be absolute path.
WORKING_DIR = os.getcwd()

# Resources for your job in qstat format
RESOURCES = '-l nodes=1:ppn=1,mem=1gb,walltime=01:00:00'
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
"""
The file might have already been downloaded and processed, in which
return False and that will be skipped
"""
def is_file_needed(fn):
  return True
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
'''
Edit these filters for your requirements (refer to the GDC documentation)
'''
case_filters = {
  'op': '=',
  'content': {
    'field': 'project.project_id',
    'value': 'TCGA-SKCM'
  }
}

file_filters = {
  'op': 'and',
  'content': [
    {
      'op': '=',
      'content': {
        'field': 'cases.submitter_id',
      }
    },
    {
      'op': '=',
      'content': {
        'field': 'data_format',
        'value': 'BAM'
      }
    },
    {
      'op': '=',
      'content': {
        'field': 'experimental_strategy',
        'value': 'WXS'
      }
    }
  ]
}
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
"""
During testing, just return a single file, then scale up
"""
def get_file_list():
  print('Starting file listing')

  files = []
  for case in GDCIterator('cases', case_filters):
    if len(files) > 2:
      break

    file_filters['content'][0]['content']['value'] = case['submitter_id']

    for fl in GDCIterator('files', file_filters):
      filename = fl['file_name']
      file_id = fl['file_id']
      if is_file_needed(filename):
        files.append((filename, file_id))

  print('{n} files found.'.format(n=len(files)))

  return files
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
"""
 This class builds and manages a batch job
"""
class Job:
  def __init__(self, output_path, file_id):
    self.file_id = file_id
    self.output_path = output_path

  def __call__(self, *args, **kwargs):
    output_path = self.output_path

    print('Building job for {fn}'.format(fn=output_path))
    s = drmaa.Session()
    s.initialize()

    try:
      jt = s.createJobTemplate()
      jt.workingDirectory = WORKING_DIR
      jt.outputPath = WORKING_DIR
      jt.joinFiles = True
      jt.jobName = os.path.basename(output_path)
      jt.remoteCommand = os.path.join(os.getcwd(), 'download-and-process.sh')
      jt.args = [self.output_path, self.file_id]
      jt.nativeSpecification = RESOURCES
      job_id = s.runJob(jt)

      print('Submitted job: {job_id}'.format(job_id=job_id))
      info = s.wait(job_id, drmaa.Session.TIMEOUT_WAIT_FOREVER)
      print('Completed job: {job_id}'.format(job_id=job_id))
      print("""\
      id:                        %(jobId)s
      exited:                    %(hasExited)s
      signaled:                  %(hasSignal)s
      with signal (id signaled): %(terminatedSignal)s
      dumped core:               %(hasCoreDump)s
      aborted:                   %(wasAborted)s
      resource usage:
      %(resourceUsage)s
      """ % info._asdict())
    finally:
      s.exit()
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
def main():
  # Get the file list and filter for the ones we want to process
  files = get_file_list()

  # A pool of workers. Each worker will manage a job in the batch system
  p = Pool(NUM_JOBS)
  # Create jobs for each file
  jobs = [Job(fn[0], fn[1]) for fn in files]
  # Submit them to the pool
  submitted_jobs = [p.apply_async(job) for job in jobs]

  # Wait for them to finish
  for submitted_job in submitted_jobs:
    submitted_job.get()
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
if __name__ == '__main__':
  main()
#-----------------------------------------------------------------------------
