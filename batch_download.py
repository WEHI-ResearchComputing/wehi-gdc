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

import os
import sys
import drmaa
from multiprocessing.pool import Pool
from argparse import ArgumentParser
from helpers import GDCIterator
import pickle
import traceback
import time

#-----------------------------------------------------------------------------
# Resources for your job in qstat format
RESOURCES = '-l nodes=1:ppn=2,mem=2gb,walltime=72:00:00'
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
"""
The file might have already been downloaded and processed, in which
return False and that will be skipped
"""
def are_files_needed(case_file_set):

  for (f, s) in zip(case_file_set.file_names, case_file_set.md5s):
    # If the output file doesn't exist we need to download it
    if not os.path.exists(f):
      print(f'no output file: {f}')
      return True

    sum_file = os.path.splitext(f)[0] + '.md5'

    # If the md5sum file doesn't exist, download is presumably incomplete
    if not os.path.exists(sum_file):
      print(f'no checksum file: {sum_file}')
      return True

    # If the md5sums don't match, download it again
    with open(sum_file, 'r') as fs:
      md5sum = fs.read().strip()

    if md5sum != s:
      print(f'Checksum mismatch for {f}. expected: {s}  got: {md5sum}')
      return True

  return False
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
'''
Edit these filters for your requirements (refer to the GDC documentation)
'''
case_filters = {
  'op': '=',
  'content': {
    'field': 'project.project_id',
    'value': None
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
file_fields = 'file_id,file_name,md5sum,file_size,cases.samples.portions.analytes.aliquots.submitter_id'
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
'''
Command line argument parser
'''
def build_parser():
  parser = ArgumentParser()
  parser.add_argument('--output-dir',
                      help='The directory where files will be downloaded to',
                      dest='output_dir',
                      required=True)
  parser.add_argument('--num-jobs',
                      dest='num_jobs',
                      help='Number of concurrent download jobs (each job downloads all files for a case in parallel.',
                      type=int,
                      default=1,
                      required=False)
  parser.add_argument('--start-after',
                      dest='start_after',
                      help='Start submitting jobs after this many queries (useful after using --stop-after).',
                      type=int,
                      default=0,
                      required=False)
  parser.add_argument('--stop-after',
                      dest='stop_after',
                      help='Stop after submitting this many jobs (useful when testing).',
                      type=int,
                      default=sys.maxsize,
                      required=False)
  parser.add_argument('--save-query-file',
                      dest='save_query_file',
                      help='If this file exists, unpickle it instead of redoing the query. ' + \
                           'If it does not exist save the query into this file.',
                      type=str,
                      default=None,
                      required=False)
  parser.add_argument('--gdc-project-id',
                      dest='gdc_project_id',
                      help='The GDC project id, e.g. TCGA-SKCM, TCGA-LUAD, etc',
                      type=str,
                      default=None,
                      required=True)
  parser.add_argument('--dry-run',
                      dest='dry_run',
                      help='Just determine how runs are required.',
                      action='store_true',
                      default=False,
                      required=False)
  parser.add_argument('--run-anyway',
                      dest='run_anyway',
                      help='Run the processing script even if files are downloaded',
                      default=False,
                      action='store_true',
                      required=False)
  parser.add_argument('--cancer',
                      dest='cancer',
                      help='The TCGA cancer name, e.g. COAD, SKCM, etc',
                      required=True)
  parser.add_argument('--whitelist',
                      dest='whitelist',
                      help='A file of case ids to process. If not specified, all cases are processed',
                      default=None,
                      required=False)

  return parser
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
'''
A simple container for files associated with an individual patient
'''
class CaseFileSet:
  def __init__(self, output_dir, case_id):
    self.file_ids      = []
    self.file_names    = []
    self.md5s          = []
    self.sizes         = []
    self.submitter_ids = []
    self.case_id       = case_id
    self.output_dir    = output_dir

  def add(self, file_id, file_name, md5, size, submitter_id):
    self.md5s.append(md5)
    self.file_ids.append(file_id)
    self.file_names.append(os.path.join(self.output_dir, file_name))
    self.sizes.append(size)
    self.submitter_ids.append(submitter_id)
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
"""
During testing, just return a single file, then scale up
"""
def get_file_list(output_dir):
  print('Starting file query')

  files = []
  for case in GDCIterator('cases', case_filters):
    this_case = case['submitter_id']
    file_filters['content'][0]['content']['value'] = this_case

    cfs = CaseFileSet(output_dir, case['case_id'])
    for fl in GDCIterator('files', file_filters, fields=file_fields):
      filename = fl['file_name']
      file_id  = fl['file_id']
      md5      = fl['md5sum']
      size     = fl['file_size']
      submitter_id = fl['cases'][0]['samples'][0]['portions'][0]['analytes'][0]['aliquots'][0]['submitter_id']
      cfs.add(file_id, filename, md5, size, submitter_id)
      print(f'found {filename}')

    files.append(cfs)

  return files
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
"""
 This class builds and manages a batch job
"""
class Job:
  def __init__(self, case_file_set, cancer):
    self.cfs = case_file_set
    self.cancer = cancer
    self.session = None

  def __call__(self, *args, **kwargs):
    while not self._submitted():
      print('Job submit failed, retrying in 120 seconds')
      time.sleep(120)

    if not self.session:
      return

    finished = False
    while not finished:
      try:
        info = self.session.wait(self.job_id, 30)
        print('Completed job: {job_id}'.format(job_id=self.job_id))
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
        finished = True
        self.session.exit()
      except drmaa.errors.NoActiveSessionException:
        finished = True
        print('No active session, giving up waiting')
      except drmaa.errors.ExitTimeoutException:
        print('Timeout, trying polling again in 60s')
        time.sleep(60)
      except drmaa.errors.InvalidJobException:
        print('Invalid job id, assuming Completed job: {job_id}'.format(job_id=self.job_id))
        finished = True
      except drmaa.errors.InternalException as ex:
        print(ex)
        traceback.print_stack()
        time.sleep(120)

  def _submitted(self):
    output_paths = self.cfs.file_names
    file_ids = self.cfs.file_ids
    md5sums = self.cfs.md5s
    sizes = self.cfs.sizes
    submitter_ids = self.cfs.submitter_ids

    if not output_paths:
      print('No files, no job.')
      return True

    print('Building job for {fn1}, etc'.format(fn1=output_paths[0]))
    s = drmaa.Session()
    s.initialize()
    self.session = s

    try:
      jt = s.createJobTemplate()
      jt.workingDirectory = os.getcwd()
      jt.outputPath = os.getcwd()
      jt.joinFiles = True
      jt.jobName = os.path.basename(output_paths[0])
      jt.remoteCommand = os.path.join(os.getcwd(), 'download-and-process.sh')
      jt.args = [
        ','.join(output_paths),
        ','.join(file_ids),
        ','.join(md5sums),
        ','.join([str(size) for size in sizes]),
        ','.join(submitter_ids),
        self.cancer
      ]
      jt.nativeSpecification = RESOURCES
      self.job_id = s.runJob(jt)

    except drmaa.errors.InternalException as ex:
      print(ex)
      traceback.print_stack()
      return False

    return True
#-----------------------------------------------------------------------------
def read_whitelist(whitelist_file):
  if not whitelist_file:
    return None

  with open(whitelist_file) as f:
    wl = f.readlines()

  whitelist = set()
  for c in wl:
    if len(c) != 0:
      whitelist.add(c.strip())

  return whitelist
#-----------------------------------------------------------------------------
def main(argv):
  parser = build_parser()
  options = parser.parse_args(args=argv)

  num_jobs = options.num_jobs
  stop_after = options.stop_after
  start_after = options.start_after
  output_dir = options.output_dir
  os.makedirs(output_dir, mode=0o770, exist_ok=True)
  save_query_file = options.save_query_file
  dry_run = options.dry_run
  run_anyway = options.run_anyway
  gdc_project_id = options.gdc_project_id
  cancer = options.cancer
  whitelist = read_whitelist(options.whitelist)

  case_filters['content']['value'] = gdc_project_id

  # Get the file list and filter for the ones we want to process
  if save_query_file is not None and os.path.exists(save_query_file):
    with open(save_query_file, 'rb') as f:
      case_files = pickle.load(f)
  else:
    case_files = get_file_list(output_dir)

  if save_query_file is not None and not os.path.exists(save_query_file):
    with open(save_query_file, 'wb') as f:
      pickle.dump(case_files, f)

  if not run_anyway:
    case_files = filter(are_files_needed, case_files)

  if whitelist:
    case_files = filter(lambda c: c.case_id in whitelist, case_files)

  if dry_run:
    cnt = 0
    for _ in case_files:
      cnt += 1
    print(f'{cnt} cases need one or more downloads')
    quit()

  # A pool of workers. Each worker will manage a job in the batch system
  p = Pool(num_jobs)

  # Create jobs for each file
  submitted_jobs = []
  cnt = 0
  for fn in case_files:
    if cnt>=stop_after:
      break
    cnt += 1
    if cnt<=start_after:
      continue

    submitted_jobs.append(p.apply_async(Job(fn, cancer)))

  # Wait for them to finish
  for submitted_job in submitted_jobs:
    submitted_job.get()
#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------
if __name__ == '__main__':
  main(sys.argv[1:])
#-----------------------------------------------------------------------------
