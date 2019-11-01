import multiprocessing as mp
from helpers import GDCIterator, GDCFileAuthProvider, GDCFileDownloader
try:
  from blessings import Terminal
  terminal_control = True
  print(Terminal().clear)
except ModuleNotFoundError:
  terminal_control = False

N_THREADS = 5

def process_file(file, q):
  fn = file['file_name']
  f_id = file['file_id']

  dl_bytes = 0
  def progress(t, c):
    nonlocal dl_bytes
    dl_bytes = dl_bytes + c
    print(f'downloading {fn}: {dl_bytes}/{t}', end='\r')

  dl = GDCFileDownloader(f_id, fn, auth_provider, progress)
  dl()
  print('')

  return dl


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

class SimpleProgressMeter:
  def __init__(self, file_name, file_cnt):
    self.dl_bytes = 0
    self.file_name = file_name
    self.file_cnt = file_cnt
    self.term = None

  def __call__(self, total, chunk):
    if not terminal_control:
      return

    if self.term is None:
      # Note: the terminal object needs to be created in the download thread otherwise it fails
      self.term = Terminal()

    self.dl_bytes = self.dl_bytes + chunk
    with self.term.location(0, self.file_cnt):
      print(f'downloading {self.file_name}: {self.dl_bytes}/{total}')

p = mp.Pool(N_THREADS)
auth_provider = GDCFileAuthProvider()

file_cnt = 0
downloads = []
for case in GDCIterator('cases', case_filters):
  file_filters['content'][0]['content']['value'] = case['submitter_id']

  for fl in GDCIterator('files', file_filters):
    file_name = fl['file_name']
    file_id = fl['file_id']

    pm = SimpleProgressMeter(file_name, file_cnt)
    download = GDCFileDownloader(file_id, file_name, auth_provider, pm)

    dh = p.apply_async(download)
    downloads.append(dh)

    file_cnt = file_cnt+1

print(f'{file_cnt} files queued for download.')
for dl in downloads:
  dl.get()
p.join()
print('Done.')
