
from helpers import GDCIterator, GDCFileAuthProvider, GDCFileDownloader

def process_file(file):
  fn = file['file_name']
  f_id = file['file_id']
  auth_provider = GDCFileAuthProvider()

  dl = GDCFileDownloader(f_id, fn, auth_provider)
  dl_bytes = 0
  def p(t, c):
    nonlocal dl_bytes
    dl_bytes = dl_bytes + c
    print(f'downloading {fn}: {dl_bytes}/{t}', end='\r')

  dl.download(p)
  print('')

  return 1


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

file_cnt = 0
for case in GDCIterator('cases', case_filters):
  file_filters['content'][0]['content']['value'] = case['submitter_id']
  for fl in GDCIterator('files', file_filters):
    process_file(fl)
    file_cnt = file_cnt+1

print(f'{file_cnt} files downloaded')
