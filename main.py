import requests
from helper import GDCIterator, GDC_ENDPOINT

def process_file(file, auth_token=None):
  fn = file['file_name']
  f_id = file['file_id']

  if file['access'] == 'controlled' and auth_token is None:
    raise Exception(f'Downloading {fn} requires authentication')

  dl = requests.get(GDC_ENDPOINT+'data/'+f_id)
  with open(fn, 'wb') as f:
    f.write(dl.content)
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
    print(fl['file_name'])
    file_cnt = file_cnt+1

print(file_cnt)
