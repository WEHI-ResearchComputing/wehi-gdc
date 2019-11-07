'''
List metadata for all files associated with a case query
'''

from helpers import GDCIterator
import json

case_filters = {
  'op': '=',
  'content': {
    'field': 'project.project_id',
    'value': 'TCGA-SKCM'
  }
}

file_filters = {
  'op': '=',
  'content': {
    'field': 'cases.submitter_id',
  }
}

for case in GDCIterator('cases', case_filters):
  file_filters['content']['value'] = case['submitter_id']

  for fl in GDCIterator('files', file_filters):
    print(json.dumps(fl, indent=2))
