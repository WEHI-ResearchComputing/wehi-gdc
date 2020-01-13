'''
List metadata for all files associated with a case query
'''

from helpers import GDCIterator
import json
import sys
from argparse import ArgumentParser

'''
Command line argument parser
'''
def build_parser():
  parser = ArgumentParser()
  parser.add_argument('--output-file',
                      help='Output file name',
                      dest='output_file',
                      required=True)
  parser.add_argument('--gdc-project-id',
                      dest='gdc_project_id',
                      help='The GDC project id, e.g. TCGA-SKCM, TCGA-LUAD, etc',
                      type=str,
                      default=None,
                      required=True)
  return parser

case_filters = {
  'op': '=',
  'content': {
    'field': 'project.project_id',
    'value': None
  }
}

# case_filters = {
#   'op': '=',
#   'content': {
#     'field': 'files.experimental_strategy',
#     'value': None
#   }
# }

file_filters = {
  'op': '=',
  'content': {
    'field': 'cases.submitter_id',
  }
}


def main(argv):
  parser = build_parser()
  options = parser.parse_args(args=argv)

  output_file = options.output_file
  gdc_project_id = options.gdc_project_id

  case_filters['content']['value'] = gdc_project_id

  cases = []
  for case in GDCIterator('cases', case_filters):
    submitter_id = case['submitter_id']
    case_id = case['case_id']
    file_filters['content']['value'] = submitter_id

    print(f'case_id: {case_id}, submitter_id: {submitter_id}')

    flmds = []
    for fl in GDCIterator('files', file_filters):
      flmds.append(fl)

    cases.append({'case_id': case_id, 'case': case, 'files': flmds})

  js = {'cases': cases}
  with open(output_file, 'w') as f:
    print(json.dumps(js, indent=2), file=f)


if __name__ == '__main__':
  main(sys.argv[1:])