from helpers import GDCFileAuthProvider, GDCFileDownloader
from argparse import ArgumentParser
import sys

def build_parser():
  parser = ArgumentParser()
  parser.add_argument('--output-path',
                      help='output path (filename) to download to',
                      dest='output_path',
                      required=True)
  parser.add_argument('--file-id',
                      dest='file_id',
                      help='GDC file id',
                      required=True)
  return parser


def main(argv):
  parser = build_parser()
  options = parser.parse_args(args=argv)

  output_path = options.output_path
  file_id = options.file_id

  auth_provider = GDCFileAuthProvider()
  dl = GDCFileDownloader(file_id, output_path, auth_provider)

  print(f'Download of {file_id} -> {output_path} started')
  dl()
  print(f'Download of {output_path} finished')

if __name__ == '__main__':
  main(sys.argv[1:])
