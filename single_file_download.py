from helpers import GDCFileAuthProvider, GDCFileDownloader
from argparse import ArgumentParser
import multiprocessing as mp
import sys

def build_parser():
  parser = ArgumentParser()
  parser.add_argument('--output-paths',
                      help='comma output paths (filename) to download to',
                      dest='output_paths',
                      required=True)
  parser.add_argument('--file-ids',
                      dest='file_ids',
                      help='GDC file ids',
                      required=True)
  return parser


def main(argv):
  parser = build_parser()
  options = parser.parse_args(args=argv)

  output_paths = options.output_paths.split(',')
  file_ids = options.file_ids.split(',')

  p = mp.Pool(len(file_ids))
  downloads = []
  auth_provider = GDCFileAuthProvider()

  for (output_path, file_id) in zip(output_paths, file_ids):
    output_path = output_path.strip()
    file_id = file_id.strip()

    dl = GDCFileDownloader(file_id, output_path, auth_provider)
    h = p.apply_async(dl)
    downloads.append(h)

  for dl in downloads:
    dl.get()

  p.join()
  print('Done.')

if __name__ == '__main__':
  main(sys.argv[1:])
