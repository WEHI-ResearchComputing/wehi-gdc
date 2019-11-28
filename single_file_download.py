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
  parser.add_argument('--md5sums',
                      dest='md5sums',
                      help='md5 hashes',
                      required=False)
  return parser


def main(argv):
  parser = build_parser()
  options = parser.parse_args(args=argv)

  output_paths = options.output_paths.split(',')
  file_ids = options.file_ids.split(',')
  md5sums = options.md5sums.split(',')
  if md5sums is None:
    md5sums = [None] * len(file_ids)

  p = mp.Pool(len(file_ids))
  downloads = []
  auth_provider = GDCFileAuthProvider()

  for (output_path, file_id, md5sum) in zip(output_paths, file_ids, md5sums):
    output_path = output_path.strip()
    file_id = file_id.strip()

    dl = GDCFileDownloader(file_id, output_path, auth_provider=auth_provider, md5sum=md5sum)
    h = p.apply_async(dl)
    downloads.append(h)

  success = True
  for dl in downloads:
    success = success and dl.get()

  if success:
    print('Downloads succeeded.')
    quit(0)
  else:
    print('Downloads failed.')
    quit(1)

if __name__ == '__main__':
  main(sys.argv[1:])
