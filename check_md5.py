import os
import glob
from helpers import md5sum
import multiprocessing as mp
import sys

cancer = sys.argv[1]
bam_files = glob.glob(f'/stornext/HPCScratch/PapenfussLab/projects/gdc_download/{cancer}/*.bam')

class FileChecker:
  def __init__(self, fn):
    self.fn = fn

  def __call__(self, *args, **kwargs):
    sum_file = os.path.splitext(self.fn)[0] + '.md5'

    if not os.path.exists(sum_file):
      print(f'# no sum file: {sum_file}')
      print(f'rm {self.fn}')
      return

    with open(sum_file, 'r') as f:
      md5_in_sum_file = f.read().strip()

    md5_in_bam_file = md5sum(self.fn)

    if md5_in_bam_file != md5_in_sum_file:
      print(f'# bam md5: {md5_in_bam_file}  sum file: {md5_in_sum_file}')
      print(f'rm {self.fn}')

p = mp.Pool(20)
checks = []
for bam_file in bam_files:
  check = FileChecker(bam_file)
  checks.append(p.apply_async(check))

for check in checks:
  check.get()


