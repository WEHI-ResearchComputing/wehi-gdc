import sys
import re
import os
import pickle
import glob

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

tumour_flags = set(['01','02','03','04','05','06','07','08','09','50','60','61'])
normal_flags = set(['10','11','12','13','14','40'])
REGEX = re.compile('.*TCGA\-[^-]+\-[^-]+\-([^-]+)[A-Z]\-[^-]+([A-Z])\-.*')

def make_pairs(files, tags, case_id):
  tumour_files = []
  normal_files = []
  for f, t in zip(files, tags):
    bn = os.path.basename(f)
    m = REGEX.match(t)
    if not m:
      continue
    if m.group(2) == 'W':
      continue
    v = m.group(1)
    if v in tumour_flags:
      tumour_files.append((bn, t))
      continue
    if v in normal_flags:
      normal_files.append((bn, t))
      continue

  pairs = []
  for nf, nt in normal_files:
    for tf, tt in tumour_files:
      pairs.append((tf, nf, tt, nt, case_id))

  return pairs

cancer = sys.argv[1]

with open(f'{cancer}-query.pkl', 'rb') as f:
  case_files = pickle.load(f)

pair_cnt = 0
pairs = []
for cf in case_files:
  pairs += make_pairs(cf.file_names, cf.submitter_ids, cf.case_id)
pair_cnt = len(pairs)

actual_dirs = []
for d in glob.glob(f'/stornext/HPCScratch/PapenfussLab/projects/tcga-data/{cancer}/*'):
  if d == f'/stornext/HPCScratch/PapenfussLab/projects/tcga-data/{cancer}/logs':
    continue
  if d == f'/stornext/HPCScratch/PapenfussLab/projects/tcga-data/{cancer}/old-logs':
    continue
  if os.path.isdir(d):
    actual_dirs.append(d)

# if pair_cnt < file_cnt:
expected_dirs = set()
completed = 0
for (tf, nf, tt, nt, cid) in pairs:
  dn = f'/stornext/HPCScratch/PapenfussLab/projects/tcga-data/{cancer}/{tt}--{nt}'
  expected_dirs.add(dn)
  if not os.path.exists(dn) or not glob.glob(os.path.join(dn, '*seqz.gz')):
    print(f'{cid}')
  else:
    completed += 1

print(f'expected: {pair_cnt} completed: {completed}')


print('==>> Unexpected dirs:')
for d in actual_dirs:
  if not d in expected_dirs:
    print(d)