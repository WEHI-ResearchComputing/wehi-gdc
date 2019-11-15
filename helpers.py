from abc import ABC, abstractmethod

import requests
import sys
import os
import hashlib
import time

GDC_ENDPOINT = 'https://api.gdc.cancer.gov/'

'''
This class implements a Python iterator that takes care of 
paging through the output from a query against the provided API endpoint
'''
class GDCIterator:
  def __init__(self, ep, filters, max_count=sys.maxsize):
    self.ep = ep
    self.filters = filters
    self.max_count = max_count
    self.hits = []
    self.total = 0
    self.frm = 0
    self.returned = 0

  def __iter__(self):
    return self

  def _get_batch(self):
    query = {
      'filters': self.filters,
      'format': 'json',
      'size': str(min(500, self.max_count)),
      'from': str(self.frm)
    }

    r = requests.post(GDC_ENDPOINT+self.ep, json=query, headers={'Content-Type': 'application/json'})
    r.raise_for_status()

    results = r.json()
    self.hits = results['data']['hits']
    self.total = int(results['data']['pagination']['total'])

  def __next__(self):
    if not self.hits:
      self._get_batch()

    self.returned = 1 + self.returned
    if self.returned > self.total:
      raise StopIteration

    return self.hits.pop(0)

'''
A class that provides the authentication token for controlled access data.
And an implementation that will read the token from a file.
'''
class GDCAuthProvider(ABC):
  @abstractmethod
  def get_token(self):
    raise NotImplemented

  def add_auth_header(self, headers):
    headers['X-Auth-Token'] = self.get_token()
    return headers

class GDCFileAuthProvider(GDCAuthProvider):
  def __init__(self, token_file=os.path.join(os.path.expanduser('~'), '.gdc-user-token.txt')):
    with open(token_file, 'r') as file:
      self.token = file.read().replace('\n', '')

  def get_token(self):
    return self.token

'''
Downloads a file from GDC
'''

class BasicProgressMeter:
  def __init__(self):
    self.sum = 0

  def __call__(self, file_name, total_length, chunk_length, **kwargs):
    self.sum = self.sum + chunk_length
    print(f'{file_name}: {self.sum}/{total_length}')

class GDCFileDownloader:
  def __init__(self, file_id, output_path, md5sum=None, auth_provider=None, progress_callback=BasicProgressMeter()):
    self.file_id = file_id
    self.output_path = output_path
    self.auth_provider = auth_provider
    self.progress_callback = progress_callback
    self.md5sum = md5sum
    self.sum_file = os.path.splitext(output_path)[0] + '.md5'

  def _check_md5(self):
    if self.md5sum is None:
      return False

    if not os.path.exists(self.sum_file):
      return False

    with open(self.sum_file, 'r') as f:
      md5sum = f.read(f).strip()

    return md5sum == self.md5sum

  def __call__(self):
    print(f'{self.output_path}: Start processing.')
    if self._check_md5():
      print(f'{self.output_path}: m5sum matches expected m5sum, skipping download.')
      return
    print(f'{self.output_path}: Download starting.')

    start = int(time.time())

    headers = {'Content-Type': 'application/json'}
    if self.auth_provider:
      self.auth_provider.add_auth_header(headers)

    md5 = hashlib.md5()

    with requests.get(f'{GDC_ENDPOINT}data/{self.file_id}', headers=headers, stream=True) as r:
      r.raise_for_status()
      total_length = int(r.headers['content-length'])
      with open(self.output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
          if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
            md5.update(chunk)
            self.progress_callback(self.output_path, total_length, len(chunk))


    md5sum = md5.hexdigest()
    print(f'{self.output_path}: md5sum={md5sum}')

    with open(self.sum_file, 'w') as f:
      f.write(md5sum + '\n')

    print(f'{self.output_path}: download completed in {int(time.time())-start} seconds')
