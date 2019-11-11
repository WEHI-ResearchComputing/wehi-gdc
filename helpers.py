from abc import ABC, abstractmethod

import requests
import sys
import os

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

def noop(t, c):
  return None

class GDCFileDownloader:
  def __init__(self, file_id, output_path, auth_provider=None, progress_callback=noop):
    self.file_id = file_id
    self.output_path = output_path
    self.auth_provider = auth_provider
    self.progress_callback = progress_callback

  def __call__(self):
    headers = {'Content-Type': 'application/json'}
    if self.auth_provider:
      self.auth_provider.add_auth_header(headers)

    with requests.get(f'{GDC_ENDPOINT}data/{self.file_id}', headers=headers, stream=True) as r:
      r.raise_for_status()
      total_length = int(r.headers['content-length'])
      with open(self.output_path, 'wb') as f:
        for chunk in r.iter_content(chunk_size=8192):
          if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
            self.progress_callback(total_length, len(chunk))
