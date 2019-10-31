import requests
import sys

GDC_ENDPOINT = 'https://api.gdc.cancer.gov/'

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