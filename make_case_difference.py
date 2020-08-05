import json

with open('LUAD-500-metadata.json') as f:
  cases1 = json.load(f)

with open('LUAD-metadata.json') as f:
  cases2 = json.load(f)

caseset = set()
for case in cases1['cases']:
  caseset.add(case['case_id'])

whitelist = []
for case in cases2['cases']:
  case_id = case['case_id']
  if case_id not in caseset:
    whitelist.append(case_id)

for case_id in whitelist:
  print(case_id)


