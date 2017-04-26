import os
from datetime import datetime, timedelta
import json
import re
import csv

class JSONMatch(object):
	def __init__(self, f, t):
		self.fr = f
		self.matches = []
		self.matches.append(t)

	def unique(self):
		return len(self.matches) == 1

class Match(object):
	def __init__(self, tr_from, tr_to):
		self.tr_from = tr_from
		self.tr_to = tr_to

class Transaction(object):
	def __init__(self, date, address, amount, trans):
		super(Transaction, self).__init__()
		self.date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
		self.address = address
		self.amount = float(amount)
		self.trans = re.findall('[0-9a-f]{64}',trans)[0]

	def __eq__(self, other):
		return self.date == other.date and self.address == other.address and self.amount == other.amount and self.trans == other.trans

	def __hash__(self):
		return hash(('date', self.date,	'address', self.address, 'amount', self.amount, 'trans', self.trans))


inputs = []
print('opening inputs')
with open('sorted_input.csv', 'r') as inputfile:
	reader = csv.DictReader(inputfile)
	for line in reader:
		if line['received_amount']:
			inputs.append(Transaction(line['date'], line['address'], line['received_amount'], line['txid']))

	#file_contents = inputfile.read().splitlines(True)
	#content = file_contents[1:]
	#for line in content:
	#	line_content = line.split(',')
	#	if line_content[2]:
	#		inputs.append(Transaction(line_content[0], line_content[2], line_content[1], line_content[3]))

outputs = []
print('opening outputs')
with open('sorted_output.csv', 'r') as outputfile:
	reader = csv.DictReader(outputfile)
	for line in reader:
		if line['sent_amount']:
			outputs.append(Transaction(line['date'], line['address'], line['sent_amount'], line['txid']))

#	file_contents = outputfile.read().splitlines(True)
#	content = file_contents[1:]
#	for line in content:
#		line_content = line.split(',')
#		if line_content[2]:
#			outputs.append(Transaction(line_content[0], line_content[1], line_content[2], line_content[3]))

print('running matches')

i = 0
count = 0
matches = []
max_matches = 0
for wallet in inputs:
	walls = []
	for out_wallet in outputs:
		if out_wallet.date >= wallet.date:
			if out_wallet.date <= wallet.date+timedelta(hours=7):
				if out_wallet.amount/wallet.amount > 0.975 and out_wallet.amount/wallet.amount < 0.977:
					walls.append(out_wallet)
			else:
				outputs.remove(out_wallet)
		else:
			break

	if len(walls) == 0:
		print("-------------------")
	else:
		for w in walls:
			matches.append(Match(wallet, w))
		if len(walls) > max_matches:
			max_matches = len(walls)
		count = count + 1

	i = i + 1
	print(i/len(inputs))


i = 0
json_matches = {}
unique = 0
for m in matches:
	
	try:
		a = json_matches[m.tr_from]
		a.matches.append(m.tr_to)
		unique = unique - 1
	except Exception as e:
		json_matches[m.tr_from] = JSONMatch(m.tr_from, m.tr_to)
		unique = unique + 1

	
	i = i + 1
	print(i/len(matches))

json_matches = list(json_matches.values())

print('Cleaning unique repeats')
unique_tr = []
for jsonM in json_matches:
	if jsonM.unique():
		unique_tr.append(jsonM.matches[0])

while True:

	i = 0
	for jsonM in json_matches:
		if not jsonM.unique():
			jsonM.matches = [x for x in jsonM.matches if x not in unique_tr]
		i = i + 1
		print(str(i/len(json_matches)))

	unique_temp = []
	i = 0
	for jsonM in json_matches:
		if jsonM.unique():
			unique_temp.append(jsonM.matches[0])
		i = i + 1
		print(str(i/len(json_matches)))

	if set(unique_temp) == set(unique_tr):
		break
	else:
		print(str(len(unique_tr))+" "+str(len(unique_temp)))
		unique_tr = unique_temp


d = {"result":json_matches}
#dump = json.dumps(d)
#print(dump)
def handler(o):
	if isinstance(o, datetime):
		return o.isoformat()
	else:
		return o.__dict__

#with open("json_matches.json", "w") as file:
#	file.write(json.dumps(d, indent=2, default=handler))

with open('matches.csv', 'w') as file:
	file.write('"Time UTC","Helix address","Amount received","Destination address","Amount sent out"\n')
	for jsonM in json_matches:
		if jsonM.unique():
			file.write(str(jsonM.fr.date.strftime('%Y-%m-%d %H:%M:%S'))+','+str(jsonM.fr.address)+','+str(jsonM.fr.amount)+','+str(jsonM.matches[0].address)+','+str(jsonM.matches[0].amount)+'\n')

print(unique)
print(max_matches)
print(count)