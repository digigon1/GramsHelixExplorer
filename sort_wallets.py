import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup
import urllib.request
import urllib
import re
import threading
import traceback

class Transaction(object):
	"""docstring for Transaction"""
	def __init__(self, date, address, amount, trans):
		super(Transaction, self).__init__()
		self.date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
		self.address = address
		self.amount = amount
		self.trans = trans

	def __eq__(self, other):
		return self.date == other.date and self.address == other.address and self.amount == other.amount and self.trans == other.trans

	def __hash__(self):
		return hash(('date', self.date,	'address', self.address, 'amount', self.amount, 'trans', self.trans))

def get_address(date, wallet, amount, txid):

	base = 'https://www.walletexplorer.com/txid/'
	url = base + txid

	j = 0
	while True:
		try:
			soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'html.parser')
			break
		except Exception:
			j = j + 1
			if j > 50:
				print('Skipping '+url)
				return
			continue
		break

	try:
		tx_table = soup.find_all('table', class_='tx')[0]
	except Exception as e:
		print(url)

	top = tx_table.find_all('tr')
	address = ""
	for tr in top[1].find_all('table', class_='empty')[1].find_all('tr'):
		link = tr.find_all('td')
		text_amount = link[2].text

		if '(' in text_amount:
			continue

		amn = float(re.findall('\d+\.?\d*',text_amount)[0])

		if float(amount) == amn:
			address = link[0].text
			#print(address)
			break


	trans.append(Transaction(date, address, amount, txid))

	pass

path = os.getcwd()
trans = []
threads = []
with open(path+'\..\..\inputs.csv', 'r') as inputfile:
	file_contents = inputfile.read().splitlines(True)
	content = file_contents[1:]
	lines = len(content)
	j = 0

	inputfile.seek(0)
	reader = csv.DictReader(inputfile)
	for line in reader:
		if line['received_amount']:
			while threading.active_count() > 50:
				continue

			t = threading.Thread(target=get_address, args=(line['date'], line['received_from'], line['received_amount'], line['transaction'], ))
	#for line in content:
	#	line_content = line.split(',')
	#	if line_content[2]:
	#		while threading.active_count() > 50:
	#			continue
#
#			t = threading.Thread(target=get_address, args=(line_content[0], line_content[1], line_content[2], line_content[6], ))
		
			threads.append(t)
			t.start()
			j = j + 1
			print(j/lines)

	j = 0
	for t in threads:
		j = j + 1
		print('Waiting for: '+str(j)+'/'+str(len(threads)))
		t.join()

with open('sorted_input.csv', 'w') as sortedinputfile:
	sortedinputfile.write('date,received_amount,address,txid\n')
	for t in sorted(list(set(trans)), key=lambda s: s.date, reverse=True):
		sortedinputfile.write(str(t.date)+','+str(t.amount)+','+t.address+','+t.trans+'\n')

print('Sorting outputs')
trans = []
with open('outputs.csv', 'r') as outputfile:
	reader = csv.DictReader(outputfile)
	for line in reader:
		trans.append(Transaction(line['date'], line['address'], line['amount'], line['txid']))

	#file_contents = outputfile.read().splitlines(True)
	#content = file_contents[1:]
	#trans = []
	#for line in content:
	#	line_content = line.split(',')
	#	if line_content[2]:
	#		trans.append(Transaction(line_content[0], line_content[1], line_content[2], line_content[3]))


with open('sorted_output.csv', 'w') as sortedoutputfile:
	sortedoutputfile.write('date,sent_amount,address,txid\n')
	for t in sorted(list(set(trans)), key=lambda s: s.date, reverse=True):
		sortedoutputfile.write(str(t.date)+','+str(t.amount)+','+t.address+','+t.trans+'\n')