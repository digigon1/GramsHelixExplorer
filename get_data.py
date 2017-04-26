import csv
from os import listdir, getcwd
import os
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import urllib.request
import urllib
import re
import threading
import traceback


max_threads = 10
depth = 10
factor = 0.3
#received = []
read = []
debug = False

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
	
def getAll(txid, n, file):
	txs = getTransactions(txid, n, file)
	if txs:
		sent.extend(txs)
		if debug:
			print('Ending transaction '+txid)

def getTransactions(txid, n, file):

	result = []

	if txid in read:
		if debug:
			print('Transaction '+txid+' already read')
		return result

	if debug:
		print(str(depth-n)+' Reading transaction '+txid)


	if txid == '':
		return result

	
	base = 'https://www.walletexplorer.com/txid/'
	url = base + txid

	i = 0
	while True:
		try:
			soup = BeautifulSoup(urllib.request.urlopen(url).read(), 'html.parser')
			break
		except Exception:
			i = i + 1
			print('Try number '+str(i))
			if i > 50:
				print('Skipping '+txid)
				return []
			continue
		break

	thrs = []

	cut_date = datetime.strptime('1980-01-01', '%Y-%m-%d')
	if os.path.exists('output/output-'+file):
		t = os.path.getmtime('output/output-'+file)
		cut_date = datetime.fromtimestamp(t)+timedelta(days=-1)

	div = soup.find('div', {'id': 'main'})

	date = div.find_all('table', class_='info')[0].find_all('td')[2].text
	this_date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')

	if this_date < cut_date:
		return result


	tx_table = div.find_all('table', class_='tx')[0]
	top = tx_table.find_all('tr')
	unparsed_total = top[0].find_all('span')[1].text
	total_amount = float(re.findall('\d+\.?\d*',unparsed_total)[0])

	#print(wallet+' '+str(total_amount))
	if total_amount < 0: # DEBUG
		if debug:
			print(total_amount)
		return result

	for tr in top[1].find_all('table', class_='empty')[1].find_all('tr'):

		link = tr.find_all('td')

		text_amount = link[2].text

		if '(' in text_amount:
			continue

		amount = float(re.findall('\d+\.?\d*',text_amount)[0])
		
		tx_a = link[3].find_all('a')
		if len(tx_a) == 0:
			continue
		tx_link = tx_a[0].get('href')
		next_tx = re.findall('[0-9a-f]{64}|$',tx_link)[0]

		wallet_link = link[1].find_all('a')
		if len(wallet_link) == 0:
			if len(re.findall('inputs: 1 ',top[0].find_all('b')[0].text)) > 0:
				result.extend(getTransactions(next_tx, n, file))
			continue
		else:
			address = link[0].text

		#if len(link) != 0:
		if amount/total_amount > factor:
			if next_tx not in read:
				if depth-n < 2 or len(re.findall('inputs: 1 ',top[0].find_all('b')[0].text)) > 0:
					
					result.extend(getTransactions(next_tx, n-1, file)) # ALL OTHER TRANSACTIONS
					
					read.append(next_tx)

		result.append(Transaction(date, address, amount, txid)) # THIS TRANSACTION

	return result


path = getcwd()
files = listdir(path)

i = 0
for file in files:
	sent = []
	threads = []
	try:

		index = file.index('.csv')

		fullfile = path+'/'+file

		num_lines = sum(1 for line in open(fullfile))
		
		with open(fullfile, 'r') as csvfile:
			print('Reading file '+file)

			reader = csv.DictReader(csvfile)

			for row in reader:
				amount = row['sent amount']
				if amount != '':
					txid = row['transaction']
					if txid != '(fee)':
						#txid = re.findall('[0-9a-f]{16}|$', txid)[0]

						#sent.extend(getTransactions(txid, 2))

						# multithreading attempt
						while threading.active_count() > max_threads:
							continue
						else:
							print('New thread')

						t = threading.Thread(target=getAll, args=(txid, depth, file, ))
						threads.append(t)
						t.start()

				i = i+1
				print(str(i)+"/"+str(num_lines))

		
		print('Waiting for threads to finish')
		j = 0
		for t in threads:
			j = j + 1
			print('Waiting for: '+str(j)+'/'+str(len(threads)))
			t.join()

		if not os.path.exists('output'):
			os.makedirs('output')

		print('Writing output')
		if os.path.exists('output/output-'+file):
			with open('output/output-'+file, 'r') as outputfile:
				reader = csv.DictReader(outputfile)
				for line in reader:
					sent.append(Transaction(line['date'], line['address'], line['amount'], line['txid']))

				#content = outputfile.read().splitlines(True)
				#content = content[1:]
				#for line in content:
				#	line_content = line.split(',')
				#	if len(line_content) > 3:
				#		sent.append(Transaction(line_content[0], line_content[2], line_content[1], line_content[3].replace('\n', '')))

		with open('output/output-'+file, 'w') as outputfile:
			outputfile.write('date,amount,address,txid\n')
			print('Starting sorting')
			for t in sorted(list(set(sent)), key=lambda s: s.date):
				outputfile.write(str(t.date)+','+str(t.amount)+','+t.address+','+t.trans+'\n')

		os.remove(file)

	except Exception:
		traceback.print_exc()

	