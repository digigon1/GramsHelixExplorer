from bs4 import BeautifulSoup
import urllib.request
import urllib
import threading
import os


class Wallet(object):
	"""docstring for Wallet"""
	def __init__(self, base, url):
		super(Wallet, self).__init__()
		self.url = url
		self.base = base

		while True:
				try:
					html = urllib.request.urlopen(base+url).read()
				except Exception:
					continue
				break
		soup = BeautifulSoup(html, 'html.parser')
		last = soup.find_all("div", class_="paging")[0].find_all("a")[2].get('href')
		self.max = int(last[last.rfind("=")+1:])
		print(self.max)

	def download(self):
		# TODO
		print('Downloading '+self.url)
		i = 1
		name = self.url[self.url.rfind('/')+1:]
		while i <= self.max:
			while True:
				try:
					url_loc = self.base+self.url+"?page="+str(i)+"&format=csv"
					print(url_loc)
					urllib.request.urlretrieve(url_loc,"wallets/"+name+"-"+str(i)+".csv")
					if i == 1:
						if '<html' in open("wallets/"+name+"-"+str(i)+".csv",'r').read():
							os.remove("wallets/"+name+"-"+str(i)+".csv")
							print('file '+name+' does not exist, please remove from inputs')
							return
				except Exception:
					continue
				break
			i = i + 1
		print('Download finished')
		


base_url = 'http://www.walletexplorer.com'
max_threads = 10

wallet_names = ['HelixMixer'] # ADD OTHER WALLETS HERE
threads = []

if not os.path.exists('wallets'):
	os.makedirs('wallets')

for wall in wallet_names:
	wallets = []
	print('Starting wallet '+wall)
	wallets.append(Wallet(base_url, '/wallet/'+wall))
	
	url = base_url + '/wallet/' + wall
	while True:
		try:
			html = urllib.request.urlopen(url).read()
		except Exception:
			continue
		break
	
	soup = BeautifulSoup(html, 'html.parser')
	
	others = soup.find_all("div", class_="alternatives")
	if others is not None and len(others) != 0:
		for link in others[0].find_all("a"):
			wallets.append(Wallet(base_url, link.get('href')))
	
	for w in wallets:

		while threading.active_count() > max_threads:
			continue

		t = threading.Thread(target=w.download, args=())
		threads.append(t)
		t.start()

for t in threads:
	t.join()