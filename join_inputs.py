from os import listdir, getcwd


path = getcwd()+"/wallets/"
data = []

for file in listdir(path):
	try:
		with open(path+file, 'r') as fin:
			file_contents = fin.read().splitlines(True)
			first = file_contents[0]
			data.extend(file_contents[1:])
	except Exception as e:
		pass

with open(getcwd()+'/inputs.csv', 'w') as fout:
	fout.write(first.replace(' ', '_'))
	fout.writelines(data)