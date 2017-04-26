from os import listdir, getcwd


path = getcwd()+"/wallets/"
data = []

for file in listdir(path):
	if file.endswith('.csv'):
		with open(path+file, 'r') as fin:
			file_contents = fin.read().splitlines(True)
		with open(path+file, 'w') as fout:
			fout.writelines(file_contents[1:])