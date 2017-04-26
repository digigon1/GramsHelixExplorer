from os import listdir, getcwd


path = getcwd()+'\\'
data = []

for file in listdir(path):
	if 'output-' in file:
		with open(path+file, 'r') as fin:
			file_contents = fin.read().splitlines(True)
			first = file_contents[0]
			data.extend(file_contents[1:])

with open('outputs.csv', 'w') as fout:
	fout.write(first)
	fout.writelines(data)