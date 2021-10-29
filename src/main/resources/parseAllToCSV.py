import csv
import glob
import os

readPath = 'F:/Users/ismer/Documents/CSD/ptuxiaki/dataset/twitterdata/exported';
writePath = 'F:/Users/ismer/Documents/CSD/ptuxiaki/dataset/twitterdata/converted';
	  
for filename in glob.glob(os.path.join(readPath, '*.txt')):
	print('Now reading: ' + filename + '\n')
	with open(os.path.join(os.getcwd(), filename), 'r') as csvfile:
		reader = csv.reader(csvfile, delimiter=' ',)
		with open(os.path.join(writePath, filename.split('\\')[-1].replace('.txt', '.csv')), mode='w', newline='') as proccesedEdges:
			writer = csv.writer(proccesedEdges, delimiter=',', quotechar='', quoting=csv.QUOTE_NONE)
			for row in reader: 
				writer.writerow([row[0], row[1], row[2]])
			print('Converted to: ' + filename.split('\\')[-1].replace('.txt', '.csv') + '\n');


	