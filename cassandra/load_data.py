import sys
from cassandra.cluster import Cluster, BatchStatement
sys.path.append("..")


cluster = Cluster()
session = cluster.connect('ecg')

insert_flow = session.prepare('''INSERT INTO patient_stats (Record, Gender, Age, Weight, Height, BSA, BMI, Smoker,SBP, SBV, IMT, MALVMi, EF, Vascular_event) 
                                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                   ''')
batch = BatchStatement()

with open("info.txt", "r") as fp:  
	lines = fp.readlines()
	cnt = 0
	for li in lines:
		line = li.split('\t')
		print line
		if 'BSA' in line[0]:
			break
		if cnt == 0:
			cnt += 1
			continue
		batch.add(insert_flow, (int(line[0]),line[1],line[2],line[3],line[4],line[5],line[6],line[7],line[8],line[9],line[10],line[11],line[12],line[13]))
		cnt += 1
	

session.execute(batch)
cluster.shutdown()
print "========load data========"

