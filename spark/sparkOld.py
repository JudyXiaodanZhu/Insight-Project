#!/usr/bin/env python2
import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import config
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from pyspark.sql import SQLContext
import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
import uuid
import pyspark_cassandra



def sparkfilter(x):
    '''
        function used to add beat type to data
    '''
    ma = {0.0: 'Normal beat', 1.0: 'Supraventricular premature beat', 2.0: 'Premature ventricular contraction',
3.0: 'Fusion of ventricular and normal beat', 4.0: 'Unclassifiable beat'}
    patient_id = int(x[0])
    disease_marker = float(x[-1].strip('\n'))
    if disease_marker in ma:
        x.append(ma[disease_marker])
    else:
        x.append('Normal')
    return x



def save_to_db(db):
    '''
        Save raw data to cassandra database 
    '''
    date = datetime.datetime.now().strftime("%B %d, %Y")
    temp = []
    for value in db[1:-3]:
        tu = (int(db[0]),date, datetime.datetime.now(), value)
        temp.append(tu)

    print "== begin save tbl2 ======"
    cassandra_cluster = Cluster(config.cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('ecg')
    flow = cassandra_session.prepare('''INSERT INTO ecg_stream(Record, day, ts, message)  VALUES (?,?,?,?)''')
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for d in temp:
        try:
            batch.add(flow, d)
        except:
            pass
    cassandra_session.execute(batch)
    cassandra_cluster.shutdown()
    print "== end save tbl2 ======"


def display(rdd):
    '''
        function used to save to cassandra database 
    '''
    print "== begin save tbl1 ======"
    cassandra_cluster = Cluster(config.cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('ecg')
    
    insert_flow = cassandra_session.prepare('''INSERT INTO display (Irregularity, Record, Age, BMI, BSA, EF, Gender, Height, IMT, MALVMi, SBP, SBV,Smoker,Vascular_event,Weight) 
                                                      VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                   ''')
    re = ()
    df = cassandra_session.execute('Select * from patient_stats where record='+rdd[0])
    for row in df:
        re += rdd[1],
        for ele in row:
            re += ele,
        print re
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    try:
        batch.add(insert_flow, re)
    except:
        pass
    cassandra_session.execute(batch)
    cassandra_cluster.shutdown()
    print "== end save tbl1 ======"
    

reload(sys)
sys.setdefaultencoding('utf-8')
sc = SparkContext(appName="PythonStreamingDirectKafka")
ssc = StreamingContext(sc, 5)

"""
load data from kafka
"""
kvs = KafkaUtils.createDirectStream(ssc, config.kafka_topic,
                                        {"metadata.broker.list": config.bootstrap_servers_ipaddress})
lines = kvs.map(lambda x: x[1])
lines = lines.map(lambda x: x.split(','))
raw = lines.map(sparkfilter)

join = raw.filter(lambda x: 'Normal' not in x[-1])
dis = join.map(lambda x:[x[0],x[-1]])
re = dis.map(display)
re.pprint()

save = raw.map(save_to_db)
save.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()