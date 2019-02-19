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
from pyspark.sql.types import *
import datetime
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
import uuid
import pyspark_cassandra
import numpy as np
import base64


def save_to_db(db):
    '''
        Save raw data to cassandra database 
    '''
    date = datetime.datetime.now().strftime("%B %d, %Y")
    timestamp = datetime.datetime.now()
    Record = int(db[0])
    message = np.array_str(db)
    print message
    print "== begin save tbl2 ======"
    cassandra_cluster = Cluster(config.cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('ecg')
    flow = cassandra_session.prepare('''INSERT INTO ecg_stream(Record, day, ts, message)  VALUES (?,?,?,?)''')
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    try:
        batch.add(flow, (Record,date,timestamp,message))
    except:
        pass
    cassandra_session.execute(batch)
    cassandra_cluster.shutdown()
    print "== end save tbl2 ======"


def display(rdd):
    '''
        function used to save to cassandra database 
    '''
    ma = {0.0: 'Normal beat', 1.0: 'Supraventricular premature beat', 2.0: 'Premature ventricular contraction',
3.0: 'Fusion of ventricular and normal beat', 4.0: 'Unclassifiable beat'}
    print "== begin save tbl1 ======"
    cassandra_cluster = Cluster(config.cass_cluster_IP)
    cassandra_session = cassandra_cluster.connect('ecg')
    
    insert_flow = cassandra_session.prepare('''INSERT INTO display (Irregularity, Record, Age, BMI, BSA, EF, Gender, Height, IMT, MALVMi, SBP, SBV,Smoker,Vascular_event,Weight) 
                                                      VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                   ''')
    re = ()
    print rdd
    df = cassandra_session.execute('Select * from patient_stats where record='+str(rdd[0]))
    for row in df:
        re += ma.get(rdd[1],0.0),
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
    
def sparkfilternew(x):
    '''
        function used to query a broadcasted hash-table to get background 
        option transaction volume and compare with streaming data
    '''
    try:
        x = np_from_json(x,"original")
    except Exception as e: 
        print(e)
    return x

def np_from_json(obj, prefix_name):
    """Deserialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return np.frombuffer(base64.b64decode(obj["{}_frame".format(prefix_name)].encode("utf-8")),
                         dtype=np.dtype(obj["{}_dtype".format(prefix_name)])).reshape(
        obj["{}_shape".format(prefix_name)])


reload(sys)
sys.setdefaultencoding('utf-8')
conf = SparkConf().setAppName("PythonStreamingDirectKafka")\
             .set("spark.streaming.backpressure.enabled", "true") \
             .set("spark.streaming.backpressure.initialRate", "1500")
sc = SparkContext(conf=conf)
#sc = SparkContext(appName="PythonStreamingDirectKafka")
ssc = StreamingContext(sc, 5)

conf = SparkConf()
conf.setMaster("local[4]")
conf.setAppName("Spark Cassandra")
conf.set("spark.cassandra.connection.host","ec2-52-10-35-46.us-west-2.compute.amazonaws.com")

sqlContext = SQLContext(sc)
df = sqlContext.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="patient_stats", keyspace="ecg")\
    .load()

rddtest = df.rdd.map(list)
coll = rddtest.collect()
print "Elements in RDD -> %s" % (coll)
list_3 = list()
for d in rddtest.collect():
    list_3.append(d)
list_4 = sc.broadcast(list_3)

"""
load data from kafka
"""
kvs = KafkaUtils.createDirectStream(ssc, config.kafka_topic,
                                        {"metadata.broker.list": config.bootstrap_servers_ipaddress})
line = kvs.map(lambda x:x[1])
parsed_msg = line.map(lambda x: json.loads(x))
lines = parsed_msg.map(sparkfilternew)

join = lines.filter(lambda x: x[-1] != 0.0)
dis = join.map(lambda x: (int(x[0]), x[-1]))

re = dis.map(display)
re.pprint()

save = lines.map(save_to_db)
save.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop()