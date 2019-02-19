#!/usr/bin/env python2
from kafka import KafkaProducer
import time
import json 
import smart_open 
import config
from random import *
from sys import getsizeof


def main():
    # load settings
    S3_KEY = config.S3_KEY
    S3_SECRET = config.S3_SECRET
    S3_BUCKET = config.S3_BUCKET
    num_record = config.num_record_streamed
    fname = config.fname
    bootstrap_servers_address = config.bootstrap_servers_address
    kafka_topic = config.kafka_topic

    """
    # create a streaming object from S3 and setup kafka producer
    """
    obj_stream = smart_open.smart_open("https://s3-us-west-2.amazonaws.com/"+S3_BUCKET+"/"+fname)

    # setup Kafka producer and topic
    producer = KafkaProducer(bootstrap_servers = bootstrap_servers_address, value_serializer=lambda x: x.encode('utf-8'), acks=0, batch_size=10000, linger_ms=100,compression_type='gzip', buffer_memory=63554432)
    #producer = KafkaProducer(bootstrap_servers = bootstrap_servers_address) 

    k=0
    start = time.time()
    for line in obj_stream:
        try:
            # skip header
            if k!=0:
                future = producer.send(kafka_topic, line)
            k+=1
            if k==num_record:
                print "reaching target"
                end = time.time()
                elapsed = end - start
                print elapsed
                break
        except:
            print "running into an error in kafka producer..."

    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        # Decide what to do if produce request failed...
        log.exception()
        pass

    # Successful result returns assigned partition and offset
    print (record_metadata.topic)
    print (record_metadata.partition)
    print (record_metadata.offset)

if __name__ == '__main__':
    main() 
