import base64
import struct
import json
import re
import sys
import time
from multiprocessing import Process
import config 
from kafka import KafkaProducer
import smart_open
import boto
import pandas as pd
from utils import np_to_json
import numpy as np
import pickle

class Stream(Process):

    def __init__(self, num,
                 topic,
                 topic_partitions=8,
                 pub_obj_key="original",
                 group=None,
                 target=None,
                 name=None,
                 verbose=False,
                 rr_distribute=False):
        """ Streaming Producer Process Class. Publishes data to a topic.
        """
        super(Stream, self).__init__()

        # TOPIC TO PUBLISH
        self.num = num
        self.topic = topic
        self.topic_partitions = topic_partitions
        self.partition_num = num
        self.object_key = pub_obj_key
        self.verbose = verbose
        self.rr_distribute = rr_distribute

    def run(self):
        """Publish data as json objects, timestamped.
        """

        # Producer object, set desired partitioner
        producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers_address,
                                       value_serializer=lambda value: json.dumps(value),
                                       batch_size=70000, linger_ms=100,compression_type='gzip')

        
        data = pd.read_csv("https://s3-us-west-2.amazonaws.com/"+config.S3_BUCKET+"/"+config.fname)
        data = data.T
        line_num = 0
        start_time = time.time()
        print start_time
        # Read URL, Transform, Publish
        while True:
           # Attach metadata to data, transform into JSON
            if time.time() > start_time + 27:
                print 'timeout'
                break

            for column in data:
                if line_num == 10000:
                    break 

                message = self.transform(line=data[column].values, line_num=line_num,
                                     object_key=self.object_key,
                                     partition=self.partition_num,
                                     verbose=self.verbose)

                # Logging
                if self.verbose:
                    print("\r[PRODUCER][Partition {}] line: {} ".format(self.partition_num, line_num))
                # Publish to specific partition
                producer.send(self.topic, key="{}_{}".format(self.partition_num, line_num), value=message)
                line_num += 1

        producer.flush()
        if self.verbose:
            print("[{}] FINISHED. STREAM TIME {}: ".format(self.partition_num, time.time() - start_time))
        return True if line_num > 0 else False


    @staticmethod
    def transform(line, line_num, object_key="original", partition=0, verbose=False):
        """Serialize data, create json message with serialized data and timestamp.
        """
        
        if verbose:
            # print raw size
            print("\nRAW ARRAY SIZE: ", sys.getsizeof(line))
    
        line_dict = np_to_json(line.astype(np.float32), prefix_name="original")
        message = {"timestamp": time.time()}
        message.update(line_dict)

        if verbose:
            # print message size
            print("\nMESSAGE SIZE: ", sys.getsizeof(message))

        return message

    