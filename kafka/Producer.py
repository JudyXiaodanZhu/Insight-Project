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

class StreamVideo(Process):

    def __init__(self, num,
                 topic,
                 topic_partitions=8,
                 pub_obj_key="original",
                 group=None,
                 target=None,
                 name=None,
                 verbose=False,
                 rr_distribute=False):
        """Video Streaming Producer Process Class. Publishes frames from a video source to a topic.
        :param video_path: video path or url
        :param topic: kafka topic to publish stamped encoded frames.
        :param topic_partitions: number of partitions this topic has, for distributing messages among partitions
        :param use_cv2: send every frame, using cv2 library, else will use imutils to speedup training
        :param pub_obj_key: associate tag with every frame encoded, can be used later to separate raw frames
        :param group: group should always be None; it exists solely for compatibility with threading.
        :param target: Process Target
        :param name: Process name
        :param verbose: print logs on stdout
        :param rr_distribute: use round robin partitioner, should be set same as consumers.
        """
        super(StreamVideo, self).__init__()

        # TOPIC TO PUBLISH
        self.num = num
        self.frame_topic = topic
        self.topic_partitions = topic_partitions
        self.camera_num = num
        self.object_key = pub_obj_key
        self.verbose = verbose
        self.rr_distribute = rr_distribute

    def run(self):
        """Publish video frames as json objects, timestamped, marked with camera number.
        Source:
            self.video_path: URL for streaming video
            self.kwargs["use_cv2"]: use raw cv2 streaming, set to false to use smart fast streaming --> not every frame is sent.
        Publishes:
            A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """

        # Producer object, set desired partitioner
        producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers_address,
                                       value_serializer=lambda value: json.dumps(value),
                                       batch_size=100000, linger_ms=2000,compression_type='gzip')

        print("[Num{}], SET PARTITIONS FOR FRAME TOPIC: {}".format(self.num,
                                                                            producer.partitions_for(
                                                                            self.frame_topic)))
        # Track frame number
        #obj_stream = smart_open.smart_open("https://s3-us-west-2.amazonaws.com/"+config.S3_BUCKET+"/"+config.fname)
        
        
        data = pd.read_csv("https://s3-us-west-2.amazonaws.com/"+config.S3_BUCKET+"/"+config.fname)
        data = data.T
        frame_num = 0
        start_time = time.time()
        print("[CAM {}] START TIME {}: ".format(self.num, start_time))

        # Read URL, Transform, Publish
        while True:
           # Attach metadata to frame, transform into JSON
            if time.time() > start_time + 27:
                print 'timeout'
                break

            for column in data:
                if frame_num == 10000:
                    break 

                message = self.transform(frame=data[column].values, frame_num=frame_num,
                                     object_key=self.object_key,
                                     camera=self.camera_num,
                                     verbose=self.verbose)

                # Logging
                if self.verbose:
                    print("\r[PRODUCER][Cam {}] FRAME: {} ".format(self.camera_num, frame_num))
                # Publish to specific partition
                producer.send(self.frame_topic, key="{}_{}".format(self.camera_num, frame_num), value=message)

                frame_num += 1
        producer.flush()
        if self.verbose:
            print("[{}] FINISHED. STREAM TIME {}: ".format(self.camera_num, time.time() - start_time))
        return True if frame_num > 0 else False


    @staticmethod
    def transform(frame, frame_num, object_key="original", camera=0, verbose=False):
        """Serialize frame, create json message with serialized frame, camera number and timestamp.
        :param frame: numpy.ndarray, raw frame
        :param frame_num: frame number in the particular video/camera
        :param object_key: identifier for these objects
        :param camera: Camera Number the frame is from
        :param verbose: print out logs
        :return: A dict {"frame": string(base64encodedarray), "dtype": obj.dtype.str, "shape": obj.shape,
                    "timestamp": time.time(), "camera": camera, "frame_num": frame_num}
        """
        
        if verbose:
            # print raw frame size
            print("\nRAW ARRAY SIZE: ", sys.getsizeof(frame))

       
    
        frame_dict = np_to_json(frame.astype(np.float32), prefix_name="original")
       
        message = {"timestamp": time.time(), "frame_num": frame_num}
        message.update(frame_dict)

        if verbose:
            # print message size
            print("\nMESSAGE SIZE: ", sys.getsizeof(message))

        return message

    