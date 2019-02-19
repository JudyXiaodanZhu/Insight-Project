#!/usr/bin/env python2
import time
from params import *
from utils import clear_topic, set_topic
import sys
from Producer import Stream
"""
if CLEAR_PRE_PROCESS_TOPICS:
    # Clear raw topic
    clear_topic(TOPIC)
    print("DONE CLEANING")
# set partitions for frame topic
set_topic(TOPIC, SET_PARTITIONS)

# Wait
time.sleep(5)
"""

"""--------------STREAMING--------------"""
# GET number of producers
num = 5
if len(sys.argv) > 1:
    num = int(sys.argv[1])
print num


# Start Publishing to topic
for i in range(num):
    p = Stream(num = i, topic=TOPIC, topic_partitions=SET_PARTITIONS, 
                         verbose=True,
                         pub_obj_key=ORIGINAL_PREFIX,
                         rr_distribute=ROUND_ROBIN, name=i)
    p.start()

print("[MAIN]")