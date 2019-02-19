"""Set Parameters for the APP here...."""

import os

MAIN_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

"""General Parameters"""
# TOPIC USED TO PUBLISH ALL OBJECTS
TOPIC = "Judy-Topic"
ORIGINAL_PREFIX = "predicted"

"""Performance Parameters"""
# TOPIC PARTITIONS
SET_PARTITIONS = 32
# PARTITIONER
ROUND_ROBIN = False

"""Demo Specific Parameters"""
LOG_DIR = "logs"
CLEAR_PRE_PROCESS_TOPICS = True