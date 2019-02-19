
from params import *
import base64


def clear_topic(topic=TOPIC):
    """Util function to clear topic.
    :param topic: topic to delete.
    """
    os.system("/usr/local/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic {}".format(topic))


def set_topic(topic=TOPIC, partitions=SET_PARTITIONS):
    """Util function to set topic.
    :param topic: topic to delete.
    :param partitions: set partitions.
    """
    # SETTING UP TOPIC WITH DESIRED PARTITIONS
    init_cmd = "/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 " \
               "--replication-factor 3 --partitions {} --topic {}".format(partitions, topic)

    print("\n", init_cmd, "\n")
    os.system(init_cmd)

def np_to_json(obj, prefix_name=""):
    """Serialize numpy.ndarray obj
    :param prefix_name: unique name for this array.
    :param obj: numpy.ndarray"""
    return {"{}_frame".format(prefix_name): base64.b64encode(obj.tostring()).decode("utf-8"),
            "{}_dtype".format(prefix_name): obj.dtype.str,
            "{}_shape".format(prefix_name): obj.shape}