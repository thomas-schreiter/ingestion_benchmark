import kafka 
import os
import dbwrapper
import datetime
import parse
from boto import kinesis
import time


#### parameters #######
DEFAULT_TOPIC = "defaulttopic"
DEFAULT_NUM_MSG = int(1e8)
DEFAULT_LOG_INTERVAL = int(1e5)

#### kafka config ######
KAFKAHOST = "52.8.85.143:9092"

#### kinesis config ####
REGION = "us-west-2"  # for some reason, us-west-1 doesn't work
SLEEP_IN_SEC = 0.2
NUM_SHARDS = 1


class Message():
    """ Messages used in benchmarking"""
    DATETIMEFORMAT = "%Y-%m-%d_%H:%M:%S.%f"

    def __init__(self, seq, prodname, created_at=None):
        self.seq = seq
        self.prodname = prodname
        if created_at is None:
            created_at = datetime.datetime.now()
        self.created_at = created_at

    def __str__(self):
        s = "Message seq={}, time={}, producer={}".format(
            self.seq, 
            self.created_at.strftime(self.DATETIMEFORMAT),
            self.prodname)
        return s
    
    @classmethod
    def from_string(cls, msgstr):
        fmt = "Message seq={seq}, time={created_at}, producer={prodname}"
        r = parse.parse(fmt, msgstr)
        msg = Message(
            int(r["seq"]), 
            r["prodname"], 
            datetime.datetime.strptime(r["created_at"], cls.DATETIMEFORMAT))
        return msg


def producer_kafka(num_msg=DEFAULT_NUM_MSG, 
                   topic=DEFAULT_TOPIC, 
                   producer_name='default_kafka_producer',
                   log_interval=DEFAULT_LOG_INTERVAL,
                   exp_started_at=None):
    """ producer process sending messages to the brokers """
    # FIXME exp_started_at should be set by runner
    if exp_started_at is None:
        exp_started_at = datetime.datetime.now()

    # get Kafka connection
    kafka_client = kafka.KafkaClient(KAFKAHOST) 
    producer = kafka.SimpleProducer(kafka_client, async=False)  # FIXME sync vs. async?

    # bombard Kafka
    for seq in range(num_msg):
        # send message to kafka
        msg = Message(seq, producer_name)
        producer.send_messages(topic, str(msg))  

        # TODO kinesis and other brokers here (that could be done with an observer pattern)

        # log
        if seq % log_interval == 0 :
            dbwrapper.store_prod_msg(
                msg.seq, topic, producer_name,
                msg.created_at, exp_started_at)  # FIXME run as background process
            print "Sent message #%d to Kafka" % seq


def producer_kinesis(num_msg=DEFAULT_NUM_MSG, 
                     topic=DEFAULT_TOPIC, 
                     producer_name='default_kinesis_producer',
                     log_interval=DEFAULT_LOG_INTERVAL,
                     exp_started_at=None):
    # FIXME exp_started_at should be set by runner
    if exp_started_at is None:
        exp_started_at = datetime.datetime.now()

    # create connection to Kinesis and stream
    kin = kinesis.connect_to_region(REGION)
    try: 
        stream = kin.create_stream(topic, NUM_SHARDS)
        # TODO wait until stream is ACTIVE
    except kinesis.exceptions.ResourceInUseException:
        pass  # stream already exists, do nothing
    
    for seq in range(num_msg):
        # send message to Kinesis
        msg = Message(seq, producer_name)
        kin.put_record(topic, str(msg), "partition_key")
        # TODO log
        if seq % log_interval == 0 :
            dbwrapper.store_prod_msg(
                msg.seq, topic, producer_name,
                msg.created_at, exp_started_at)  # FIXME run as background process
            print "Sent message #%d to Kinesis" % seq


def consumer(topic=DEFAULT_TOPIC, 
             broker='kafka',
             consumer_name='default_kafka_consumer',
             consumer_group='default_group',
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None):
    """ consumer process receiving messages from the brokers """
    # get Kafka connection
    kafka_client = kafka.KafkaClient(KAFKAHOST) 
    consumer = kafka.SimpleConsumer(kafka_client, consumer_group, topic)

    # FIXME exp_started_at should be set by runner
    if exp_started_at is None:
        exp_started_at = datetime.datetime.now()

    # read from Kafka
    for raw in consumer:
        consumed_at = datetime.datetime.now()
        msg = Message.from_string(raw.message.value)

        # log
        if msg.seq % log_interval == 0 :
            dbwrapper.store_con_msg(
                msg.seq, topic, consumer_name, broker,
                consumed_at, exp_started_at)  # FIXME run as background process
            print "Read message #%d from Kafka" % msg.seq


def consumer_kinesis(topic=DEFAULT_TOPIC, 
             broker='kinesis',
             consumer_name='default_kinesis_consumer',
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None):
    """ consumer process receiving messages from the brokers """
    # get Kafka connection
    kin = kinesis.connect_to_region(REGION)

    # FIXME exp_started_at should be set by runner
    if exp_started_at is None:
        exp_started_at = datetime.datetime.now()

    # listen to stream forever
    shard_id = 'shardId-000000000000'
    starting_sequence = "LATEST"  # or: TRIM_HORIZON to start from beginning
    shard_it = kin.get_shard_iterator(topic, shard_id, starting_sequence)["ShardIterator"]
    while True:
        # read next message
        out = kin.get_records(shard_it, limit=1)
        shard_it = out["NextShardIterator"]
        consumed_at = datetime.datetime.now()
        try:
            raw = out["Records"][0]["Data"]
        except IndexError:
            raw = None

        # log
        if (raw is not None):
            msg = Message.from_string(raw)
            if msg.seq % log_interval == 0:
                dbwrapper.store_con_msg(
                    msg.seq, topic, consumer_name, broker,
                    consumed_at, exp_started_at)  # FIXME run as background process
                print "Read message #%d from Kinesis" % msg.seq

        # wait a bit before requesting next chunk, otherwise AWS will cut it off
        time.sleep(SLEEP_IN_SEC)


# TODO: some sort of runner
# TODO: runner must create topic
