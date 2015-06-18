import kafka 
import os
import dbwrapper
import datetime
import parse

#### parameters #######
DEFAULT_TOPIC = "defaulttopic"
DEFAULT_NUM_MSG = int(1e8)
DEFAULT_LOG_INTERVAL = int(1e5)

#### kafka config ######
KAFKAHOST = "52.8.85.143:9092"


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
        print msgstr
        fmt = "Message seq={seq}, time={created_at}, producer={prodname}"
        r = parse.parse(fmt, msgstr)
        msg = Message(
            int(r["seq"]), 
            r["prodname"], 
            datetime.datetime.strptime(r["created_at"], cls.DATETIMEFORMAT))
        return msg


def producer(num_msg=DEFAULT_NUM_MSG, 
             topic=DEFAULT_TOPIC, 
             producer_name='default_producer',
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None):
    """ producer process sending messages to the brokers """
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
                     producer_name='default_producer',
                     log_interval=DEFAULT_LOG_INTERVAL,
                     exp_started_at=None):
    # TODO stream name
    python python.py my-first-stream
    
    # TODO prep messages

    # TODO send messages

    # TODO log

    # TODO merge with function above


# TODO expand for Kinesis
def consumer(topic=DEFAULT_TOPIC, 
             broker='kafka',
             consumer_name='default_consumer',
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


# TODO: some sort of runner
# TODO: runner must create topic
