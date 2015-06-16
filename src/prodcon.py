from kafka import SimpleProducer, KafkaClient
import os
import dbwrapper
import datetime

#### parameters #######
DEFAULT_TOPIC = "defaulttopic"
DEFAULT_NUM_MSG = int(1e8)
DEFAULT_LOG_INTERVAL = int(1e5)

#### kafka config ######
KAFKAHOST = "52.8.85.143:9092"


class Message():
    """ Messages used in benchmarking"""
    def __init__(self, seq, prodname):
        self.seq = seq
        self.prodname = prodname
        self.created_at = datetime.datetime.now()

    def __str__(self):
        s = "Message seq={}, time={}, producer={}".format(
            self.seq, 
            self.created_at.strftime("%Y-%m-%d_%H:%M:%S.%f"),
            self.prodname)
        return s


def producer(num_msg=DEFAULT_NUM_MSG, 
             topic=DEFAULT_TOPIC, 
             producer_name='default_producer',
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None):
    """ producer process sending messages to the brokers """
    # get Kafka connection
    kafka = KafkaClient(KAFKAHOST) 
    producer = SimpleProducer(kafka, async=False)  # FIXME sync vs. async?

    # bombard Kafka
    for i in range(num_msg):
        # send message to kafka
        msg = Message(i, producer_name)
        producer.send_messages(topic, str(msg))  

        # TODO kinesis and other brokers here (that could be done with an observer pattern)

        # log
        if i % log_interval == 0 :
            dbwrapper.store_prod_msg(
                msg.seq, topic, producer_name,
                msg.created_at, exp_started_at)  # FIXME run as background process
            print "Sent message #%d to Kafka" % i


# TODO
def consumer():
    pass

# TODO: some sort of runner
# TODO: runner must create topic
