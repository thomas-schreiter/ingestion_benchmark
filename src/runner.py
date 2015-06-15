import datetime
import os
import subprocess
import kafka
import prod_realdata
import logging


KAFKABIN = os.path.join('/', 'usr', 'local', 'kafka', 'bin')
LOGDIR = os.path.join('..', 'log')
starttimestr = '' 

def main():
    global starttimestr  # is this bad style?     
    starttimestr = datetime.datetime.now().strftime("%Y%b%d-%H%M%S")
    # setup logger

    start_logger()
 
    # create kafka client
    client = create_kafka_client()

    # create unique kafka topic
    topic = create_topic()

    # fire up producer incl. TODO logging
    prod_realdata.main()

    # TODO fire up consumer incl. logging (is that necessary?) 

    # TODO analyze logs to calculate metrics and write result to DB 

    # TODO print metrics on terminal

    # TODO remove topic from kafka (if that's possible)


def start_logger():
    if not os.path.exists(LOGDIR):
        os.makedirs(LOGDIR)
    logging.basicConfig(filename=os.path.join(LOGDIR, '%s.log' % starttimestr),
                        filemode='w',
                        level=logging.DEBUG,
                        format='%(levelname)s:%(message)s')
    logging.info("Logging started at %s." % datetime.datetime.now())    


def create_kafka_client():
    client = kafka.client.KafkaClient("localhost:9092")
    return client


def create_topic(topic=None, partitions=1, replication_factor=3):
    # create topic name if does not yet exist
    if topic is None:
        topic = "topic-%s" % starttimestr  
    # create topic in kafka
    subprocess.call([os.path.join(KAFKABIN, 'kafka-topics.sh'),
        '--create',  
        '--zookeeper', 'localhost:2181',
	    '--topic', str(topic), 
        '--partitions', str(partitions),
        '--replication-factor', str(replication_factor)])

    return topic


if __name__ == "__main__":
    main()
