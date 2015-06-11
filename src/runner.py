import datetime
import os
import subprocess
import kafka
import prod_realdata

KAFKABIN=os.path.join('/', 'usr', 'local', 'kafka', 'bin')

def main():
    # TODO setup logger
 
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


def create_kafka_client():
    client = kafka.client.KafkaClient("localhost:9092")
    return client


def create_topic(topic=None, partitions=1, replication_factor=3):
    # create topic name if does not yet exist
    if topic is None:
        topic = "topic-%s" % datetime.datetime.now().strftime("%Y%b%d-%H%M%S")
    
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
