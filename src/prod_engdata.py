from kafka import SimpleProducer, KafkaClient
import logging
import os

#### parameters #######
# directory containing the raw data 
datadir = os.path.join("..", "data")
# vehicle detector station to keep (i.e. discard all other messages)
SENSOR_TO_KEEP = 402814 
TOPIC = "fun"

def main():

    # setup sending mode: synchronously
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka, async=False)

    # read each line of each file
    for filename in os.listdir(datadir):
        print "Processing file '%s'" % filename
        with open(os.path.join(datadir, filename)) as f:
            for row in f:
                # produce messages of specified sensor
                if str(SENSOR_TO_KEEP) in row:
                    logging.info("submitting message to Kafka: %s" % row)
                    producer.send_messages(TOPIC, row)


if __name__ == "__main__":
    main()
