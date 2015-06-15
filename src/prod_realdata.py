from kafka import SimpleProducer, KafkaClient

import os

#### parameters #######
# directory containing the raw data 
datadir = os.path.join("..", "data")
TOPIC = 'fun'

def main():

    # setup sending mode: synchronously
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka, async=False)

    # read each line of each file
    for filename in os.listdir(datadir):
        print "Processing file '%s'" % filename
        with open(os.path.join(datadir, filename)) as f:
            for row in f:
                # print "submitting message to Kafka: %s" % row  # FIXME logger here
                producer.send_messages(TOPIC, row)


if __name__ == "__main__":
    main()
