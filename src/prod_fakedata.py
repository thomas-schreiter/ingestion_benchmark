from kafka import SimpleProducer, KafkaClient

import os

#### parameters #######
TOPIC = "totallyfake"
num_msg = int(1e8)
print_msg = int(1e5)

def main():

    # setup sending mode: synchronously
    kafka = KafkaClient("localhost:9092")
    producer = SimpleProducer(kafka, async=False)

    # bombard Kafka
    for i in range(num_msg):
        # send message to kafka	
        msg = "This is message %d" % i;
        producer.send_messages(TOPIC, msg) 
        # print info to console
        if i % print_msg == 0 :
            print "Sent message #%d to Kafka" % i


if __name__ == "__main__":
    main()
