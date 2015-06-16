from kafka import SimpleProducer, KafkaClient
import os
import dbwrapper

#### parameters #######
TOPIC = "test"
num_msg = int(1e8)
print_msg = int(1e5)



class Message():

    def __init__(self, seq, created_at, prodname):
        self.seq = seq
        self.created_at = created_at
        self.prodname = prodname

    def __str__(self):
        s = "Message seq={}, time={}, producer={}".format(
            self.seq, 
            self.created_at.strftime("%Y-%m-%d_%H:%M:%S.%f"),
            self.prodname)
        return s


# TODO
def producer():
    pass


# TODO
def consumer():
    pass



















def main():

    # setup sending mode: synchronously
    kafka = KafkaClient("localhost:9092")  # FIXME kafka ip (store in conf.py)
    producer = SimpleProducer(kafka, async=False)

    # bombard Kafka
    for i in range(num_msg):
        # send message to kafka	
        msg = "This is message %d" % i;   # FIXME message format 
        producer.send_messages(TOPIC, msg)  
        # print info to console
        if i % print_msg == 0 :
            print "Sent message #%d to Kafka" % i # FIXME send row to mysql db (in background / different thread)


if __name__ == "__main__":
    main()
