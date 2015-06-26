import kafka 
import os
import dbwrapper
import datetime
import parse
from boto import kinesis
import time
import multiprocessing
import argparse

""" General producer and consumer logic for benchmark. 

Use the producer() and consumer() functions 
"""


#### parameters #######
DEFAULT_TOPIC = "defaulttopic"
DEFAULT_NUM_MSG = int(1e8)
DEFAULT_LOG_INTERVAL = int(1e5)
DEFAULT_POOLSIZE = 1
DEFAULT_BULKSIZE = 500
DEFAULT_NUM_PARTITIONS = 1

#### kafka config ######
KAFKAHOST = "52.8.85.143:9092"

#### kinesis config ####
REGION = "us-west-1"  # in order to use us-west-1, modify the boto's endpoints.json file
SLEEP_IN_SEC = 0.1
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


class Logger():
    """ Log messages to DB """

    def __init__(self, prodcon_type, prodcon_name, broker, topic, log_interval, exp_started_at=None):
        broker = broker.lower()
        assert broker in ['kafka', 'kinesis']
        assert prodcon_type in ['producer', 'consumer']
        self.prodcon_type = prodcon_type
        if prodcon_name is None:
            prodcon_name = "default_%s_%s" % (broker, prodcon_type)
        self.prodcon_name = prodcon_name
        self.broker = broker
        self.topic = topic
        self.log_interval = log_interval
        if exp_started_at is None:
            exp_started_at = datetime.datetime.now()
        self.exp_started_at = exp_started_at
        
        self.prev_seq = None
        self.prev_time = None

    def log(self, msg, time):
        # only log first entry or every `log_interval` steps
        if self.prev_seq is not None and msg.seq - self.prev_seq < self.log_interval:
            return

        # calculate throughput
        if self.prev_seq is not None:
            diff_seq = self.prev_seq - msg.seq
            diff_time = self.prev_time - time
            throughput = diff_seq / diff_time.total_seconds()
        else: 
            throughput = None

        # calculate delay
        if self.prodcon_type == 'consumer':
            delay = time - msg.created_at
            
        # update fields
        self.prev_seq = msg.seq
        self.prev_time = time

        # store result in db
        if self.prodcon_type == 'producer':
            dbwrapper.store_prod_msg(
                seq=msg.seq, 
                topic=self.topic, 
                producer=self.prodcon_name,
                produced_at=time, 
                throughput=throughput, 
                exp_started_at=self.exp_started_at)
            print "Sent message #%d to %s" % (msg.seq, self.broker)
        elif self.prodcon_type == 'consumer':
            dbwrapper.store_con_msg(
                seq=msg.seq, 
                topic=self.topic, 
                consumer=self.prodcon_name, 
                broker=self.broker,
                consumed_at=time,
                throughput=throughput,
                delay=delay,
                exp_started_at=self.exp_started_at) 
            print "Read message #%d from %s" % (msg.seq, self.broker)
        else:
            assert False


class Broker():
    
    @classmethod
    def create(cls, brokertype, num_partitions, topic, *args, **kwargs):
        brokertype = brokertype.lower()
        topic = '%s_%dprod' % (brokertype, num_partitions)
        if brokertype == 'kafka':
            return Kafka(num_partitions, topic, *args, **kwargs)
        elif brokertype == 'kinesis':
            return Kinesis(num_partitions, topic, *args, **kwargs)
        else:
            assert False


class Kafka(Broker):

    def __init__(self, num_partitions, topic, *args, **kwargs):
        
        self.con = kafka.KafkaClient(KAFKAHOST)
        self.client = kafka.SimpleProducer(self.con, async=False)
        self.topic = topic
        print "Set topic to %s" % self.topic

    def send_message(self, msg):
        self.client.send_messages(self.topic, str(msg))

    def consume_forever(self, logger): 
        """ consumer process receiving messages from the brokers """
        # get Kafka connection
        consumer_group='default_group'
        self.consumer = kafka.SimpleConsumer(self.con, consumer_group, self.topic)

 	# TODO check out self.con.get_messages(count=1000) to send messages in bulk
        # read from Kafka
        for raw in self.consumer:
            consumed_at = datetime.datetime.now()
            msg = Message.from_string(raw.message.value)

            # log
            logger.log(msg, consumed_at)


class Kinesis(Broker):

    def __init__(self, num_partition, topic, *args, **kwargs):
        self.brokertype = "kinesis"
        self.con = kinesis.connect_to_region(REGION)
        self.num_shards = num_partitions
        try: 
            self.bulksize = kwargs["bulksize"]
        except:
            self.bulksize = 1
        self.msg_bulk = []
        self.topic = topic
        self._create_stream()

    def _create_stream(self):
        try:
            # check whether stream already exists
            desc = self.con.describe_stream(self.topic)
            actual_num_shards = len(desc["StreamDescription"]["Shards"])
            if self.num_shards is not None:
                assert actual_num_shards == self.num_shards, \
                    "Kinesis stream %s alredy exists with %d shards!!" % (self.topic, actual_num_shards)
            print "Kinesis stream %s already exists (%d shards)" % (self.topic, actual_num_shards)
            self.num_shards = actual_num_shards

        except kinesis.exceptions.ResourceInUseException:
            self.stream = self.con.create_stream(self.topic, self.num_shards)
            print "Creating Kinesis stream; wait 60 sec to let AWS create stream ...."
            time.sleep(60)  # wait until stream is ACTIVE, 60 seconds feels ok

    def send_message(self, msg):
        record = {'Data': str(msg), 'PartitionKey': str(hash(msg.seq))}
        self.msg_bulk.append(record)
        if len(self.msg_bulk) >= self.bulksize:    
            self.con.put_records(self.msg_bulk, self.topic)
            self.msg_bulk = []             

    def consume_forever(self, logger):
        """ consumer process receiving messages from the brokers """
        
        # listen to stream forever
        shard_id = 'shardId-000000000000'
        starting_sequence = "LATEST"  # or: TRIM_HORIZON to start from beginning
        shard_it = self.con.get_shard_iterator(self.topic, shard_id, starting_sequence)["ShardIterator"]
        while True:
            # read next message
            out = self.con.get_records(shard_it, limit=1)
            shard_it = out["NextShardIterator"]
            consumed_at = datetime.datetime.now()
            try:
                raw = out["Records"][0]["Data"]
            except IndexError:
                raw = None

            # log
            if (raw is not None):
                msg = Message.from_string(raw)
                logger.log(msg, consumed_at)  

            # wait a bit before requesting next chunk, otherwise AWS will cut it off
            time.sleep(SLEEP_IN_SEC)


def producer(brokertype,
             num_msg=DEFAULT_NUM_MSG, 
             producer_name=None,
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None,
             num_partitions=1,
	         bulksize=None,
             num_producers=1):
    """ api for general producer """
    # initialize broker and logger
    broker = Broker.create(brokertype, num_partitions, num_producers, bulksize=bulksize)
    logger = Logger('producer', producer_name, brokertype, broker.topic, log_interval, exp_started_at=None)
    
    # bombard the broker with messages
    for seq in range(num_msg):
        msg = Message(seq, producer_name)
        broker.send_message(msg)
        logger.log(msg, msg.created_at)


def consumer(brokertype,
             consumer_name=None,
             consumer_group='default_group',
             log_interval=DEFAULT_LOG_INTERVAL,
             exp_started_at=None,
             num_partitions=1,
             bulksize=1,
             num_producers=1):
    """ api for general consumer """
    # initialize broker and logger
    broker = Broker.create(brokertype, num_partitions, num_producers, bulksize=bulksize)
    logger = Logger('consumer', consumer_name, brokertype, broker.topic, log_interval, exp_started_at=None)
   
    # comsumer logic is quite different between the brokers 
    # logging is inside the consume_forever methods
    broker.consume_forever(logger)


if __name__ == '__main__':
    # argparse
    parser = argparse.ArgumentParser(description='Producer/Consumer for the benchmark.')
    parser.add_argument('prod_or_con', choices=['producer', 'consumer'], 
        help='Choose "producer" or "consumer".')
    parser.add_argument('brokertype', choices=['kafka', 'kinesis'],
        help='Brokertype')
    parser.add_argument('--num_msg', '-m', type=int, default=DEFAULT_NUM_MSG,
        help='number of total messages')
    parser.add_argument('--log_interval', '-l', type=int, default=DEFAULT_LOG_INTERVAL,
        help='interval [in #msg] after which a throughput is logged to the database')
    parser.add_argument('--num_partitions', '-s', type=int, default=DEFAULT_NUM_PARTITIONS,
        help='number of partitions/shards')
    parser.add_argument('--bulksize', '-b', type=int, default=DEFAULT_BULKSIZE,
        help='number of messages that are sent in bulk to Kinesis')
    parser.add_argument('--poolsize', '-p', type=int, default=DEFAULT_POOLSIZE,
        help='number of producers/consumers that work in parallel')

    args = parser.parse_args()
    print args

    def start_producers(instance_id): 
        print "Started producer %s %d" % (args.brokertype, instance_id)
        producer(brokertype=args.brokertype, 
                 num_msg=args.num_msg/args.poolsize,
                 log_interval=args.log_interval/args.poolsize,
                 exp_started_at=datetime.datetime.now(),
                 num_partitions=args.num_partitions,
                 bulksize=args.bulksize,
                 producer_name="%s_prod_%d" % (args.brokertype, instance_id),
                 num_producers=args.poolsize)
    
    def start_consumer(instance_id): 
        print "Started consumer %s %d" % (args.brokertype, instance_id)
        consumer(brokertype=args.brokertype, 
                 log_interval=args.log_interval/args.poolsize,
                 exp_started_at=datetime.datetime.now(),
                 num_partitions=args.num_partitions,
                 bulksize=args.bulksize,
                 producer_name="%s_prod_%d" % (args.brokertype, instance_id),
                 num_producers=args.poolsize)

    pool = multiprocessing.Pool(args.poolsize)
    if args.prod_or_con == 'producer':
        start_instances = start_producers
    elif args.prod_or_con == 'consumer':
        start_instances = start_consumers
    else: 
        assert False
    pool.map(start_instances, range(args.poolsize))



