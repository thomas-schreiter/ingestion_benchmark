from boto import kinesis
import testdata
import json
import time


def get_connection():
    kin = kinesis.connect_to_region("us-west-2")
    
    try:
        stream = kin.create_stream("BotoDemo", 1)
    except kinesis.exceptions.ResourceInUseException:
        pass  # stream already exists, do nothing

    print kin.describe_stream("BotoDemo")
    return kin


class Users(testdata.DictFactory):
    firstName = testdata.FakeDataFactory('firstName')
    lastname = testdata.FakeDataFactory('lastName')
    age = testdata.RandomInteger(10, 30)
    gender = testdata.RandomSelection(['female', 'male'])


def producer(kin=None):
    if kin is None:
        kin = get_connection()
    for user in Users().generate(50):
        print user
        kin.put_record("BotoDemo", json.dumps(user), "partionkey")


def consumer(kin=None):
    if kin is None:
        kin = get_connection()
    shard_id = 'shardId-000000000000'
    shard_it = kin.get_shard_iterator("BotoDemo", shard_id, "LATEST")["ShardIterator"]
    while True:
        out = kin.get_records(shard_it, limit=2)
        shard_it = out["NextShardIterator"]
        print out
        time.sleep(0.2)
