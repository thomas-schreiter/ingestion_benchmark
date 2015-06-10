from kafka import SimpleProducer, KafkaClient

# send message synchronously
kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

# send some messages
producer.send_messages("my-topic", "some message")
producer.send_messages("my-topic", "this method", "is variadic")


