from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import json

class Queue:
  def __init__(self, process_text, log_textset_error) -> None:
    self.process_text = process_text
    self.log_textset_error = log_textset_error
    self.running = True
    conf = {
      'bootstrap.servers': 'localhost:9092',
      'group.id': "textlooker",
      'enable.auto.commit': False,
      'auto.offset.reset': 'earliest'
    }

    self.consumer = Consumer(conf)

  def wait_for_connection(self):
    self.consumer.subscribe(['textlooker'])

  def run(self):
    try:
      self.wait_for_connection()

      while self.running:
        message = self.consumer.poll(timeout=1.0)
        if message is None: continue

        if message.error():
          if message.error().code() == KafkaError._PARTITION_EOF:
            # End of partition event
            sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                    (message.topic(), message.partition(), message.offset()))
          elif message.error():
            raise KafkaException(message.error())
        else:
          try:
            text = json.loads(message.value())
            self.process_text(text)
            raise Exception("Yo boys")
          except Exception as exception:
            self.log_textset_error(exception, text)
          finally:
            self.consumer.commit()
    finally:
      # Close down consumer to commit final offsets.
      self.consumer.close()

  def shutdown(self):
    self.running = False