
from queue_client import Queue
from process import TextProcessor

if __name__ == '__main__':
  text_processor = TextProcessor()
  queue = Queue(text_processor.process)
  queue.run()
