from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class Elastic:
  def __init__(self) -> None:
    self.elastic_search = Elasticsearch()
      
  def save(self, payload_generator, text_set):
    actions = list(payload_generator(text_set))
    res = bulk(self.elastic_search, actions)

  def log_text_error(self, exception, text):
    doc = {
      'message': str(exception),
      'exception_type': str(type(exception)),
      'text': str(text)
    }

    self.elastic_search.index(index="text-error", body=doc)

  def log_textset_error(self, exception, textset):
    doc = {
      'message': str(exception),
      'exception_type': str(type(exception)),
      'text': str(textset)
    }

    self.elastic_search.index(index="textset-error", body=doc)