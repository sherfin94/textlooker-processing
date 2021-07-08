from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

class Elastic:
  def __init__(self) -> None:
    self.elastic_search = Elasticsearch()
      
  def save(self, payload_generator, text_set):
    actions = list(payload_generator(text_set))
    res = bulk(self.elastic_search, actions)