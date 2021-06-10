from datetime import datetime
from elasticsearch import Elasticsearch

class Elastic:
  def __init__(self) -> None:
    self.elastic_search = Elasticsearch()
      
  def save(self, payload):
    res = self.elastic_search.index(index="analytics", id=1, body=payload)