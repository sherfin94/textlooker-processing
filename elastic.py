from datetime import datetime
from elasticsearch import Elasticsearch

class Elastic:
  def __init__(self) -> None:
    self.elastic_search = Elasticsearch()
      
  def save(self, payload, id):
    res = self.elastic_search.index(index="analytics", id=id, body=payload)
    print(res)