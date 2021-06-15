import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from elastic import Elastic

class TextProcessor:
  def __init__(self) -> None:
    spacy.prefer_gpu()
    self.nlp = spacy.load("en_core_web_sm")
    self.elastic = Elastic()

  def tokens(self, doc):
    result = []
    for token in doc:
      lexeme = self.nlp.vocab[token.text]
      if not lexeme.is_stop and not lexeme.is_punct: result.append(token.text)
    return result
  
  def process(self, text):
    doc = self.nlp(text['content'])
    people = list(set([entity.text for entity in doc.ents if entity.label_ == 'PERSON']))
    gpe = list(set([entity.text for entity in doc.ents if entity.label_ == 'GPE']))
    tokens = self.tokens(doc)
    id = text['id']
    source_id = text['source_id']
    content = text['content']
    author = text['author']
    date = text.get('date')
    created_at = text['created_at']
    updated_at = text['updated_at']
    deleted_at = text['deleted_at']


    self.save(id, content, author, date, people, gpe, tokens, source_id, created_at, updated_at, deleted_at)

  def save(self, id, content, author, date, people, gpe, tokens, source_id, created_at, updated_at, deleted_at):
    payload = {
      'content': content,
      'author': author,
      'people': people,
      'gpe': gpe,
      'tokens': tokens,
      'source_id': source_id,
      'created_at': created_at,
      'updated_at': updated_at,
      'deleted_at': deleted_at
    }
    if date: payload['date'] = date

    self.elastic.save(payload, id)
