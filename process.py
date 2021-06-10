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
    people = [entity.text for entity in doc.ents if entity.label_ == 'PERSON']
    gpe = [entity.text for entity in doc.ents if entity.label_ == 'GPE']
    tokens = self.tokens(doc)
    id = text['id']
    source_id = text['source_id']
    content = text['content']
    author = text['author']
    date = text['date']

    self.save(id, content, author, date, people, gpe, tokens, source_id)

  def save(self, id, content, author, date, people, gpe, tokens, source_id):
    payload = {
      'id': id,
      'content': content,
      'author': author,
      'date': date,
      'people': people,
      'gpe': gpe,
      'tokens': tokens,
      'source_id': source_id
    }

    self.elastic.save(payload)
