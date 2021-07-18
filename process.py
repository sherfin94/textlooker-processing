import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from elastic import Elastic

class TextProcessor:
  def __init__(self) -> None:
    # spacy.prefer_gpu()
    self.nlp = spacy.load("en_core_web_sm")
    self.entity_types = ['PERSON', 'NORP', 'FAC', 'ORG', 'GPE', 'LOC', 'PRODUCT', 'EVENT', 'WORK_OF_ART', 'LAW', 'LANGUAGE', 'DATE_TOKENS', 'TIME', 'PERCENT', 'MONEY', 'QUANTITY', 'ORDINAL', 'CARDINAL']
    self.elastic = Elastic()

  def tokens(self, doc):
    result = []
    for token in doc:
      lexeme = self.nlp.vocab[token.text]
      if not lexeme.is_stop and not lexeme.is_punct and lexeme.text != '�' : result.append(token.text)
    return result
  
  def generate_entities(self, doc):
    entities = {}
    for entity in doc.ents:
      if entity.label_ in self.entity_types:
        entities[entity.label_] = entity.text
    
    for entity_type in self.entity_types:
      if entity_type not in entities:
        entities[entity_type] = []

    return entities


  def process(self, text_set):
    self.elastic.save(self.payload_generator, text_set)

  def payload_generator(self, text_set):
      contents = [text['content'] for text in text_set['text_set']]
      docs = list(self.nlp.pipe(contents))

      for index, text in enumerate(text_set['text_set']):
        try:
          doc = docs[index]
          tokens = self.tokens(doc)
          id = text['id']
          source_id = text['source_id']
          content = text['content']
          author = text['author']
          date = text['date']
          created_at = text['created_at']
          updated_at = text['updated_at']
          deleted_at = text['deleted_at']

          entities = self.generate_entities(doc)

          yield self.create_payload(id, content, author, date, tokens, source_id, created_at, updated_at, deleted_at, entities)
        except Exception as exception:
          self.elastic.log_text_error(exception, text)


  def create_payload(self, id, content, author, date, tokens, source_id, created_at, updated_at, deleted_at, entities):
    source = {
      'content': content,
      'author': author,
      'tokens': tokens,
      'source_id': source_id,
      'date': date,
      'created_at': created_at,
      'updated_at': updated_at,
      'deleted_at': deleted_at
    }
    source.update(entities)

    payload = {
      '_index':'analytics',
      '_op_type': 'index',
      '_source': source
    }

    return payload
