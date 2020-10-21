from __future__ import annotations
import json
import pandas as pd

from enum import Enum
from redis import Redis
from redis.client import Pipeline
from typing import Dict, Set as SetType, OrderedDict as OrderedDictType, Union, List as ListType, Optional, Tuple
from collections import OrderedDict
from functools import reduce
from pprint import pformat
from ..resource import Resource
from moda.style import CustomStyled, Styleds, Format

def ordered_representation(representation: any) -> any:
  if isinstance(representation, dict):
    return OrderedDict(sorted({k: ordered_representation(v) for k, v in representation.items()}.items(), key=lambda t: t[0]))
  elif isinstance(representation, list) or isinstance(representation, tuple):
    return list(map(ordered_representation, representation))
  elif isinstance(representation, set):
    return list(sorted({ordered_representation(v) for v in representation}))
  else:
    return representation

class Definition:
  @classmethod
  def from_dict(cls, representation: Dict[str, any]) -> Definition:
    return cls(**representation)

  @property
  def structure_dict(self) -> Dict[str, any]:
    return {}

class Element(Definition):
  _identifier: str
  _title: str
  _description: str
  _tags: SetType[str]

  def __init__(self, identifier: str, title: str, description: str, tags: SetType[str]=set()):
    self._identifier = identifier
    self._title = title
    self._description = description
    self._tags = set().union(tags)

  @classmethod
  def from_dict(cls, representation: Dict[str, any]) -> Element:
    return super().from_dict(representation=representation)

  @property
  def identifier(self) -> str:
    return self._identifier

  @property
  def title(self) -> str:
    return self._title

  @property
  def description(self) -> str:
    return self._description

  @property
  def tags(self) -> SetType[str]:
    return self._tags

  @property
  def display_name(self) -> str:
    return f'{self.title}'

  @property
  def display_summary(self) -> str:
    return f'({self.identifier}): {self.description} [{", ".join(sorted(self.tags))}]'

  def display_metadata(self, redis: Union[Redis, Pipeline]) -> str:
    return ''

  def display_detail(self, redis: Union[Redis, Pipeline]) -> str:
    return ''

  @property
  def structure_dict(self) -> Dict[str, any]:
    return {
      'title': self.title,
      'identifier': self.identifier,
      'description': self.description,
      'tags': self.tags,
    }

  @property
  def ordered_structure_dict(self) -> OrderedDictType[str, any]:
    return ordered_representation(self.structure_dict)

class ContentConverter(Enum):
  string = 'string'
  dictionary = 'dictionary'
  json = 'json'
  json_object = 'json_object'
  resource = 'resource'

  @property
  def converts_collection(self) -> bool:
    if self is ContentConverter.resource:
      return True
    else:
      return False

  def convert_instance(self, serialization: any) -> any:
    if self is ContentConverter.string:
      return serialization
    elif self is ContentConverter.dictionary:
      return serialization
    elif self is ContentConverter.json or self is ContentConverter.json_object:
      return json.loads(serialization)
    elif self is ContentConverter.resource:
      return Resource(contents=serialization)

  def instance_dict(self, instance: any) -> Dict[str, any]:
    if self is ContentConverter.string or self is ContentConverter.json:
      return {'': instance}
    elif self is ContentConverter.dictionary:
      return instance
    elif self is ContentConverter.json_object:
      return instance
    elif self is ContentConverter.resource:
      return instance._contents

class ContentType(Element):
  _properties: Dict[str, str]
  _converter: ContentConverter

  def __init__(self, identifier: str, title: str, description: str, tags: SetType[str]=set(), properties: Dict[str, str]={}, converter: ContentConverter=ContentConverter.string):
    super().__init__(identifier=identifier, title=title, description=description, tags=tags)
    self._properties = {**properties}
    self._converter = converter

  @classmethod
  def from_dict(cls, representation: Dict[str, any]) -> ContentType:
    r = representation
    r['converter'] = ContentConverter(r['converter'])
    return super().from_dict(representation=r)

  @property
  def properties(self) -> Dict[str, str]:
    return self._properties

  @property
  def converter(self) -> ContentConverter:
    return self._converter

  @property
  def display_summary(self) -> str:
    properties_text = ', '.join(f'{p}: {d}' for p, d in self.properties.items())
    return f'{super().display_summary} ({properties_text}) {self.converter.value}'

  @property
  def structure_dict(self) -> Dict[str, any]:
    return {
      **super().structure_dict,
      'converter': self.converter.value,
    }

class StructureType(Enum):
  value = 'value'
  list = 'list'
  set = 'set'
  ordered_set = 'ordered_set'
  hash = 'hash'
  stream = 'stream'

  def get_metadata(self, key: str, redis: Union[Redis, Pipeline]) -> Dict[str, any]:
    if self is StructureType.list:
      return {'length': redis.llen(key)}
    elif self is StructureType.set:
      return {'length': redis.scard(key)}
    elif self is StructureType.ordered_set:
      return {'length': redis.zcard(key)}
    elif self is StructureType.hash:
      return {'length': redis.hlen(key)}
    elif self is StructureType.stream:
      return {'length': redis.xlen(key)}
    else:
      raise NotImplementedError()

  def get_content(self, key: str, redis: Union[Redis, Pipeline]) -> any:
    if self is StructureType.list:
      return redis.lrange(key, 0, -1)
    elif self is StructureType.set:
      return redis.smembers(key)
    elif self is StructureType.ordered_set:
      return redis.zrange(key, 0, -1, withscores=True)
    elif self is StructureType.hash:
      return redis.hgetall(key)
    elif self is StructureType.stream:
      return redis.xread({key: '0'})
    else:
      raise NotImplementedError()

  def convert_to_records(self, content: any, converter: ContentConverter) -> ListType[Dict[str, any]]:
    if converter.converts_collection:
      return [converter.instance_dict(converter.convert_instance(content))]
    elif self is StructureType.list:
      return [converter.instance_dict(converter.convert_instance(m)) for m in content]
    elif self is StructureType.set:
      return [converter.instance_dict(converter.convert_instance(m)) for m in content]
    elif self is StructureType.ordered_set:
      return [{'.ordered_set_score': s, **converter.instance_dict(converter.convert_instance(m))} for m, s in content]
    elif self is StructureType.hash:
      return [{'.hash_key': k, **converter.instance_dict(converter.convert_instance(m))} for k, m in content.items()]
    elif self is StructureType.stream:
      return [{'.stream_id': k, **converter.instance_dict(converter.convert_instance(d))} for k, d in content[0][1]]
    else:
      raise NotImplementedError()

class Join(Definition):
  structure: str
  select: ListType[str]
  key_on: ListType[str]
  on: Dict[str, str]
  prefix: str
  sort: ListType[Tuple[str, bool]]
  ranges: ListType[Tuple[Optional[int], Optional[int]]]

  def __init__(self, structure: str, select: ListType[str]=[], key_on: ListType[str]=[], on: Dict[str, str]={}, prefix: Optional[str]=None, sort: ListType[Tuple[str, bool]]=[], ranges: ListType[Tuple[int, int]]=[]):
    self.structure = structure
    self.select = [*select]
    self.key_on = [*key_on]
    self.on = {**on}
    self.prefix = prefix if prefix is not None else f'{self.structure}.'
    self.sort = sort
    self.ranges = ranges

  @classmethod
  def from_dict(cls, representation: Dict[str, any]) -> Element:
    r = representation
    r['sort'] = list(map(tuple, r['sort']))
    r['ranges'] = list(map(tuple, r['ranges']))
    return super().from_dict(representation=representation)

  @property
  def structure_dict(self) -> Dict[str, any]:
    return {
      'structure': self.structure,
      'select': self.select,
      'key_on': self.key_on,
      'on': self.on,
      'prefix': self.prefix,
      'sort': list(map(list, self.sort)),
      'ranges': list(map(list, self.ranges)),
    }
  
  def get_structure(self, redis: Union[Redis, Pipeline]) -> Structure:
    from .common_structures import micra_structures
    return Structure.from_dict(json.loads(redis.hget(micra_structures.key, self.structure)))

  def join(self, data_frame: pd.DataFrame, redis: Union[Redis, Pipeline]) -> pd.DataFrame:
    structure = self.get_structure(redis=redis)
    assert len(self.key_on) == len(structure.key_tokens)
    if self.key_on:
      keys_data_frame = data_frame.groupby(self.key_on).size().reset_index()
      join_data_frame = pd.DataFrame()
      for r in keys_data_frame.iterrows():
        join_data_frame = join_data_frame.append(structure.with_tokens(tokens=[r[1][t] for t in self.key_on]).get_data_frame(redis=redis))
    else:
      join_data_frame = structure.get_data_frame(redis=redis)
    for column in self.select:
      if column not in join_data_frame.columns:
        join_data_frame[column] = None
    select_data_frame = join_data_frame[self.select] if self.select else join_data_frame
    if self.on:
      select_data_frame = select_data_frame.add_prefix(self.prefix)
      on_items = self.on.items()
      left_on = [data_frame[o[0]] for o in on_items]
      right_on = [join_data_frame[o[1]] for o in on_items]
      joined = data_frame.merge(
        right=select_data_frame, 
        how='left', 
        left_on=left_on, 
        right_on=right_on
      )
      # drop the dummy column created by pandas for the merge
      joined.drop('key_0', axis=1, inplace=True)
    else:
      # pass sort=False to silence a pandas warning about future behavior
      joined = data_frame.append(join_data_frame, sort=False)

    if self.sort:
      sort_columns = [s[0] for s in self.sort]
      sort_ascending = [s[1] for s in self.sort]
      for column in sort_columns:
        if column not in joined.columns:
          joined[column] = None
      joined.sort_values(by=sort_columns, ascending=sort_ascending, inplace=True)
    joined.reset_index(inplace=True)
    if self.ranges:
      joined_length = len(joined)
      indices = reduce(lambda s, r : s.union(set(range(0 if r[0] is None else joined_length + r[0] if r[0] < 0 else r[0], joined_length if r[1] is None else joined_length + r[1] + 1 if r[1] < 0 else r[1] + 1))), self.ranges, set())
      joined = joined.iloc[list(sorted(indices))]
    return joined

class Structure(Element):
  _key: str
  _structure_type: StructureType
  _content_type: str
  _key_tokens: ListType[str]
  _joins: ListType[Join]

  def __init__(self, identifier: str, title: str, description: str, key: str, structure_type: StructureType, content_type: str, tags: SetType[str]=set(), key_tokens: ListType[str]=[], joins: ListType[Join]=[]):
    super().__init__(identifier=identifier, title=title, description=description, tags=tags)
    self._key = key
    self._structure_type = structure_type
    self._content_type = content_type
    self._key_tokens = [*key_tokens]
    self._joins = [*joins]

  @classmethod
  def from_dict(cls, representation: Dict[str, any]) -> Structure:
    r = representation
    r['structure_type'] = StructureType(r['structure_type'])
    r['joins'] = [Join.from_dict(j) for j in r['joins']]
    return super().from_dict(representation=r)

  @property
  def key(self) -> str:
    return self._key

  @property
  def structure_type(self) -> StructureType:
    return self._structure_type

  @property
  def content_type(self) -> ContentType:
    return self._content_type

  @property
  def key_tokens(self) -> ListType[str]:
    return self._key_tokens

  @property
  def joins(self) -> ListType[Join]:
    return self._joins

  @property
  def structure_dict(self) -> Dict[str, any]:
    return {
      **super().structure_dict,
      'key': self.key,
      'structure_type': self.structure_type.value,
      'content_type': self.content_type,
      'key_tokens': self.key_tokens,
      'joins': [j.structure_dict for j in self.joins],
    }

  def with_key(self, key: str) -> Structure:
    return type(self).from_dict(representation={**self.structure_dict, 'key': key, 'key_tokens': []})

  def with_tokens(self, tokens: ListType[str]) -> str:
    return self.with_key(key=self.key_from_tokens(tokens=tokens))

  def key_from_tokens(self, tokens: ListType[str]) -> str:
    assert len(tokens) == len(self.key_tokens)
    return self.key.format(*tokens)

  def get_content_type(self, redis: Union[Redis, Pipeline]) -> ContentType:
    from .common_structures import micra_content_types
    return ContentType.from_dict(json.loads(redis.hget(micra_content_types.key, self.content_type)))

  def get_metadata(self, redis: Union[Redis, Pipeline]) -> Dict[str, any]:
    assert not self.key_tokens
    return self.structure_type.get_metadata(key=self.key, redis=redis)

  def get_content(self, redis: Union[Redis, Pipeline]) -> any:
    assert not self.key_tokens
    return self.structure_type.get_content(key=self.key, redis=redis)

  def get_data_frame(self, redis: Union[Redis, Pipeline]) -> pd.DataFrame:
    content_type = self.get_content_type(redis=redis)
    df = pd.DataFrame()
    if self.key:
      try:
        content = self.get_content(redis=redis)
        records = self.structure_type.convert_to_records(content=content, converter=content_type.converter)
      except (KeyboardInterrupt, SystemExit):
        raise
      except Exception as e:
        records = [{
          '.error_context': 'content',
          '.error': repr(e),
        }]
      if records:
        df = pd.DataFrame(records).rename(lambda c: content_type.identifier if not c else c[1:] if c.startswith('.') else  f'{content_type.identifier}.{c}', axis='columns')
        df.insert(0, 'key', self.key)
    for index, join in enumerate(self.joins):
      try:
        df = join.join(data_frame=df, redis=redis)
      except (KeyboardInterrupt, SystemExit):
        raise
      except Exception as e:
        df = df.append([{
          'error_context': f'join:{index}',
          'error': repr(e),
        }])
    return df

  @property
  def display_summary(self) -> str:
    return f'{super().display_summary} {self.key} > {self.structure_type.value} > {self.content_type} + ({", ".join(j.structure for j in self.joins)})'

  def display_metadata(self, redis: Union[Redis, Pipeline]) -> str:
    if not self.key or self.key_tokens:
      return '()'
    metadata_text = ', '.join(f'{m}: {v}' for m, v in self.get_metadata(redis=redis).items())
    return f'({metadata_text})'

  def display_detail(self, redis: Union[Redis, Pipeline]) -> str:
    if not self.key:
      return 'No key'
    if self.key_tokens:
      return f'Token key {self.key_from_tokens(tokens=[f"{{{t}}}" for t in self.key_tokens])}'
    return pformat(self.get_content(redis=redis))

  def display_content(self, redis: Union[Redis, Pipeline]) -> str:
    description = Styleds(parts=[
      CustomStyled(self.display_name, Format().cyan()),
      CustomStyled(f' {self.display_summary}', Format().blue()),
      CustomStyled(f'\n{self.display_metadata(redis=redis)}', Format().cyan()),
    ])
    df = self.get_data_frame(redis=redis)
    with pd.option_context('display.max_rows', None, 'display.max_columns', df.shape[1]):
      return f'{description.styled}\n{df}'

class BaseStructure(Structure):
  default_structure_type: StructureType=None

  def __init__(self, identifier: str, title: str, description: str, key: str, content_type: str, tags: SetType[str]=set(), key_tokens: ListType[str]=[]):
    super().__init__(identifier=identifier, title=title, description=description, key=key, structure_type=type(self).default_structure_type, content_type=content_type, tags=tags, key_tokens=key_tokens)

class Value(BaseStructure):
  default_structure_type: StructureType=StructureType.value

class List(BaseStructure):
  default_structure_type: StructureType=StructureType.list

class Set(BaseStructure):
  default_structure_type: StructureType=StructureType.set

class Hash(BaseStructure):
  default_structure_type: StructureType=StructureType.hash

class Stream(BaseStructure):
  default_structure_type: StructureType=StructureType.stream

class OrderedSet(BaseStructure):
  default_structure_type: StructureType=StructureType.ordered_set