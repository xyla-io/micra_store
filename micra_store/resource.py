import re

from redis import Redis, WatchError
from typing import List, Dict, Optional

class Resource:
  _name: str=''
  _contents: Dict[str, any]={}
  _optional_attributes: List[str]=[]

  @classmethod
  def escaped_name_component(cls, component: str):
    return component.replace('\\', '\\ ').replace(':', '\\-')

  @classmethod
  def unescaped_name_component(cls, component: str):
    return component.replace('\\-', ':').replace('\\ ', '\\')

  @classmethod
  def name_from_components(cls, components: List[str]):
    return ':'.join(cls.escaped_name_component(c) for c in components)

  @classmethod
  def components_form_name(cls, name: str):
    return [cls.unescaped_name_component(c) for c in name.split(':')]

  def __init__(self, name: str='', contents: Dict[str, any]={}):
    self._name = name
    self._contents = {}
    self._contents.update(contents)

  def _get(self, redis: Redis):
    contents = redis.hgetall(self._name)
    self._contents = contents if contents is not None else {}
    return self

  def _put(self, redis: Optional[Redis]=None, check_map: Optional[Dict[str, str]]=None, pipe: Optional[any]=None) -> bool:
    if pipe is None:
      assert redis is not None
      pipe = redis.pipeline()
    if check_map is not None:
      pipe.watch(self._name)
      check_keys = list(check_map.keys())
      check_values = pipe.hmget(self._name, check_keys)
      for index, key in enumerate(check_keys):
        if check_values[index] != check_map[key]:
          return False
      pipe.multi()
    pipe.delete(self._name)
    pipe.hmset(self._name, self._contents)
    try:
      pipe.execute()
    except WatchError:
      return False
    return True

  def _get_key(self, key: str, optional: bool=False) -> Optional[any]:
    if key in self._contents:
      return self._contents[key]
    elif optional:
      return None
    else:
      raise ValueError(None)

  def _set_key(self, key: str, value: Optional[any], optional: bool=False):
    if value is None:
      if not optional:
        raise ValueError(None)
      else:
        del self._contents[key]
    else:
      self._contents[key] = value

  def __getattribute__(self, name):
    try:
      return object.__getattribute__(self, name)
    except AttributeError:
      value = self._get_key(key=name, optional=True)
      if value is not None or name in self._optional_attributes:
        return value
      raise

  def __setattr__(self, name, value):
    try:
      object.__getattribute__(self, name)
      object.__setattr__(self, name, value)
    except (AttributeError, ValueError):
      self._set_key(key=name, value=value, optional=name in self._optional_attributes)