import json
import datetime

from .resource import Resource
from typing import Dict, List, Optional

class Job(Resource):
  @property
  def _version_name(self) -> str:
    return type(self).name_from_components(type(self).components_form_name(self._name)[:-1])
  
  @property
  def _job_name(self) -> str:
    return type(self).name_from_components(type(self).components_form_name(self._name)[:-2])

  @property
  def source(self) -> str:
    return self._get_key('source')

  @property
  def realm(self) -> str:
    return self._get_key('realm')

  @realm.setter
  def realm(self, value: str):
    return self._set_key('realm', value)

  @property
  def company(self) -> str:
    return self._get_key('company')

  @property
  def action(self) -> str:
    return self._get_key('action')

  @property
  def target(self) -> str:
    return self._get_key('target')

  @property
  def objective(self) -> str:
    return self._get_key('objective')

  @property
  def version(self) -> str:
    return self._get_key('version')

  @property
  def configuration(self) -> Optional[any]:
    json_value = self._get_key('configuration', optional=True)
    return json.loads(json_value) if json_value is not None else None

  @configuration.setter
  def configuration(self, value: Optional[any]):
    self._set_key('configuration', json.dumps(value) if value is not None else value)

  @property
  def created(self) -> datetime.datetime:
    raw_value = self._get_key('created', optional=True)
    return datetime.datetime.fromisoformat(raw_value) if raw_value else None

  @created.setter
  def created(self, value: datetime.datetime):
    self._set_key('created', value.isoformat())

  @property
  def ran(self) -> datetime.datetime:
    raw_value = self._get_key('ran', optional=True)
    return datetime.datetime.fromisoformat(raw_value) if raw_value else None

  @ran.setter
  def ran(self, value: datetime.datetime):
    self._set_key('ran', value.isoformat())

  @property
  def finished(self) -> datetime.datetime:
    raw_value = self._get_key('finished', optional=True)
    return datetime.datetime.fromisoformat(raw_value) if raw_value else None

  @finished.setter
  def finished(self, value: datetime.datetime):
    self._set_key('finished', value.isoformat())

  @property
  def result(self) -> Optional[str]:
    return self._get_key('result', optional=True)

  @result.setter
  def result(self, value: Optional[str]):
    self._set_key('result', value)

  @property
  def host(self) -> Optional[str]:
    return self._get_key('host', optional=True)

  @host.setter
  def host(self, value: Optional[str]):
    self._set_key('host', value)