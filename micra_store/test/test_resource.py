from ..resource import Resource
from ..job import Job
from .base import client

def test_resource():
  contents = {
    'a': 1,
  }
  r = Resource(name='test_resource', contents=contents)
  assert r._contents == contents
  assert r.a == 1
  try:
    r.b
    assert False
  except AttributeError:
    pass
  
def test_job(client):
  j = Job(name='test_job')
  j._get(client)
  j.realm = 'almacen_api'
  j._put(client, {'realm': 'almacen'})
