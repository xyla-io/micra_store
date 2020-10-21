import json

from collections import OrderedDict
from ..structure import ordered_representation

def test_ordered_representation():
  d = {
    'a': [{'c': 1, 'b': 2, 'd': 'x'}, {'c': 0, 'b': 1, 'd': 'y'}],
    'e': 'f',
  }
  ordered_d = OrderedDict([
    ('a', [OrderedDict([('b', 2), ('c', 1), ('d', 'x')]), OrderedDict([('b', 1), ('c', 0), ('d', 'y')])]),
    ('e', 'f'),
  ])
  assert json.dumps(ordered_representation(d)) == json.dumps(ordered_d)
