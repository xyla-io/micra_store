import redis
import pytest

from environments import environment, set_environment
set_environment('development')

@pytest.fixture
def client() -> redis.Redis:
  yield redis.Redis(**environment['redis'])
