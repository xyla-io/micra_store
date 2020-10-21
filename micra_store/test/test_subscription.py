import IPython

from .base import client, redis

def test_subscription(client: redis.Redis):
  channel_name = 'test_subscription'
  r = client
  assert r.publish(channel_name, 'x') == 0
  p = r.pubsub()
  p.subscribe(channel_name)
  assert r.publish(channel_name, 'y') == 1
  q = r.pubsub()
  q.subscribe(channel_name)
  assert r.publish(channel_name, 'z') == 2
  m = p.get_message()
  assert m['type'] == 'subscribe'
  assert m['data'] == 1
  m = p.get_message()
  assert m['type'] == 'message'
  assert m['data'] == b'y'
  p.unsubscribe(channel_name)
  assert r.publish(channel_name, 'w') == 1

  # IPython.terminal.embed.InteractiveShellEmbed().mainloop()