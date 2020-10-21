import pdb
import traceback
import click
import atexit
import time

from uuid import uuid4
from typing import Dict, List, Optional
from .error import MicraQuit, MicraStopRetry
from functools import wraps
from queue import Queue
from moda.log import log

def uuid() -> str:
  return uuid4().hex

is_exiting: bool=False
@atexit.register
def stop_retry():
  global is_exiting
  is_exiting = True

def retry(enabled: bool=True, pdb_enabled: bool=False, queue: Optional[Queue]=None):
  def wrap(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
      while True:
        if is_exiting:
          break
        try:
          return f(*args, **kwargs)
        except (KeyboardInterrupt, SystemExit):
          raise
        except MicraStopRetry:
          raise
        except:
          if enabled:
            traceback.print_exc()
            if pdb_enabled:
              pdb.post_mortem()
              if not click.confirm('Continue', default=True):
                if queue is not None:
                  queue.put('q', block=False)
                raise MicraQuit
          else:
            raise
        log(f'Retrying {f.__name__}...')
        time.sleep(1)
    return wrapper
  return wrap
