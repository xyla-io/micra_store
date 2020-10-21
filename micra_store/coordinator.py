from __future__ import annotations

import os
import sys
import threading
import time
import click
import pdb
import json
import shlex
import atexit
import pandas as pd

from enum import Enum
from redis import Redis
from typing import Dict, Optional, List, Callable
from gevent.socket import wait_read
from .base import retry
from .error import MicraInputTimeout, MicraSubprocessEnded, MicraQuit
from .structure import Element, ContentType, Structure, micra_content_types, micra_structures
from .command_base import Command
from queue import Queue, Empty, Full
from moda.user import MenuOption, UserInteractor
from moda.style import Styled, CustomStyled, Format, Styleds
from moda.log import log
from moda.process import spawn_process

class Listener:
  _runner: Optional[Callable[[], None]]
  _cleaner: Optional[Callable[[], None]]
  _stopper: Optional[Callable[[], bool]]
  _name: Optional[str]
  input_queue: Queue
  output_queue: Queue
  info: Dict[str, any]

  def __init__(self, runner: Optional[Callable[[], None]]=None, cleaner: Optional[Callable[[], None]]=None, stopper: Optional[Callable[[], bool]]=None, info: Dict[str, any]={}, name: Optional[str]=None):
    self._runner = runner
    self._cleaner = cleaner
    self._stopper = stopper
    self._name = name
    self.input_queue = Queue()
    self.output_queue = Queue()
    self.info = {**info}

  @property
  def runner(self) -> Callable[[], None]:
    if self._runner is None:
      return lambda : None
    return self._runner

  @property
  def name(self) -> str:
    return self._name if self._name is not None else self.runner.__name__

  def stop(self) -> bool:
    return self._stopper() if self._stopper else False

  def clean(self):
    if self._cleaner:
      self._cleaner()

  def get_info(self) -> Dict[str, any]:
    try:
      for _ in range(0, self.output_queue.qsize()):
        info = self.output_queue.get_nowait()
        self.output_queue.task_done()
        self.info.update(info)
    except Empty:
      pass
    return self.info

class Coordinator:
  class MonitorOption(MenuOption):
    status = 's'
    list_types = 'lt'
    list_structrues = 'ls'
    view_content = 'v'
    quit = 'q'

    @property
    def option_text(self) -> str:
      if self is Coordinator.MonitorOption.status:
        return 'Status (s)'
      elif self is Coordinator.MonitorOption.list_types:
        return 'List Types (lt)'
      elif self is Coordinator.MonitorOption.list_structrues:
        return 'List Structures (ls)'
      elif self is Coordinator.MonitorOption.view_content:
        return 'View Content (v)'
      elif self is Coordinator.MonitorOption.quit:
        return 'Quit (q)'

    @property
    def styled(self) -> Styled:
      if self is Coordinator.MonitorOption.status:
        style = Format().green()
      elif self is Coordinator.MonitorOption.quit:
        style = Format().red()
      else:
        style = Format().blue()
      return CustomStyled(text=self.option_text, style=style)
  
  redis: Optional[Redis] = None
  config: Dict[str, any]
  listeners: Dict[Listener, threading.Thread]
  queue: Queue
  pdb_enabled: bool
  dry_run: bool
  should_listen: bool
  should_define: bool
  user: UserInteractor
  running: bool
  subprocesses: Dict[int, str]
  messages: Dict[str, str]
  commands_to_run: List[str]

  def __init__(self, config: Dict[str, any], pdb_enabled: bool=False, dry_run: bool=False, should_listen: bool=True, should_define: bool=True, interactive: bool=True, user: Optional[UserInteractor]=None):
    self.config = config
    self.listeners = {}
    self.queue = Queue()
    self.pdb_enabled = pdb_enabled
    self.dry_run = dry_run
    self.should_listen = should_listen
    self.should_define = should_define
    self.user = user if user is not None else UserInteractor()
    self.user.interactive = interactive
    self.running = False
    self.subprocesses = {}
    self.messages = {}
    self.commands_to_run = []

  @classmethod
  def listener_status(cls, listener: Listener, thread: threading.Thread) -> str:
    info = listener.get_info()
    info_text = f' [{", ".join(f"{k}: {v}" for k, v in info.items())}]' if info else ''
    return f'Listening: {listener.name}{info_text} ({thread.ident}) {"alive" if thread.is_alive() else "dead"}'

  @classmethod
  def subprocess_status(cls, pid: int, command: str) -> str:
    status = 'alive'
    try:
      os.getpgid(pid)
    except ProcessLookupError:
      status = 'dead'
    return f'Subprocess: {command} ({pid}) {status}'

  @classmethod
  def subprocess_command(cls, run_args: List[str]) -> str:
    return ' '.join(shlex.quote(a) for a in run_args)

  @property
  def definitions(self) -> List[Element]:
    return []

  @property
  def commands(self) -> List[Command]:
    return []

  @property
  def listener_starters(self) -> Dict[str, Callable[[], None]]:
    return {
      'accept': lambda k: self.start_accept_commands(key=k),
    }

  @property
  def status_items(self) -> List[str]:
    status = []
    if self.running:
      run_states = list(filter(lambda s: s is not None, [
        'interactive' if self.user.interactive else 'non-interactive',
        'listening' if self.listeners else None,
        'dry-run' if self.dry_run else None,
      ]))
      status.append(f'Running: {", ".join(run_states)}')
    status += [
      Coordinator.listener_status(listener=l, thread=self.listeners[l]) 
      for l in sorted(self.listeners.keys(), key=lambda l: l.name)
    ]
    status += [
      Coordinator.subprocess_status(pid=p, command=self.subprocesses[p])
      for p in sorted(self.subprocesses.keys())
    ]
    status += [
      self.messages[k]
      for k in sorted(self.messages.keys())
    ]
    return status
  
  def connect(self):
    self.redis = Redis(**self.config['redis'])

  def disconnect(self):
    self.redis = None

  def start_listener(self, listener: Listener, force: bool=False):
    if not self.should_listen and not force:
      return
    thread = threading.Thread(target=listener.runner)
    thread.setDaemon(True)
    thread.start()
    self.user.present_message(Format().cyan()(f'Starting listener {listener.name} ({thread.ident}).'))
    self.listeners[listener] = thread

  def stop_listener(self, thread_id: int):
    items = list(filter(lambda i: i[1].ident == thread_id, self.listeners.items()))
    assert len(items) <= 1
    for listener, thread in items:
      if not listener.stop():
        self.user.present_message(Format().red()(f'Cannot stop listener {listener.name} ({thread.ident}).'))

  def start_subprocess(self, run_args: List[str], eternal: bool=False):
    process, terminator = spawn_process(run_args=run_args)
    self.queue.put(f'subprocess {process.pid} set {shlex.quote(Coordinator.subprocess_command(run_args))}')
    while True:
      time.sleep(1)
      return_code = process.poll()
      if return_code is None:
        continue
      atexit.unregister(terminator)
      self.queue.put(f'subprocess {process.pid} clear')
      if return_code != 0 or eternal:
        raise MicraSubprocessEnded(pid=process.pid)

  def define_structure(self, element: Element, hash: Optional[str]=None):
    if hash is None:
      if isinstance(element, ContentType):
        hash = micra_content_types.key
      elif isinstance(element, Structure):
        hash = micra_structures.key
      else:
        raise TypeError(f'Cannot infer element hash from element type {type(element).__name__}')
    self.redis.hset(hash, element.identifier, json.dumps(element.ordered_structure_dict))

  def run_command(self, command: str):
    filtered_commands = list(filter(lambda c: c.matches_command(command=command), self.commands))
    if not filtered_commands:
      print(f'Invalid command: {command}')
      return
    if len(filtered_commands) > 1:
      print(f'Command input matches multiple commands: {command} ({", ".join(c.name for c in filtered_commands)})')
      return
    micra_command = filtered_commands[0]
    result = micra_command.run(command=command)
    if result is not None:
      print(result)

  def add_subprocess(self, pid: int, command: str):
    self.subprocesses[pid] = command
    self.user.present_message(Format().cyan()(f'Adding subprocess {command} ({pid}).'))

  def remove_subprocess(self, pid: int):
    if pid in self.subprocesses:
      del self.subprocesses[pid]

  def add_message(self, key: str, message: str):
    self.messages[key] = message

  def remove_message(self, key: str):
    del self.messages[key]

  def start_accept_commands(self, key: str):
    r = self.redis
    queue = self.queue

    @retry(pdb_enabled=self.pdb_enabled, queue=queue)
    def accept_commands():
      while True:
        queue.join()
        command = r.brpop(key)[1]
        queue.put(command)

    self.start_listener(Listener(runner=accept_commands, info={'key': key}))

  def update_listeners(self) -> bool:
    updated = False
    for listener, thread in list(self.listeners.items()):
      if not thread.is_alive():
        self.user.present_message(Format().cyan()(f'Listener {thread.ident} ended.'))
        try:
          listener.clean()
        except (KeyboardInterrupt, SystemExit):
          raise
        except Exception as e:
          self.user.present_message(f'An error occurred while cleaning listener {listener.name} ({thread.ident})', error=e)
        del self.listeners[listener]
        updated = True
    return updated

  def update_subprocesses(self) -> bool:
    updated = False
    for pid in sorted(self.subprocesses.keys()):
      try:
        os.getpgid(pid)
      except ProcessLookupError:
        self.user.present_message(Format().cyan()(f'Subprocess {pid} ended.'))
        del self.subprocesses[pid]
        updated = True
    return updated

  def try_command(self) -> Optional[bool]:
    command = None
    if self.commands_to_run:
      command = self.commands_to_run.pop(0)
      from_queue = False
    elif self.listeners:
      try:
        command = self.queue.get(block=not self.user.interactive)
        from_queue = True
      except Empty:
        pass
    elif not self.subprocesses and not self.user.interactive:
      self.user.present_message(Format().yellow()('Nothing to do.'))
      return False
    if command is None:
      return None
    try:
      self.run_command(command=command)
    except (KeyboardInterrupt, SystemExit):
      raise
    except MicraQuit:
      return False
    except Exception as e:
      self.user.present_message(message=f'Error running command: {command}', error=e)
      if self.pdb_enabled:
        pdb.post_mortem()
    if from_queue:
      self.queue.task_done()
    return True

  def start(self):
    assert not self.running

    if self.should_define:
      for definition in self.definitions:
        self.user.present_message(Format().blue()(f'Defining {definition.identifier}'))
        self.define_structure(element=definition)

    self.running = True
    should_print_menu = True
    if self.user.interactive:
      self.user.present_message(Format().green()('Starting in interactive mode.'))
    next_update = time.time() + 1
    while True:
      if next_update < time.time():
        should_print_menu = self.update_listeners() or should_print_menu
        should_print_menu = self.update_subprocesses() or should_print_menu
        next_update = time.time() + 1

      command_result = self.try_command()
      sys.stdout.flush()
      sys.stderr.flush()
      if command_result is True:
        should_print_menu = True
      elif command_result is False:
        break
      if self.user.interactive:
        try:
          if should_print_menu:
            self.user.present_message('\n'.join(c.display_styled.styled for c in self.commands if c.can_run))
            print('µ—>', end=' ')
            sys.stdout.flush()
          wait_read(sys.stdin.fileno(), timeout=0.01, timeout_exc=MicraInputTimeout)
          user_command = sys.stdin.readline().strip()
          if user_command:
            self.commands_to_run.append(user_command)
        except MicraInputTimeout:
          should_print_menu = False

    self.running = False
