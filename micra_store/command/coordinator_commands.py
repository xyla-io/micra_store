import click
import json
import re
import io
import os
import signal
import IPython
import pandas as pd

from ..command_base import CommandCategory, Command
from ..coordinator import Coordinator
from ..structure import Element, ContentType, Structure, micra_content_types, micra_structures
from ..error import MicraQuit
from moda.style import Styleds, CustomStyled, Format
from typing import List, Set, TypeVar, Generic, Callable
from enum import Enum
from redis import Redis
from pprint import pformat

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ElementTarget(Enum):
  content_type = 'types'
  structure = 'structures'

  def get_elements(self, redis: Redis) -> List[Element]:
    if self is ElementTarget.content_type:
      return [ContentType.from_dict(json.loads(v)) for v in redis.hgetall(micra_content_types.key).values()]
    elif self is ElementTarget.structure:
      return [Structure.from_dict(json.loads(v)) for v in redis.hgetall(micra_structures.key).values()]

class OutputFormat(Enum):
  string = 'string'
  pretty = 'pretty'
  json = 'json'
  csv = 'csv'
  dataframe = 'dataframe'

  def format(self, items: List[any], redis: Redis):
    items_are_structures = True
    for item in items:
      # TODO handle non-Structure items as necessary
      if not isinstance(item, Structure):
        items_are_structures = False
        break

    if self is OutputFormat.string:
      if items_are_structures:
        return '\n\n'.join(f'{s.identifier} {s.display_metadata(redis=redis)}\n{"—" * len(s.identifier)}\n{s.display_detail(redis=redis)}' for s in items)
      else:
        return '\n'.join(str(i) for i in items)
    elif self is OutputFormat.pretty:
      if items_are_structures:
        return '\n'.join(s.display_content(redis=redis) for s in items)
      else:
        return '\n\n'.join(pformat(item) for i in items)
    elif self is OutputFormat.json:
      if items_are_structures:
        # items = [
        #   {
        #     **json.loads(item.get_data_frame(redis=redis).to_json(orient='table')),
        #     'identifier': item.identifier,
        #   }
        #   for item in items
        # ]
        json_items = []
        for item in items:
          data_frame = item.get_data_frame(redis=redis)
          if 'level_0' in data_frame.columns:
            data_frame.drop(['level_0'], axis=1, inplace=True)
          json_items.append({
            **json.loads(data_frame.to_json(orient='table')),
            'identifier': item.identifier,
          })
      return json.dumps(json_items)
    elif self is OutputFormat.csv:
      if items_are_structures:
        df = pd.DataFrame()
        for structure in items:
          structure_df = structure.get_data_frame(redis=redis)
          structure_df['identifier'] = structure.identifier
          df = df.append(structure_df)
        # df = df.reindex(sorted(df.columns), axis=1)
      else:
        df = pd.DataFrame([{'item': i} for i in items])
      buf = io.StringIO()
      df.to_csv(buf)
      return buf.getvalue()
    elif self is OutputFormat.dataframe:
      if items_are_structures:
        dfs = {item.identifier: item.get_data_frame(redis=redis) for item in items}
      else:
        dfs = {
          'all': pd.DataFrame([{'item': item} for item in items])
        }
      local_ns = {'dfs': dfs}
      if len(dfs) == 1:
        local_ns['df'] = list(dfs.values())[0]
      console = IPython.terminal.embed.InteractiveShellEmbed()
      console.mainloop(local_ns=local_ns)
      return None

# ---------------------------------------------------------------------------
# Decorators
# ---------------------------------------------------------------------------

format_command_option = click.option('-f', '--format', 'format_value', type=click.Choice([f.value for f in OutputFormat]), default=OutputFormat.string.value)
publish_command_option = click.option('-p', '--publish', 'publish', type=str, multiple=True)
echo_command_option = click.option('-e', '--echo', 'should_echo', is_flag=True)

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

C = TypeVar(Coordinator)
class CoordinatorCommand(Generic[C], Command[C]):
  pass

class StartCommand(CoordinatorCommand[CoordinatorCommand]):
  _all_names: str

  def __init__(self, all_names: List[str], context: CoordinatorCommand):
    self._all_names = all_names
    super().__init__(context=context)

  @property
  def name(self) -> str:
    return self._all_names[0]

  @property
  def aliases(self) -> List[str]:
    return self._all_names[1:]

  @property
  def click_command(self) -> click.Command:
    @click.command(name=self.name)
    def start():
      self.context.start()
    return start

  @property
  def can_run(self) -> bool:
    return not self.context.running

class QuitCommand(CoordinatorCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.caution

  @property
  def name(self) -> str:
    return 'quit'

  @property
  def aliases(self) -> List[str]:
    return ['q']

  @property
  def click_command(self) -> click.Command:
    @click.command(name=self.name, help='Quit µ')
    def click_command():
      raise MicraQuit()

    return click_command

  @property
  def can_run(self) -> bool:
    return self.context.running

class OutputCommand(Generic[C], CoordinatorCommand[C]):
  @property
  def micra_decorators(self) -> List[Callable[[Callable[..., any]], Callable[..., any]]]:
    return [
      self.publish_command_output,
      self.format_command_output,
    ]

  @property
  def click_decorators(self) -> List[Callable[[Callable[..., any]], Callable[..., any]]]:
    return [
      publish_command_option,
      echo_command_option,
      format_command_option,
    ]

  def format_command_output(self, f: Callable[..., any]) -> Callable[..., any]:
    def wrapped(*args, format_value: str, **kwargs):
      format = OutputFormat(format_value)
      items = f(*args, **kwargs)
      if items is None:
        return ''
      return format.format(items=items, redis=self.context.redis)

    return wrapped

  def publish_command_output(self, f: Callable[..., any]) -> Callable[..., any]:
    def wrapped(*args, publish: List[str], should_echo: bool, **kwargs):
      result = f(*args, **kwargs)
      for channel in publish:
        subscriber_count = self.context.redis.publish(channel, result)
        if should_echo:
          print(f'Sent response to {subscriber_count} subscribers.')
      if not publish or should_echo:
        print(result)

    return wrapped

class StatusCommand(OutputCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.info

  @property
  def name(self) -> str:
    return 'status'

  @property
  def aliases(self) -> List[str]:
    return ['s']

  @property
  def click_command(self) -> click.Command:
    @click.command()
    @self.decorate
    def status():
      return self.context.status_items
    return status

class ListCommand(OutputCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.info

  @property
  def name(self) -> str:
    return 'list'

  @property
  def aliases(self) -> List[str]:
    return ['ls']

  @property
  def click_command(self) -> click.Command:
    @click.command(name=self.name)
    @click.option('-t', '--target', 'targets', type=click.Choice([t.value for t in ElementTarget]), multiple=True)
    @self.decorate
    def click_command(targets):
      elements = [
        e
        for t in ElementTarget if not targets or t.value in targets
        for e in t.get_elements(redis=self.context.redis)
      ]
      return [Styleds(parts=[
        CustomStyled(e.display_name, Format().cyan()),
        CustomStyled(f' {e.display_summary}', Format().blue()),
      ]).styled for e in elements]

    return click_command

class ViewCommand(OutputCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.info

  @property
  def name(self) -> str:
    return 'view'

  @property
  def aliases(self) -> List[str]:
    return ['v']

  @property
  def click_command(self) -> click.Command:
    @click.command(name=self.name)
    @click.option('-s', '--structure-id', 'ids', help='Filter output by structure IDs.', multiple=True)
    @click.option('-t', '--tag', 'tags', help='Filter output by tags.', multiple=True)
    @self.decorate
    def click_command(ids: List[str], tags: List[str]):
      id_regexes = list(map(re.compile, ids))
      def key_matches(keys: Set[str], regexes: List[re.Pattern]):
        for key in keys:
          for regex in regexes:
            if regex.match(key): return True
        return False

      filtered_keys = [
        k
        for k in self.context.redis.hkeys(micra_structures.key)
        if not ids or key_matches(keys={k}, regexes=id_regexes)
      ]
      if not filtered_keys:
        return []

      structures = [
        Structure.from_dict(json.loads(v))
        for v in self.context.redis.hmget(micra_structures.key, filtered_keys)
      ]
      tag_regexes = list(map(re.compile, tags))
      structures = list(filter(lambda s: not tags or key_matches(keys=s.tags, regexes=tag_regexes), structures))
      return structures

    return click_command

class SubprocessCommand(CoordinatorCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.caution

  @property
  def name(self) -> str:
    return 'subprocess'

  @property
  def click_command(self) -> click.Command:
    @click.group(name=self.name)
    @click.argument('pid', type=int)
    @click.pass_context
    def click_command(ctx: any, pid:int):
      ctx.obj = pid

    @click_command.command(name='set')
    @click.argument('command')
    @click.pass_obj
    def set_command(pid: int, command: str):
      self.context.add_subprocess(pid=pid, command=command)

    @click_command.command()
    @click.pass_obj
    def clear(pid: int):
      self.context.remove_subprocess(pid=pid)

    @click_command.command()
    @click.pass_obj
    def terminate(pid: int):
      try:
        os.kill(pid, signal.SIGTERM)
      except ProcessLookupError:
        pass

    @click_command.command()
    @click.pass_obj
    def kill(pid: int):
      try:
        os.kill(pid, signal.SIGKILL)
      except ProcessLookupError:
        pass

    return click_command

  @property
  def can_run(self) -> bool:
    return self.context.running

class MessageCommand(CoordinatorCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.caution

  @property
  def name(self) -> str:
    return 'message'

  @property
  def click_command(self) -> click.Command:
    @click.group(name=self.name)
    @click.argument('key')
    @click.pass_context
    def click_command(ctx: any, key:str):
      ctx.obj = key

    @click_command.command(name='set')
    @click.argument('message')
    @click.pass_obj
    def set_command(key: str, message: str):
      self.context.add_message(key=key, message=message)

    @click_command.command()
    @click.pass_obj
    def clear(key: str):
      self.context.remove_message(key=key)

    return click_command

class ListenCommand(OutputCommand[Coordinator]):
  @property
  def category(self) -> CommandCategory:
    return CommandCategory.caution

  @property
  def name(self) -> str:
    return 'listen'

  @property
  def click_command(self) -> click.Command:
    @click.group(name=self.name)
    def click_command():
      pass

    @click_command.command()
    @click.argument('listener')
    @click.argument('starter_args', nargs=-1)
    def start(listener: str, starter_args: List[str]):
      self.context.listener_starters[listener](*starter_args)

    @click_command.command()
    @click.argument('thread_id', type=int)
    def stop(thread_id: int):
      self.context.stop_listener(thread_id=thread_id)

    @click_command.command(name='list')
    @self.decorate
    def list_command():
      return list(sorted(self.context.listener_starters.keys()))

    return click_command

  @property
  def can_run(self) -> bool:
    return self.context.running

class ForwardCommand(CoordinatorCommand[Coordinator]):
  _name: str
  key: str

  def __init__(self, name: str, key: str, context: CoordinatorCommand):
    self._name = name
    self.key = key
    super().__init__(context=context)

  @property
  def category(self) -> CommandCategory:
    return CommandCategory.caution

  @property
  def name(self) -> str:
    return self._name

  @property
  def click_command(self) -> click.Command:
    @click.command(name=self.name)
    @click.argument('forward_args', nargs=-1)
    @click.pass_context
    def click_command(ctx: any, forward_args: List[str]):
      self.context.redis.lpush(self.key, self.quote_command(command_args=forward_args))

    return click_command

  @property
  def can_run(self) -> bool:
    return self.context.running
