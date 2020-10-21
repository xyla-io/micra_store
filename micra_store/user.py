import click

from typing import Optional, List, Any as any
from environments import set_environment, environment
from .coordinator import Coordinator
from .command import Command

class MicraCommandGroup(click.MultiCommand):
  _micra_commands: Optional[List[Command[Coordinator]]]=None

  @property
  def micra_commands(self) -> List[Command[Coordinator]]:
    if self._micra_commands is None:
      self._micra_commands = []
    return self._micra_commands

  def add_micra_commands(self, commands: List[Command[Coordinator]]):
    self._micra_commands = self.micra_commands + commands

  def get_micra_command(self, name: str) -> Command[Coordinator]:
    return next(filter(lambda c: c.matches_command(name), self.micra_commands))

  def list_commands(self, ctx: any) -> List[str]:
    return [n for c in self.micra_commands for n in c.all_names]

  def get_command(self, ctx: any, name: str) -> click.Command:
    return self.get_micra_command(name=name).click_command

class RunContext:
  environment_name: Optional[str]
  pdb_enabled: bool
  dry_run: bool
  should_listen: bool
  should_define: bool
  interactive: bool
  quiet: bool
  commands: List[str]

  def __init__(self, pdb_enabled: bool=False, dry_run: bool=False, should_listen: bool=True, should_define: bool=True, interactive: bool=True, quiet: bool=False, environment_name: Optional[str]=None, commands: List[str]=[]):
    self.pdb_enabled = pdb_enabled
    self.dry_run = dry_run
    self.should_listen = should_listen
    self.should_define = should_define
    self.interactive = interactive
    self.quiet = quiet
    self.environment_name = environment_name
    self.commands = [*commands]

  def configure_coordinator(self, coordinator: Coordinator):
    coordinator.pdb_enabled = self.pdb_enabled
    coordinator.dry_run = self.dry_run
    coordinator.should_listen = self.should_listen
    coordinator.should_define = self.should_define
    coordinator.user.interactive = self.interactive
    coordinator.user.quiet = self.quiet
    coordinator.config = environment
    coordinator.commands_to_run = self.commands

@click.command(cls=MicraCommandGroup)
@click.option('--pdb/--no-pdb', 'pdb_enabled')
@click.option('--dry-run/--no-dry-run', 'dry_run')
@click.option('-l/-L', '--listen/--no-listen', 'should_listen', default=True)
@click.option('-d/-D', '--define/--no-define', 'should_define', default=True)
@click.option('-i/-I', '--interactive/--no-interactive', 'interactive', default=True)
@click.option('-q', '--quiet', 'quiet', is_flag=True)
@click.option('-e', '--environment', 'environment_name', type=str)
@click.option('-c', '--command', 'commands', type=str, multiple=True)
@click.pass_context
def run(ctx: any, pdb_enabled: bool, dry_run: bool, should_listen: bool, should_define: bool, interactive: bool, quiet: bool, environment_name: Optional[str], commands: List[str]):
  ctx.obj = RunContext(pdb_enabled=pdb_enabled, dry_run=dry_run, should_listen=should_listen, should_define=should_define, interactive=interactive, quiet=quiet, environment_name=environment_name, commands=commands)
  if ctx.obj.environment_name:
    set_environment(identifier=ctx.obj.environment_name)
  micra_subcommand = ctx.command.get_micra_command(name=ctx.invoked_subcommand)
  ctx.obj.configure_coordinator(coordinator=micra_subcommand.context)
  micra_subcommand.context.connect()

@run.resultcallback()
def finish_run(result: any, **kwargs):
  ctx = click.get_current_context()
  micra_subcommand = ctx.command.get_micra_command(name=ctx.invoked_subcommand)
  micra_subcommand.context.disconnect()


