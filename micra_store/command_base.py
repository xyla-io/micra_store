import shlex
import click

from typing import TypeVar, Generic, List, Callable
from enum import Enum
from moda.style import Styling, Color, Font, Styled, Styleds, CustomStyled

class CommandCategory(Enum):
  info = 'info'
  normal = 'normal'
  caution = 'caution'
  destructive = 'destructive'

  @property
  def style(self) -> Styling:
    if self is CommandCategory.info:
      return Color.cyan
    elif self is CommandCategory.normal:
      return Color.green
    elif self is CommandCategory.caution:
      return Color.yellow
    elif self is CommandCategory.destructive:
      return Color.red

T = TypeVar(any)
class Command(Generic[T]):
  context: T
  
  def __init__(self, context: T):
    self.context = context

  @classmethod
  def quote_command(self, command_args: List[str]):
    return ' '.join(shlex.quote(a) for a in command_args)

  @property
  def category(self) -> CommandCategory:
    return CommandCategory.normal

  @property
  def name(self) -> str:
    raise NotImplementedError()

  @property
  def aliases(self) -> List[str]:
    return []

  @property
  def all_names(self) -> List[str]:
    return [self.name, *self.aliases]

  @property
  def summary(self) -> str:
    return self.click_command.get_short_help_str()

  @property
  def description(self) -> str:
    c = self.click_command
    return self.click_command.get_help(c.make_context(c.name, []))

  @property
  def display_styled(self) -> Styled:
    return Styleds(parts=[
      CustomStyled(self.name, Font.bold + self.category.style),
      CustomStyled(f' ({", ".join(self.aliases)})' if self.aliases else '', self.category.style),
      CustomStyled(f' {self.summary}'),
    ])

  @property
  def click_command(self) -> click.Command:
    raise NotImplementedError()

  @property
  def can_run(self) -> bool:
    return True

  @property
  def micra_decorators(self) -> List[Callable[[Callable[..., any]], Callable[..., any]]]:
    return []

  @property
  def click_decorators(self) -> List[Callable[[Callable[..., any]], Callable[..., any]]]:
    return []

  def matches_command(self, command: str):
    return shlex.split(command)[0] in self.all_names

  def run(self, command: str):
    argv = shlex.split(command)
    return self.click_command.main(args=argv[1:], prog_name=argv[0], standalone_mode=False)

  def decorate(self, f: Callable[..., any]) -> Callable[..., any]:
    decorated = f
    for decorator in reversed(self.click_decorators + self.micra_decorators):
      decorated = decorator(decorated)
    return decorated