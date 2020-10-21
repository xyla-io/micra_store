from .base import ContentType, ContentConverter, Join, Structure, StructureType, Hash, Set

json_type = ContentType(
  identifier='json',
  title='JSON',
  description='A JSON string.',
  converter=ContentConverter.json
)

json_object_type = ContentType(
  identifier='json_object',
  title='JSON Object',
  description='A JSON object string.',
  converter=ContentConverter.json_object
)

micra_command = ContentType(
  identifier='micra_command',
  title='Micra Command',
  description='A micra command string.',
  converter=ContentConverter.string
)

micra_content_types = Hash(
  identifier='micra_content_types',
  title='Micra Content Types',
  description='Micra type definitions stored as JSON objects.',
  key='micra_content_types',
  content_type=json_object_type.identifier
)

micra_structures = Hash(
  identifier='micra_structures',
  title='Micra Structures',
  description='Micra structure definitions stored as JSON objects.',
  key='micra_structures',
  content_type=json_object_type.identifier
)

micra_definitions = Structure(
  identifier='micra_definitions',
  title='Micra Definitions',
  description='Micra content type and structure definitions.',
  key='',
  structure_type=StructureType.hash,
  content_type=json_object_type.identifier,
  joins=[
    Join(structure=micra_content_types.identifier),
    Join(structure=micra_structures.identifier),
  ]
)

micra_structures_with_types = Structure(
  identifier='micra_structures_with_types',
  title='Micra Structures with Content Types',
  description='Micra structure definitions with content types attached.',
  key='',
  structure_type=StructureType.hash,
  content_type=json_object_type.identifier,
  joins=[
    Join(structure=micra_structures.identifier),
    Join(
      structure=micra_content_types.identifier,
      select=['json_object.title', 'json_object.description', 'json_object.converter'],
      on={'json_object.content_type': 'json_object.identifier'}
    ),
  ]
)

micra_commands = Structure(
  identifier='micra_commands',
  title='Micra Commands',
  description='A queue of commands for Micra to execute.',
  key='micra_commands',
  structure_type=StructureType.list,
  content_type=micra_command.identifier
)
