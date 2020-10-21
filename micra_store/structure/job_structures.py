from .base import Set, OrderedSet, Stream, ContentType, ContentConverter

job_identifier = ContentType(
  identifier='job_identifier',
  title='Job Identifier',
  description='Unique identifier for a job.',
  converter=ContentConverter.string,
  tags={'job'}
)

job_version = ContentType(
  identifier='job_version',
  title='Job Version',
  description='Unique version of a job, representing a specific set of execution parameters.',
  converter=ContentConverter.string,
  tags={'job'}
)

job_instance = ContentType(
  identifier='job_identifier',
  title='Job Identifier',
  description='Unique instance of a job, representing a specific time the job was performed.',
  converter=ContentConverter.string,
  tags={'job'}
)

job_appointment = ContentType(
  identifier='job_appointment',
  title='Job Appointment',
  description='Information indicating a job that a worker can claim.',
  converter=ContentConverter.dictionary,
  tags={'job'},
  properties={'job': 'job_instance'}
)

jobs_active = Set(
  identifier='jobs_active',
  title='Active Jobs',
  description='All jobs that are scored, enqueued, or currently running.',
  key='active_jobs',
  content_type=job_identifier.identifier,
  tags={'job'} 
)

jobs_ready = Set(
  identifier='jobs_ready',
  title='Ready Jobs',
  description='All jobs that are enqueued or currently running.',
  key='ready_jobs',
  content_type=job_identifier.identifier,
  tags={'job'}
)

jobs_ready_almacen = Stream(
  identifier='jobs_ready_almacen',
  title='Jobs for Almacén',
  description='All jobs sent to Almacén workers.',
  key='almacen_ready_jobs',
  content_type=job_appointment.identifier,
  tags={'job'}
)

jobs_scored = OrderedSet(
  identifier='jobs_scored',
  title='Scored Jobs',
  description='All jobs that have been assigned a run priority score.',
  key='scored_jobs',
  content_type=job_identifier.identifier,
  tags={'job'}
)