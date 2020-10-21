class MicraError(Exception):
  pass

class MicraInputTimeout(MicraError):
  pass

class MicraSubprocessEnded(MicraError):
  def __init__(self, pid):
    super().__init__(f'Subprocess {pid} ended.')

class MicraQuit(MicraError):
  pass

class MicraResurrect(MicraError):
  pass

class MicraStopRetry(MicraError):
  pass