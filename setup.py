from setuptools import setup, find_packages

setup(
  name='micra_store',
  version='0.0.1',
  description='Xyla\'s common code to support microservices.',
  url='https://github.com/xyla-io/micra_store',
  author='Xyla',
  author_email='gklei89@gmail.com',
  license='MIT',
  packages=find_packages(),
  install_requires=[
    'redis',
    'hiredis',
    'pytest',
    'ipython',
  ],
  zip_safe=False
)