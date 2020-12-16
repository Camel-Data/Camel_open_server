from setuptools import setup
from setuptools import find_packages

setup(name='camel_open_server',
      version='0.0.1',
      description='Query and analyze open server data',
      author='The fastest man alive.',
      packages=find_packages(),
      install_requires=['dbx','camel_utils_x','camel_queries','numpy', 'pandas','tqdm',
      'pymysql','matplotlib','seaborn'])
