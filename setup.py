import os
from setuptools import setup

# with open('requirements.txt') as f:
#     required = f.read().splitlines()

required = ["pandas", "MySQL-python", "psycopg2", "paramiko", "pysftp",]

setup(name='python-tools',
      version='0.1',
      description='python tools for generating reports and stuff',
      install_requires=required,
      url='https://github.com/adamrdavid/python-tools',
      author='Adam David',
      author_email='adamrdavid@gmail.com',
      license='MIT',
      packages=['reports', 'hulk', 'scraping'],
      zip_safe=False)