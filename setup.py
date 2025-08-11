#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quickbase',
      version='2.0.3',
      description='Singer.io tap for extracting data from QuickBase',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbase'],
      install_requires=[
          'singer-python==5.13.2',
          'requests==2.32.4',
          'python-dateutil==2.9.0',
          'pytz==2018.9',
      ],
      entry_points='''
        [console_scripts]
        tap-quickbase=tap_quickbase:main
      ''',
      packages=['tap_quickbase'],
)
