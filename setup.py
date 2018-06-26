#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quickbase',
      version='1.0.3',
      description='Singer.io tap for extracting data from QuickBase',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_quickbase'],
      install_requires=[
          'singer-python==5.0.6',
          'requests==2.18.4',
          'python-dateutil==2.6.1',
      ],
      entry_points='''
        [console_scripts]
        tap-quickbase=tap_quickbase:main
      ''',
      packages=['tap_quickbase'],
)
