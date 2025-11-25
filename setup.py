

from setuptools import setup, find_packages


setup(name="tap-quickbase",
      version="0.0.1",
      description="Singer.io tap for extracting data from Quickbase API",
      author="Stitch",
      url="http://singer.io",
      classifiers=["Programming Language :: Python :: 3 :: Only"],
      py_modules=["tap_quickbase"],
      install_requires=[
        "singer-python==6.3.0",
        "requests==2.32.4",
        "backoff==2.2.1",
        "parameterized"
      ],
      entry_points="""
          [console_scripts]
          tap-quickbase=tap_quickbase:main
      """,
      packages=find_packages(),
      package_data = {
          "tap_quickbase": ["schemas/*.json"],
      },
      include_package_data=True,
)
