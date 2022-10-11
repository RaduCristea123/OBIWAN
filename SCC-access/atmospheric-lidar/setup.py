#!/usr/bin/env python

from setuptools import setup
import os
import re
import io

# Read the long description from the readme file
with open("readme.rst", "rb") as f:
    long_description = f.read().decode("utf-8")


# Read the version parameters from the __init__.py file. In this way
# we keep the version information in a single place.
def read(*names, **kwargs):
    with io.open(
            os.path.join(os.path.dirname(__file__), *names),
            encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


# Run setup
setup(name='atmospheric_lidar',
      packages=['atmospheric_lidar', 'atmospheric_lidar.scripts', 'atmospheric_lidar.systems'],
      version=find_version("atmospheric_lidar", "__init__.py"),
      description='Package for reading raw atmospheric lidar data.',
      long_description=long_description,
      url='https://bitbucket.org/iannis_b/atmospheric-lidar/',
      author='Ioannis Binietoglou',
      author_email='ioannis@inoe.ro',
      license='MIT',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Intended Audience :: Science/Research',
          'Topic :: Scientific/Engineering :: Atmospheric Science',
      ],
      keywords='lidar aerosol licel SCC',
      install_requires=[
          "netCDF4",
          "numpy",
          "matplotlib",
          "sphinx",
          "numpydoc",
          "pytz",
          "pyyaml",
      ],
      entry_points={
          'console_scripts': ['licel2scc = atmospheric_lidar.scripts.licel2scc:main',
                              'licel2scc-depol = atmospheric_lidar.scripts.licel2scc_depol:main',
                              'licel2tc = atmospheric_lidar.scripts.licel2tc:main'],
      },
      )
