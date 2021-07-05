#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import distutils
import subprocess
from os.path import dirname, join

from setuptools import setup, find_packages



install_requires = [
    'faker',
    'six',
    'progress',
    'psycopg2',
    'pyyaml==5.2.0'
]

tests_require = [
    'coverage',
    'flake8',
    'pydocstyle',
    'pylint',
    'pytest-pep8',
    'pytest-cov',
    'pytest-pythonpath',
    'pytest',
]

setup(
    name='pganonymize',
    description='Commandline tool to anonymize PostgreSQL databases',
    author='Henning Kage',
    author_email='henning.kage@rheinwerk-verlag.de',
    maintainer='Rheinwerk Verlag GmbH Webteam',
    maintainer_email='webteam@rheinwerk-verlag.de',
    url='https://github.com/rheinwerk-verlag/postgresql-anonymizer',
    license='MIT license',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Environment :: Console',
        'Topic :: Database'
    ],
    packages=find_packages(include=['pganonymizer*']),
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    entry_points={
        'console_scripts': [
            'pganonymize = pganonymizer.__main__:main'
        ]
    }
)
