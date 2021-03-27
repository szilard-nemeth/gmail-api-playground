# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

from setuptools import setup, find_packages


with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='gmailplayground',
    version='0.1.0',
    description='Gmail API Playground',
    long_description=readme,
    author='Szilard Nemeth',
    author_email='szilard.nemeth88@gmail.com',
    url='',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

