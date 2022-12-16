from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='ong_esios',
    version='0.2.1',
    packages=['ong_esios'],
    url='www.neirapinuela.es',
    license='',
    author='ongpi',
    author_email='oneirag@yahoo.es',
    description='Functions to download from from esios api',
    install_requires=required,
)
