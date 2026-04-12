#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='scraper_manager',
    version='3.0.0',
    description='Queue-driven stock market scraper with RabbitMQ scheduler/worker runtime',
    author='Lucas Ward',
    author_email='lward@ipponusa.com',
    url='https://github.com/lward27/scraper_manager',
    packages=find_packages(),
    python_requires='>=3.11',
    install_requires=[
        'aiohttp>=3.9.0',
        'aio-pika>=9.4.1',
        'certifi>=2023.7.22',
    ],
)
