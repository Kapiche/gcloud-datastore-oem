import os
from setuptools import setup, find_packages


with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()


# Use the VERSION file to get caterpillar version
with open(os.path.join(os.path.dirname(__file__), 'gcloudoem', 'VERSION')) as fh:
    version = fh.read().strip()

setup(
    name='gcloud-datastore-oem',
    version=version,
    description='gcloud-datastore-oem is a Python Object-Entity Mapper for working with Google Datastore.',
    long_description=long_description,
    url='https://github.com/Kapiche/gcloud-datastore-oem',
    author='Kapiche',
    author_email='opensource@kapiche.com',
    license='Apache Software License',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    packages=find_packages(exclude=["tests*", "docs*"]),
    install_requires=[
        "future",
        'httplib2',
        'oauth2client >= 1.4.7',
        'protobuf >= 3.0.0a1',  # Need Python 3 support
        'pycrypto',
        'pytz',
        "six",
    ],
    extras_require={
        'test': ['nose', 'coverage', 'unittest2'],
    },
    package_data={
        'gcloudoem': ['VERSION'],
    }
)
