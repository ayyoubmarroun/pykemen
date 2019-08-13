from setuptools import setup
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages

setup(
    name='pykemen',
    version='0.2.4',
    description='a pykemen pip-installable package',
    license='Eiup',
    packages=find_packages(),
    include_package_data=True,
    package_data={'pykemen': ['google/*.py', '*']},
    author='Ayyoub Marroun',
    author_email='ayyoub@metriplica.com',
    keywords=['google apis', 'utilities'],
    url='https://github.com/ayyoubmarroun/pykemen',
    download_url='https://github.com/ayyoubmarroun/pykemen/archive/0.2.4.tar.gz',
    install_requires=[
        'httplib2',
        'pandas',
        'google-api-python-client',
        'oauth2client',
        'google-cloud',
        'google-cloud-core',
        'google-cloud-bigquery',
        'future'
    ]
)