from setuptools import setup

setup(
    name='pykemen',
    version='0.0.8b',
    description='a pykemen pip-installable package',
    license='Eiup',
    packages=['pykemen'],
    author='Ayyoub Marroun',
    author_email='ayyoub@metriplica.com',
    keywords=['google apis', 'utilities'],
    url='https://github.com/ayyoubmarroun/pykemen',
    download_url='https://github.com/ayyoubmarroun/pykemen/archive/0.0.8b.tar.gz',
    install_requires=[
        'httplib2',
        'pandas',
        'google_api_python_client',
        'oauth2client',
    ]
)