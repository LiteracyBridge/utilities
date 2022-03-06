from distutils.core import setup

requirements = [
    'python-dateutil==2.8.2',
    'six==1.16.0'
]

setup(
    name='v2LogReader',
    version='0.8',
    packages=['utils', 'tbstats', 'V2LogReader', 'S3Data'],
    py_modules=['run'],
    url='',
    license='GPLv2',
    author='bill',
    author_email='bill@amplio.org',
    install_requires=requirements,
    python_requires=">=3.9.0",
    description='Read and process TBv2 logs and statistics.'
)
