import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='amplio',
    version='0.0.109',
    author='Bill Evans',
    author_email='bill@amplio.org',
    description='Amplio Python Library',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/pypa/sampleproject',
    packages=setuptools.find_packages(exclude=('tests', 'docs')),
    install_requires=[
        #'fuzzywuzzy>=0.17.0',
        #'python-Levenshtein>=0.12.0',
        #'boto3',
        'openpyxl>=2.6.2'
    ],
    classifiers=["Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",],
    python_requires='>=3.7',
)
