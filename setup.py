from setuptools import setup, find_packages

setup(
    name='mkpipe',
    version='0.3.12',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests', 'scripts', 'deploy', 'install_jars.py']),
    install_requires=[
        'psycopg2-binary>=2.9.10',
        'pyspark>=3.5.5',
        'pydantic>=2.10.3',
        'PyYAML>=6.0.2',
        'python-dotenv>=1.0.1',
        'celery>=5.4.0',
        'kombu>=5.4.2',
        'sqlalchemy>=2.0.36',
        'pysqlite3>=0.5.4',
        'duckdb>=1.2.1',
        'clickhouse-connect>=0.8.16',
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'mkpipe=mkpipe.main:cli',
        ],
        'mkpipe.extractors': [],
        'mkpipe.loaders': [],
        'mkpipe.transformers': [],
    },
    description='Core ETL pipeline framework for mkpipe.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
    ],
    python_requires='>=3.8',
)
