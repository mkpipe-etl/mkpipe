from setuptools import setup, find_packages

setup(
    name='mkpipe',
    version='1.0.0',
    license='Apache License 2.0',
    packages=find_packages(exclude=['tests', 'scripts', 'deploy']),
    install_requires=[
        'pyspark>=3.5.0',
        'pydantic>=2.0',
        'PyYAML>=6.0',
        'python-dotenv>=1.0',
        'psutil>=5.0',
        'click>=8.0',
    ],
    extras_require={
        'postgres-backend': ['psycopg2-binary>=2.9'],
        'duckdb-backend': ['duckdb>=1.0'],
        'clickhouse-backend': ['clickhouse-connect>=0.8'],
        'all-backends': [
            'psycopg2-binary>=2.9',
            'duckdb>=1.0',
            'clickhouse-connect>=0.8',
        ],
    },
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'mkpipe=mkpipe.cli:cli',
        ],
        'mkpipe.extractors': [],
        'mkpipe.loaders': [],
        'mkpipe.backends': [],
    },
    description='Spark-based modular ETL pipeline framework.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Metin Karakus',
    author_email='metin_karakus@yahoo.com',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
    ],
    python_requires='>=3.9',
)
