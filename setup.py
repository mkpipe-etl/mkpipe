from setuptools import setup, find_packages

setup(
    name="mkpipe-core",
    version="0.1.2",
    license="Apache License 2.0",
    packages=find_packages(exclude=["tests", "scripts"]),
    install_requires=[],
    include_package_data=True,
    entry_points={
        "mkpipe.extractors": [],
        "mkpipe.loaders": [],
        "mkpipe.transformers": [],
    },
    description="Core ETL pipeline framework for mkpipe.",
    author="Metin Karakus",
    author_email="metin_karakus@yahoo.com",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.8",
)
