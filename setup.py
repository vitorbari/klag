"""
Setup script for klag package.
"""

from setuptools import setup, find_packages

setup(
    name="klag",
    version="1.0.0",
    description="Kafka Consumer Lag Visualizer",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Vitor Bari Buccianti",
    author_email="",  # Add your email if desired
    url="https://github.com/vitorbari/klag",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pyyaml>=5.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "klag=klag.__main__:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Utilities",
    ],
    python_requires=">=3.6",
)