from setuptools import setup, find_packages
from io import open


with open("README.md", encoding="utf-8") as readme_file:
    readme = readme_file.read()

REQUIRES = ['sqlparse']

setup(
    name="fme-reportnet",
    packages=find_packages("src"),
    package_dir={"": "src"},
    version="1.0.2",
    description="Code for the eea.reportnet package in FME",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="EEA",
    url="https://github.com/eea/eea.reportnet3.api.fme",
    keywords="fme fmepy transformer",
    classifiers=[
        "DO NOT UPLOAD TO PYPI",
        "Framework :: FME",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    zip_safe=False,
    install_requires=REQUIRES,
)
