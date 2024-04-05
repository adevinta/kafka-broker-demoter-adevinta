import inspect
import os
import sys
from pathlib import Path

from setuptools import Extension, find_packages, setup
from setuptools.command.test import test as TestCommand

__location__ = os.path.join(
    os.getcwd(), os.path.dirname(inspect.getfile(inspect.currentframe()))
)

version = "2.1.0"

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()


def get_install_requirements(path):
    content = open(os.path.join(__location__, path)).read()
    requires = [req for req in content.split("\\n") if req != ""]
    return requires


setup(
    python_requires=">=3.7",
    name="kafka_broker_demoter",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=version,
    description="Tool for safely demote a broker from a kafka cluster",
    author="Sergio Troiano",
    author_email="sergio_troiano@hotmail.com",
    packages=find_packages(include=["kafka_broker_demoter"]),
    install_requires=get_install_requirements("requirements/requirements.txt"),
    test_suite="tests",
    zip_safe=False,
    entry_points={"console_scripts": ["kafka_broker_demoter=kafka_broker_demoter.cli:main"]},
)
