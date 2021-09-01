from setuptools import find_packages, setup

requirements_file = "requirements.txt"

with open("README.md") as f:
    readme = f.read()

setup(
    name="tap-twitter",
    version="0.1.0",
    description="Singer.io tap for extracting data from Twitter v2 API.",
    long_description=readme,
    author="Peter Begle",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_twitter"],
    install_requires=open(requirements_file).readlines(),
    entry_points="""
    [console_scripts]
    tap-twitter=tap_twitter:main
    """,
    packages=find_packages(exclude=["tests"]),
    package_data = {
        "schemas": ["tap_twitter/schemas/*.json"]
    },
    include_package_data=True,
)
