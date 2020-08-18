import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyetltools", # Replace with your own username
    version="0.0.1",
    author="Artur Borecki",
    author_email="artur.borecki@gmail.com",
    description="Set of helper tools for data processing tasks including: Pandas, Spark, XLSX, CSV, Jira",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aborecki/pyetltools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)