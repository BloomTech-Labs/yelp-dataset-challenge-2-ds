import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="awstools", # Replace with your own username
    version="0.0.1",
    author="Vincent Brandon",
    author_email="vincent.a.brandon@gmail.com",
    description="Wrapper for Boto3 commands",
    # long_description=long_description,
    # long_description_content_type="text/markdown",
    url="https://github.com/Lambda-School-Labs/yelp-dataset-challenge-2-ds/tree/master/awstools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)