import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="scott.kerbey",
    version="0.0.1",
    author="Scott Kerbey",
    author_email="skerbey@hotmail.com",
    description="An application for predicting which dota team will win",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://dev.azure.com/scottkerbey/_git/Dota2%20Win%20Predictor",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
