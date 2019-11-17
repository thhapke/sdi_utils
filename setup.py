import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gensolution",
    version="0.0.2",
    author="Thorsten Hapke",
    author_email="thorsten.hapke@sap.com",
    description="Generates solution package when locally developing SAP DI operators",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thhapke/textfield_parser",
    keywords = ['textfield parsing'],
    packages=setuptools.find_packages(),
    install_requires=[],
    classifiers=[
    	'Programming Language :: Python :: 3.5',
    	'Programming Language :: Python :: 3.6',
    	'Programming Language :: Python :: 3.7',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)