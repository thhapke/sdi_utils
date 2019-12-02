import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sdi_utils",
    version="0.0.43",
    author="Thorsten Hapke",
    author_email="thorsten.hapke@sap.com",
    description="List of SAP DI helper functions like gensolution (package locally developed operators,textfield_parser, time_monitoring",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thhapke/gensolution",
    keywords = ['SAP Data Intelligence','genjson','tprogress','textfield_parser'],
    packages=setuptools.find_packages(),
    install_requires=[
        'requests'
    ],
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'gensolution = sdi_utils.gensolution:main'
        ]
    },
    classifiers=[
    	'Programming Language :: Python :: 3.5',
    	'Programming Language :: Python :: 3.6',
    	'Programming Language :: Python :: 3.7',
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)