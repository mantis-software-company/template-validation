from setuptools import setup,find_packages

requirements = list()

setup(
    name="template-validation-1",
    version="1.0.0",
    author="Nurettin Şen",
    author_email="nurie487@gmail.com",
    description="Job library that checks data via rabbitmq and pushes valid data to one queue and invalid data to another queue ",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    platforms="all",
    classifiers=[

        "License :: OSI Approved :: Apache Software License",
        "Topic :: Internet",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
	    "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Operating System :: MacOS",
        "Operating System :: POSIX",
        "Operating System :: Microsoft",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8"
    ],
    install_requires=requirements,
    python_requires=">3.6.*, <4",
    packages=find_packages(include=['rabbitmq_validator']),
    project_urls={ 
        'Source': 'https://github.com/nuri35/template-validation-1',
    },
)
