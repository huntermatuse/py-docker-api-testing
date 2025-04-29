import setuptools

setuptools.setup(
    name="iDocker",
    version="1.0.0",
    author="hunter matuse",
    author_email="",
    description="A wrapper for the docker api to do my common tasks",
    url="https://github.com/huntermatuse/py-docker-api-testing",
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.10',
    install_requires=[
        'certifi',
        'charset-normalizer',
        'docker',
        'idna',
        'requests',
        'urllib3'
    ]
)