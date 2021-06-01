import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

REQUIREMENTS = [
    'PyGithub == 1.*',
]

DEV_REQUIREMENTS = [
    'coveralls == 3.*',
    'flake8',
    'mock == 4.*',
    'pytest == 6.*',
    'pytest-cov == 2.*',
]

setuptools.setup(
    name='github-archive',
    version='3.1.0',
    description='A powerful script to concurrently clone your entire GitHub instance or save it as an archive.',  # noqa
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='http://github.com/justintime50/github-archive',
    author='Justintime50',
    license='MIT',
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=REQUIREMENTS,
    extras_require={
        'dev': DEV_REQUIREMENTS
    },
    entry_points={
        'console_scripts': [
            'github-archive=github_archive.cli:main'
        ]
    },
    python_requires='>=3.6',
)
