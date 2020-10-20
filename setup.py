from setuptools import setup, find_packages
with open("README.md", "r") as fh:
    long_description = fh.read()

CLASSIFIERS = [
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Topic :: Scientific/Engineering :: Information Analysis",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Development Status :: 4 - Beta"
]

setup(
    name='audiencemanager',
    version="0.0.2",
    license='GPL',
    description='Adobe Audience Manager API python wrapper',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Julien Piccini',
    author_email='piccini.julien@gmail.com',
    url='https://github.com/pitchmuc/audiencemanager',
    keywords=['adobe', 'audience manager', 'API', 'python'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'pandas',
        'pathlib2',
        'pathlib',
        'requests',
        'PyJWT[crypto]',
        'PyJWT',
        ],
    classifiers=CLASSIFIERS,
    python_requires='>=3.7'
)
