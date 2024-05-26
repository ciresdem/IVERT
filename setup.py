### setup.py
##
## Copyright (c) 2024 Regents of the University of Colorado
##
## Permission is hereby granted, free of charge, to any person obtaining a copy 
## of this software and associated documentation files (the "Software"), to deal 
## in the Software without restriction, including without limitation the rights 
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
## of the Software, and to permit persons to whom the Software is furnished to do so, 
## subject to the following conditions:
##
## The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
##
## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
## INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR 
## PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
## FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
## ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
##
### Code:
import setuptools
import site
# import sys

# We'll install as user for now. This could be changed in the future.
site.ENABLE_USER_SITE = True

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'ivert',
    version = '0.1.0',
    description = 'Modules and scripts for the ICESat-2 Validation of Elevations Reporting Tool (IVERT)',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    license = 'MIT',
    author = 'CIRES Coastal DEM Team',
    author_email = 'michael.macferrin@colorado.edu',
    url = 'http://github.com/ciresdem/IVERT',
    packages = ['ivert'],
    package_data = {'ivert': ['data/icesat2/ATL03_EMPTY_TILE.h5', 'data/empty_tile.tif', 'config/*.ini', 'config/*.sql']},
    classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI APPROVED :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires = [
        'numpy', # all
        'scipy', # waffles/convex hulls
        'pyproj', # all
        'h5py', # nsidc
        'boto3', # for amazon
        'utm', # for cshelph
        'pandas', # for cshelph
        'dateparser',
        'osgeo'
    ],
    entry_points = {
        'console_scripts': [
            'ivert = src.ivert_client:ivert_client_cli',
        ],
    },
    scripts = [
        'src/s3.py',
        'src/sns.py'
        'src/validate_dem.py',
        'src/validate_dem_collection.py'
    ],
    python_requires = '>=3.9',
    project_urls = {
        'Source': 'https://github.com/ciresdem/IVERT',
    },
)

### End
