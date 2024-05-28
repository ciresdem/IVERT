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

# We'll install as user for now. This could be changed in the future.
site.ENABLE_USER_SITE = True

with open('README.md', 'r') as fh:
    long_description = fh.read()

with open('VERSION', 'r') as fh:
    version = fh.read().strip()

setuptools.setup(
    name = 'ivert',
    version = version,
    description = 'The ICESat-2 Validation of Elevations Reporting Tool (IVERT)',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    license = 'MIT',
    author = 'Michael MacFerrin',
    author_email = 'michael.macferrin@colorado.edu',
    url = 'http://github.com/ciresdem/IVERT',
    packages = ['ivert'],
    package_dir = {'': 'src'},
    package_data = {'ivert': ['data/icesat2/ATL03_EMPTY_TILE.h5', 'data/empty_tile.tif', 'config/*.ini', 'config/*.sql']},
    classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI APPROVED :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires = [
        'numpy', # all
        'scipy', # waffles/convex hulls
        'h5py', # nsidc
        'boto3', # for amazon
        'pandas', # for cshelph
        'dateparser',
        'sqlite3',
    ],
    entry_points = {
        'console_scripts': [
            'ivert = ivert_client:ivert_client_cli'
        ]
    },
    py_modules = [
        's3.py',
        'sns.py'
        'client_job_download.py',
        'client_job_status.py',
        'client_job_upload.py',
        'ivert_client.py',
        'client_job_validate.py',
        'client_subscriptions.py',
        'client_test_job.py',
        'jobs_database.py',
        'utils/create_empty_tiff.py',
        'utils/bcolors.py',
        'utils/configfile.py',
        'utils/is_aws.py',
        'utils/is_conda.py',
        'utils/is_email.py',
        'utils/progress_bar.py',
        'utils/query_yes_no.py',
        'utils/sizeof_format.py',
        'utils/traverse_directory.py',
    ],
    scripts = [
        's3.py',
        'jobs_database.py'
    ],
    python_requires = '>=3.9',
    project_urls = {
        'Source': 'https://github.com/ciresdem/IVERT',
    },
)

### End
