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
    packages = ['ivert', 'ivert_utils'],
    package_dir = {'ivert': 'src',
                   'ivert_utils': 'src/utils'},
    data_files = [('ivert_data/data/', ["data/empty_tile.tif"]),
                  ('ivert_data/config', ["config/email_templates.ini",
                                         "config/ivert_config.ini",
                                         "config/ivert_job_config_TEMPLATE.ini",
                                         "config/ivert_user_config_TEMPLATE.ini",
                                         "config/ivert_jobs_schema.sql"]),
                  ("ivert_data/", ["VERSION", "LICENSE", "README.md", "VERSION_CLIENT_MIN"]),
                  ],
    # package_data = {'ivert': ['data/icesat2/ATL03_EMPTY_TILE.h5',
    #                           'data/empty_tile.tif',
    #                           'Config/*.ini',
    #                           'Config/*.sql']},
    classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI APPROVED :: MIT License',
        'Operating System :: OS Independent',
    ],
    install_requires = [
        'numpy', # all
        'scipy',
        'h5py',
        'boto3', # for amazon
        'pandas', # for reading the IVERT database tables.
        'dateparser',
        'tabulate',
        'psutil'
    ],
    entry_points = {
        'console_scripts': [
            'ivert = ivert.client:ivert_client_cli'
        ]
    },
    py_modules = [
    ],
    scripts = [
        'src/s3.py',
        'src/jobs_database.py'
    ],
    python_requires = '>=3.9',
    project_urls = {
        'Source': 'https://github.com/ciresdem/IVERT',
    },
)

### End
