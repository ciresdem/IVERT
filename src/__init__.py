import os
# in the codes files.
VFILE1 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "VERSION"))
# In the package files, after installing.
VFILE2 = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ivert_data", "VERSION"))

if os.path.exists(VFILE1):
    file_to_use = VFILE1
elif os.path.exists(VFILE2):
    file_to_use = VFILE2
else:
    raise FileNotFoundError("Could not find 'VERSION' file.")

__version__ = None
with open(file_to_use, 'r') as f:
    __version__ = f.read().strip()
