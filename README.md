# IVERT
The ICESat-2 Validation of Elevations Reporting Tool

This code is currently in active development by the CIRES Coastal DEM Team. Primary authors are [Mike MacFerrin](https://github.com/mmacferrin) (developing the IVERT code) and [Matthew Love](https://github.com/matth-love) (developing the [CUDEM](https://github.com/ciresdem/cudem) modules underpinning various aspects of IVERT's functionality). Some modules may have broken dependencies or other shortfalls, bugs are being worked out as we migrate to a cloud computing environment. This code is not yet considered stable.

Primary modules for DEM validation are:

- **[validate_dem.py](https://github.com/ciresdem/IVERT/blob/main/src/validate_dem.py)** -- Code for performing ICESat-2 validations--with masking and vertical datum conversions--on a single DEM.
- **[validate_dem_collection.py](https://github.com/ciresdem/IVERT/blob/main/src/validate_dem_collection.py)** -- Code for performing ICESat-2 validations on a group or directory of DEMs. A wrapper for looped execution and gathering summary results from looped calls of validate_dem.py

Both the scripts can be run independently as Python scripts with the "-h" or "--help" flags to see a complete list of command-line options.

Code to execute IVERT in a client-server setting in an AWS environment is currently underway. This README will be updated when that is completed.
