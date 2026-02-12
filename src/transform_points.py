
from cudem import srsfun, regions
import numpy
import pandas
import pyproj
import typing

def transform_points(x: typing.Union[list, tuple, numpy.ndarray, pandas.Series],
                     y: typing.Union[list, tuple, numpy.ndarray, pandas.Series],
                     z: typing.Union[list, tuple, numpy.ndarray, pandas.Series],
                     src_epsg: typing.Union[str, int],
                     dst_epsg: typing.Union[str, int],
                     src_region: typing.Union[list, tuple, numpy.ndarray, regions.Region, None] = None,
                     ) -> tuple:
    """Transform a set of 3D points from one coordinate reference system to another, vertically and horizontally.

    Parameters:
        x (list, tuple, numpy.ndarray, pandas.DataFrame): The x-coordinates of the points to transform.
        y (list, tuple, numpy.ndarray, pandas.DataFrame): The y-coordinates of the points to transform.
        z (list, tuple, numpy.ndarray, pandas.DataFrame): The z-coordinates of the points to transform.
        src_epsg (str or int): The source coordinate reference system, as an EPSG code (can be combined).
            Should be a 3D reference system.
        dst_epsg (str or int): The destination coordinate reference system, as an EPSG code (can be combined).
            Should be a 3D reference system.
        src_region (list, tuple, cudem.regions.Region): The source region to use for the transformation.
            Should be in same coordinate system as src_epsg. If None, the region will be set to the bounding box extents of the points given.

    Creates:
        This function may download and create some files and sub-directories within the active working directory
         to perform the needed set of transformations. The files will remain where they were created. The user can
         change where that will happen by making an os.chdir() call before this function is called.

    Raises:
        ValueError if either the horizontal or vertical transformation cannot be made.

    Returns:
        A 3-long tuple of x, y, and z points, same length as in the input points, in the dst_espg 3D reference system.
        """
    # First, check if the EPSG codes are the same. If so, just return the inputs.
    # Convert the user input into pyproj projection objects
    src_proj = pyproj.CRS.from_user_input(src_epsg)
    dst_proj = pyproj.CRS.from_user_input(dst_epsg)

    if src_proj.is_exact_same(dst_proj):
        return x, y, z

    # By just taking the 1st 4 digits, it can handle a 6-value bbox like IVERT uses,
    # ignoring the tmin, tmax fields at the end.
    # If it's a tuple, create a region object from the tuple.
    if type(src_region) in (list, tuple, numpy.ndarray):
        region_obj = regions.Region().from_list([float(src_region[0]), float(src_region[1]),
                                                 float(src_region[2]), float(src_region[3]),])

    # If no region is given (None), just get it from the extents of the points provided.
    elif src_region is None:
        xmin = min(x)
        xmax = max(x)
        ymin = min(y)
        ymax = max(y)
        region_obj = regions.Region().from_list([xmin, xmax, ymin, ymax])

    # If it was already provided as a regions.Region object, just use it.
    elif isinstance(src_region, regions.Region):
        region_obj = src_region

    # Unhandled data type.
    else:
        raise TypeError(f"Unhandled type {str(type(src_region))} for 'src_region' parameter. Should be a tuple, list, or cudem.regions.Region object.")

    # Create the transform object.
    transform = srsfun.set_transform(src_srs=f"EPSG:{src_proj.to_epsg()}",
                                     dst_srs=f"EPSG:{dst_proj.to_epsg()}",
                                     region = region_obj)

    # Transform the horizontal coordinates, if we can.
    if transform['transformer'] is None:
        raise ValueError("No transformation found for this dataset over the source region.")
    else:
        trans_x, trans_y = transform['transformer'].transform(x, y)

    # Transform the vertical coordinates, if we can.
    if transform['vert_transformer'] is None:
        raise ValueError("No vertical transformation found for this dataset over the source region.")
    else:
        _, _, trans_z = transform['vert_transformer'].transform(x, y, z)

    # Return the transformed points.
    return trans_x, trans_y, trans_z
