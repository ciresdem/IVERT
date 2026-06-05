#!/usr/bin/env python3
"""plot_photon_clouds_v2.py — plot classified ICESat-2 photon curtains from .nc granule files.

Usage
-----
    python plot_photon_clouds_v2.py <nc_file> [options]

The script reads a granule .nc file produced by IS2Database._process_h5_to_nc().
If the matching ATL03 .h5 file is available (searched in the same directory and
in the ivert cache), it splits photons by beam and plots one curtain per beam.
Without the .h5, all photons are plotted together sorted by latitude.

Class codes (current convention):
     1 = ground/land
     2 = canopy
     3 = canopy top
     7 = built structure
    40 = bathy floor (seafloor)
    41 = bathy surface (water surface)
"""

import argparse
import os
import glob
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import netCDF4


# ---------------------------------------------------------------------------
# Style
# ---------------------------------------------------------------------------
CLASS_STYLE = {
     0: dict(color="grey",    label="Noise",          zorder=0.5, alpha=0.5, s=1),
     1: dict(color="saddlebrown",  label="Ground",         zorder=2, alpha=1.0, s=3),
     2: dict(color="mediumseagreen", label="Canopy",       zorder=1, alpha=0.5, s=1),
     3: dict(color="darkgreen",    label="Canopy Top",     zorder=1, alpha=0.5, s=1),
     7: dict(color="red",          label="Built Structure", zorder=2, alpha=0.8, s=2),
    40: dict(color="darkorange",   label="Bathy Floor",    zorder=3, alpha=1.0, s=3),
    41: dict(color="dodgerblue",   label="Water Surface",  zorder=1, alpha=0.6, s=1),
    42: dict(color="dodgerblue",   label="Inland Water",   zorder=1, alpha=0.6, s=1),
}
DEFAULT_STYLE = dict(color="lightgrey", label="Other", zorder=0, alpha=0.3, s=1)



def _granule_id(filepath):
    """Return the bare granule ID from a file path (strips _subsetted and bbox suffixes)."""
    stem = os.path.splitext(os.path.basename(filepath))[0]
    for marker in ("_subsetted", "_W", "_N", "_E", "_S"):
        idx = stem.find(marker)
        if idx >= 0:
            stem = stem[:idx]
    return stem


def _find_h5(nc_path):
    """Search for a matching ATL03 .h5 whose name starts with the same granule ID."""
    gid = _granule_id(nc_path)
    if not gid:
        return None
    search_dirs = [
        os.path.dirname(nc_path),
        os.path.expanduser("~/.ivert/cache/icesat2"),
        os.path.expanduser("~/.ivert/cache"),
    ]
    for d in search_dirs:
        hits = sorted(glob.glob(os.path.join(d, "**", f"{gid}*.h5"), recursive=True))
        if hits:
            return hits[0]
    return None


def _utm_along_track_km(lons, lats):
    """Return cumulative along-track distance (km) for ordered lon/lat arrays.

    Reprojects to the auto-detected UTM zone so distances are computed in metres
    using true Cartesian geometry, then converts to km.
    """
    import geopandas as gpd
    gdf = gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons, lats), crs="EPSG:4326")
    gdf_utm = gdf.to_crs(gdf.estimate_utm_crs())
    x = gdf_utm.geometry.x.values
    y = gdf_utm.geometry.y.values
    step_m = np.sqrt(np.diff(x) ** 2 + np.diff(y) ** 2)
    return np.concatenate([[0.0], np.cumsum(step_m)]) / 1000.0


def _beam_delta_times(h5_path):
    """Return {beam_name: delta_time_array} for all beams present in the .h5."""
    import h5py
    beams = {}
    with h5py.File(h5_path, "r") as f:
        for beam in ["gt1l", "gt1r", "gt2l", "gt2r", "gt3l", "gt3r"]:
            try:
                dt = f[f"{beam}/heights/delta_time"][...]
                beams[beam] = dt
            except KeyError:
                pass
    return beams


def _load_h5_beam_photons(h5_path, beam):
    """Load all photons for one beam from ATL03 .h5, returning a DataFrame with class_code=0.

    Heights are converted from ellipsoidal to EGM2008 geoid by subtracting the
    geoid undulation interpolated from geophys_corr/geoid.  Photons are sorted by
    delta_time and a cumulative along_track_km column is computed via UTM projection.
    """
    import h5py
    with h5py.File(h5_path, "r") as f:
        try:
            delta_time = f[f"{beam}/heights/delta_time"][...]
            lon = f[f"{beam}/heights/lon_ph"][...]
            lat = f[f"{beam}/heights/lat_ph"][...]
            h_ph = f[f"{beam}/heights/h_ph"][...]
            geoid_dt = f[f"{beam}/geophys_corr/delta_time"][...]
            geoid = f[f"{beam}/geophys_corr/geoid"][...]
        except KeyError:
            return pd.DataFrame(columns=["x", "y", "z", "delta_time", "class_code", "along_track_km"])

    # Interpolate geoid undulation (segment rate) to per-photon rate, then subtract
    geoid_ph = np.interp(delta_time, geoid_dt, geoid)
    z = h_ph - geoid_ph

    # Sort by delta_time so along-track distance is computed in acquisition order
    order = np.argsort(delta_time)
    delta_time, lon, lat, z = delta_time[order], lon[order], lat[order], z[order]

    along_track_km = _utm_along_track_km(lon, lat)

    return pd.DataFrame({
        "x": lon,
        "y": lat,
        "z": z,
        "delta_time": delta_time,
        "class_code": np.zeros(len(delta_time), dtype=np.int8),
        "along_track_km": along_track_km,
    })


def load_nc(nc_path):
    """Load the .nc granule into a DataFrame."""
    with netCDF4.Dataset(nc_path) as ds:
        df = pd.DataFrame({
            v: ds.variables[v][:].data if hasattr(ds.variables[v][:], "data")
               else np.array(ds.variables[v][:])
            for v in ds.variables
        })
    return df


def plot_beam(df_beam, beam_name, outpath, zlim=None, dlim=None, classes=None, title_extra=""):
    """Plot one beam's photon curtain (along-track km vs elevation).

    classes: None  → plot all class codes present
             set() → reclassify all classified photons as noise (class 0)
             {1, 40, …} → plot those class codes; all others reclassified as noise
    """
    sort_col = "delta_time" if "delta_time" in df_beam.columns else "y"
    df_beam = df_beam.sort_values(sort_col).reset_index(drop=True)

    # Drop photons with non-physical elevations
    df_beam = df_beam[(df_beam["z"] >= -1e5) & (df_beam["z"] <= 1e5)].reset_index(drop=True)

    # Reclassify photons not in the requested set to noise (class 0) so they still appear
    if classes is not None:
        unselected = (df_beam["class_code"] != 0) & ~df_beam["class_code"].isin(classes)
        df_beam.loc[unselected, "class_code"] = 0

    along_track = df_beam["along_track_km"].values
    z = df_beam["z"].values
    cc = df_beam["class_code"].values

    fig, ax = plt.subplots(figsize=(12, 4))

    for code in np.unique(cc):
        mask = cc == code
        style = CLASS_STYLE.get(int(code), DEFAULT_STYLE)
        ax.scatter(along_track[mask], z[mask],
                   c=style["color"], label=f"{style['label']} (n={mask.sum():,})",
                   zorder=style["zorder"], alpha=style["alpha"], s=style["s"],
                   linewidths=0)

    ax.set_xlabel("Along-track distance (km)")
    ax.set_ylabel("Elevation / depth (m, geoid)")
    title = f"{os.path.basename(outpath).replace('.png', '')}  —  {beam_name}"
    if title_extra:
        title += f"  {title_extra}"
    ax.set_title(title, fontsize=8)
    if zlim is not None:
        ax.set_ylim(bottom=zlim[0], top=zlim[1])
    if dlim is not None:
        ax.set_xlim(left=dlim[0], right=dlim[1])
    ax.legend(loc="upper right", fontsize=7, markerscale=2)
    ax.grid(True, linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    fig.savefig(outpath, dpi=200)
    plt.close(fig)
    print(f"  Saved {outpath}")


def main():
    parser = argparse.ArgumentParser(description="Plot classified ICESat-2 photon curtains.")
    parser.add_argument("nc_file", help="Path to the .nc granule file")
    parser.add_argument("--beam", "-b", default=None,
                        help="Beam to plot (e.g. gt2l). Default: plot all beams.")
    parser.add_argument("--outdir", "-o", default=None,
                        help="Output directory for images (default: same as nc_file).")
    parser.add_argument("--zmin", type=float, default=None,
                        help="Minimum elevation to display (m). Data outside [-1e5, 1e5] is "
                             "always filtered regardless.")
    parser.add_argument("--zmax", type=float, default=None,
                        help="Maximum elevation to display (m).")
    parser.add_argument("--dmin", type=float, default=None,
                        help="Minimum along-track distance to display (km).")
    parser.add_argument("--dmax", type=float, default=None,
                        help="Maximum along-track distance to display (km).")
    parser.add_argument("--classes", default=None,
                        help="Slash-separated class codes to highlight (e.g. '1/40/41'). "
                             "Photons not in the list are reclassified as noise and shown "
                             "in grey. Default: show all classes. Pass '' to show all "
                             "photons as noise.")
    parser.add_argument("--h5", nargs="?", const=True, default=None,
                        help="ATL03 .h5 file to use as noise background. Supply a path, or "
                             "omit the path to search the IVERT cache for a file whose name "
                             "starts with the same granule ID as the .nc file.")
    args = parser.parse_args()

    nc_path = os.path.abspath(args.nc_file)
    if not os.path.exists(nc_path):
        sys.exit(f"File not found: {nc_path}")

    outdir = args.outdir or os.path.dirname(nc_path)
    os.makedirs(outdir, exist_ok=True)

    zlim = None
    if args.zmin is not None or args.zmax is not None:
        zlim = (args.zmin, args.zmax)

    dlim = None
    if args.dmin is not None or args.dmax is not None:
        dlim = (args.dmin, args.dmax)

    if args.classes is None:
        classes = None
    elif args.classes == "":
        classes = set()
    else:
        classes = {int(c) for c in args.classes.split("/")}

    print(f"Loading {os.path.basename(nc_path)} ...", flush=True)
    df = load_nc(nc_path)
    nc_stem = os.path.splitext(os.path.basename(nc_path))[0]

    # Resolve .h5 path for beam splitting and noise background
    if args.h5 is True:
        # --h5 given without a path: search the IVERT cache by granule ID
        h5_path = _find_h5(nc_path)
        if h5_path is None:
            print("Warning: --h5 given but no matching .h5 found in cache.", flush=True)
    elif args.h5:
        # --h5 given with an explicit path
        h5_path = os.path.abspath(args.h5)
        if not os.path.exists(h5_path):
            sys.exit(f".h5 file not found: {h5_path}")
    else:
        # --h5 not given: still try auto-find for beam splitting
        h5_path = _find_h5(nc_path)

    if h5_path:
        print(f"Found .h5: {os.path.basename(h5_path)}", flush=True)
        beam_dts = _beam_delta_times(h5_path)
        beams_to_plot = [args.beam] if args.beam else list(beam_dts.keys())

        for beam in beams_to_plot:
            if beam not in beam_dts:
                print(f"  Beam {beam} not in .h5, skipping.")
                continue
            dt_set = set(beam_dts[beam].tolist())
            df_beam = df[df["delta_time"].isin(dt_set)].copy()
            if df_beam.empty:
                print(f"  Beam {beam}: no photons in .nc, skipping.")
                continue

            # Load all raw photons from .h5 as noise background, then overlay classified.
            # along_track_km is computed from the full h5 beam; nc photons receive their
            # position by merging on delta_time so both share the same axis.
            df_bg = _load_h5_beam_photons(h5_path, beam)
            if not df_bg.empty:
                df_beam = df_beam.merge(
                    df_bg[["delta_time", "along_track_km"]], on="delta_time", how="left"
                ).dropna(subset=["along_track_km"])
                print(f"  Beam {beam}: {len(df_beam):,} classified + {len(df_bg):,} background photons", flush=True)
                df_plot = pd.concat([df_bg, df_beam], ignore_index=True)
            else:
                # No h5 background — compute along-track directly from nc photons
                df_beam = df_beam.sort_values("delta_time" if "delta_time" in df_beam.columns else "y").reset_index(drop=True)
                df_beam["along_track_km"] = _utm_along_track_km(df_beam["x"].values, df_beam["y"].values)
                print(f"  Beam {beam}: {len(df_beam):,} photons", flush=True)
                df_plot = df_beam

            outpath = os.path.join(outdir, f"{nc_stem}_{beam}.png")
            plot_beam(df_plot, beam, outpath, zlim=zlim, dlim=dlim, classes=classes)
    else:
        print("No .h5 found — plotting all photons together.", flush=True)
        beam = args.beam or "all_beams"
        sort_col = "delta_time" if "delta_time" in df.columns else "y"
        df = df.sort_values(sort_col).reset_index(drop=True)
        df["along_track_km"] = _utm_along_track_km(df["x"].values, df["y"].values)
        outpath = os.path.join(outdir, f"{nc_stem}_{beam}.png")
        plot_beam(df, beam, outpath, zlim=zlim, dlim=dlim, classes=classes)


if __name__ == "__main__":
    main()
