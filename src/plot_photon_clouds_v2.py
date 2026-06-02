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
     1: dict(color="saddlebrown",  label="Ground",         zorder=2, alpha=1.0, s=3),
     2: dict(color="mediumseagreen", label="Canopy",       zorder=1, alpha=0.5, s=1),
     3: dict(color="darkgreen",    label="Canopy Top",     zorder=1, alpha=0.5, s=1),
     7: dict(color="red",          label="Built Structure", zorder=2, alpha=0.8, s=2),
    40: dict(color="darkorange",   label="Bathy Floor",    zorder=3, alpha=1.0, s=3),
    41: dict(color="dodgerblue",   label="Water Surface",  zorder=1, alpha=0.6, s=1),
}
DEFAULT_STYLE = dict(color="lightgrey", label="Other", zorder=0, alpha=0.3, s=1)


def _haversine_along_track(lons, lats):
    """Return cumulative along-track distance (km) for ordered lon/lat arrays."""
    R = 6371.0
    lon_r = np.radians(lons)
    lat_r = np.radians(lats)
    dlon = np.diff(lon_r)
    dlat = np.diff(lat_r)
    a = np.sin(dlat / 2) ** 2 + np.cos(lat_r[:-1]) * np.cos(lat_r[1:]) * np.sin(dlon / 2) ** 2
    dist = 2 * R * np.arcsin(np.sqrt(a))
    return np.concatenate([[0.0], np.cumsum(dist)])


def _find_h5(nc_path):
    """Try to locate the matching ATL03 .h5 file for a granule .nc."""
    # Extract granule id from nc filename (everything before first _W or _N bbox suffix)
    nc_base = os.path.basename(nc_path)
    # Strip bbox suffix: ATL03_<id>_subsetted_W...nc → ATL03_<id>_subsetted.h5
    stem = nc_base.split("_W")[0].split("_N")[0].split("_E")[0].split("_S")[0]
    if not stem.endswith("_subsetted"):
        stem = stem + "_subsetted"
    h5_name = stem + ".h5"

    search_dirs = [
        os.path.dirname(nc_path),
        os.path.expanduser("~/.ivert/cache/icesat2"),
        os.path.expanduser("~/.ivert/cache"),
    ]
    for d in search_dirs:
        candidate = os.path.join(d, h5_name)
        if os.path.exists(candidate):
            return candidate
        # Also try recursive glob in cache dirs
        hits = glob.glob(os.path.join(d, "**", h5_name), recursive=True)
        if hits:
            return hits[0]
    return None


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


def load_nc(nc_path):
    """Load the .nc granule into a DataFrame."""
    with netCDF4.Dataset(nc_path) as ds:
        df = pd.DataFrame({
            v: ds.variables[v][:].data if hasattr(ds.variables[v][:], "data")
               else np.array(ds.variables[v][:])
            for v in ds.variables
        })
    return df


def plot_beam(df_beam, beam_name, outpath, ylim=None, title_extra=""):
    """Plot one beam's photon curtain (along-track km vs elevation)."""
    # Sort along-track by delta_time (or by y/lat if unavailable)
    sort_col = "delta_time" if "delta_time" in df_beam.columns else "y"
    df_beam = df_beam.sort_values(sort_col).reset_index(drop=True)

    lon = df_beam["x"].values
    lat = df_beam["y"].values
    along_track = _haversine_along_track(lon, lat)
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
    if ylim:
        ax.set_ylim(ylim)
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
    parser.add_argument("--ylim", default=None,
                        help="Y-axis limits as 'min,max' (e.g. '-30,5').")
    parser.add_argument("--h5", default=None,
                        help="Explicit path to matching ATL03 .h5 file.")
    args = parser.parse_args()

    nc_path = os.path.abspath(args.nc_file)
    if not os.path.exists(nc_path):
        sys.exit(f"File not found: {nc_path}")

    outdir = args.outdir or os.path.dirname(nc_path)
    os.makedirs(outdir, exist_ok=True)

    ylim = None
    if args.ylim:
        lo, hi = args.ylim.split(",")
        ylim = (float(lo), float(hi))

    print(f"Loading {os.path.basename(nc_path)} ...", flush=True)
    df = load_nc(nc_path)
    nc_stem = os.path.splitext(os.path.basename(nc_path))[0]

    # Try to split by beam using the .h5
    h5_path = args.h5 or _find_h5(nc_path)
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
            outpath = os.path.join(outdir, f"{nc_stem}_{beam}.png")
            print(f"  Beam {beam}: {len(df_beam):,} photons", flush=True)
            plot_beam(df_beam, beam, outpath, ylim=ylim)
    else:
        print("No .h5 found — plotting all photons together.", flush=True)
        beam = args.beam or "all_beams"
        outpath = os.path.join(outdir, f"{nc_stem}_{beam}.png")
        plot_beam(df, beam, outpath, ylim=ylim)


if __name__ == "__main__":
    main()
