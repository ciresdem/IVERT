
from cudem import regions, fetches

def download_granules(short_name="ATL03",
                      region=[-180,-90,180,90],
                      local_dir=os.getcwd(),
                      dates=['2021-01-01','2021-12-31'],
                      version='005',
                      fname_filter='',
                      force=False,
                      query_only=False,
                      fname_python_regex=r"\.h5\Z",
                      use_wget='backup',
                      download_only_matching_granules=True,
                      skip_granules_if_photon_db_exists=True,
                      quiet=False):
    # TODO: Fill in downloading ICESat-2 data using fetches rather than directly through CMR.


if __name__ == "__main__":
