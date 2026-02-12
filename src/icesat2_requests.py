import ast
import dateparser
import datetime
import numpy
import os
import pandas
import time
import typing

import utils.configfile
from cudem import regions
from cudem.fetches import earthdata, fetches

class ICESat2Request():
    """A class for making requests to the NASA Harmony service and downloading ICESat-2 data files."""
    def __init__(self,
                 bbox: typing.Union[list, tuple],
                 download_directory: str,
                 subset_data: bool = True,
                 ):
        self.config = utils.configfile.Config()

        self.csv_obj = ICESat2RequestsCSV()
        self.is2_objects_dict = {}
        self.json_dict = {}

        self.bbox = bbox
        self.download_directory = os.path.normpath(os.path.expanduser(download_directory))
        self.subset_data = subset_data

    @staticmethod
    def _process_input_date_str(date_str: typing.Union[str, int]) -> str:
        """Process a date string and return a 'YYYY-MM-DD' string.

        Input date string can be anything that can be parsed by dateparser.parse().
        """
        try:
            if len(str(date_str)) == 8 and 19000000 <= int(date_str) <= 21000000:
                return datetime.datetime.strptime(str(date_str), "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass

        return dateparser.parse(str(date_str)).strftime("%Y-%m-%d")


    def make_icesat2_request(self,
                             use_previous_if_matching: bool = True,
                             verbose: bool = True) -> dict:
        """For a given bounding box, request all the ICESat-2 data from NASA's Harmony service to perform an IVERT job.

    This will entail requesting the ATL03, ATL08, and ATL24 datasets (if existing) for each granule within the bounding box.
    Bounding boxes are 6-tuples including (xmin, xmax, ymin, ymax, tmin, tmax).

    Return a dictionary with the dataset short_name as the key ("ATL03", e.g.) and the Harmony status JSON dict as the value.
    A dictionary of dictionaries, as it were.
    """
        json_responses = {}
        is2_objects = {}
        requests_csv = ICESat2RequestsCSV()
        new_request_made = False

        for short_name in ("ATL03", "ATL08", "ATL24"):
            # ATL03 granules go in "cmr". ATL08 & ATL24 granules go in "cmr/cmr".
            # This is hard-coded, keep ICESat-2 data in the EPSG:4326+3855 datum. It will be converted ad-hoc to DEM coordinates
            # when doing an IVERT validation.

            if use_previous_if_matching:
                previous_request_json = requests_csv.find_matching_request(short_name,
                                                                           self.bbox,
                                                                           only_unexpired=True,
                                                                           return_rows=False)

            else:
                previous_request_json = None

            # Create a CUDEM region object.
            region = regions.Region().from_list(list(self.bbox[0:4]))

            # Get date strings "yyyy-mm-dd:00:00:00" from each tmin, tmax.
            start_datestr = self._process_input_date_str(self.bbox[4])
            end_datestr = self._process_input_date_str(self.bbox[5])

            print("region:", region)
            print("download_dir:", self.download_directory)
            print("time_start:", start_datestr)
            print("time_end:", end_datestr)
            print("subset:", self.subset_data)
            print("short_name:", short_name)
            print("version:", "006")

            is2 = earthdata.IceSat2(src_region=region,
                                    outdir=self.download_directory,
                                    time_start=start_datestr,
                                    time_end=end_datestr,
                                    subset=self.subset_data,
                                    short_name=short_name,
                                    version="006")

            if use_previous_if_matching and previous_request_json is None:
                json = is2.harmony_make_request()
                new_request_made = True
                self.csv_obj.add_record(short_name, self.bbox, json, write_file=False)
            else:
                json = previous_request_json

            json_responses[short_name] = json
            is2_objects[short_name] = is2

        self.json_dict = json_responses
        self.is2_objects_dict = is2_objects

        if new_request_made:
            self.csv_obj.export()

        return json_responses

    def update_request_statuses(self,
                                update_csv: bool = True):
        """Update the requests for new data from harmony."""
        if self.json_dict is None or len(self.json_dict) == 0 or self.is2_objects_dict is None or len(self.is2_objects_dict) == 0:
            self.make_icesat2_request(use_previous_if_matching=True)

        any_json_changed = False

        for short_name in sorted(self.json_dict.keys()):
            job_id = self.json_dict[short_name]['jobID']
            previous_status = self.json_dict[short_name]['status']

            # If the job had already been marked "successful", just leave it as-is, no need to re-query.
            if previous_status != "successful":
                is2 = self.is2_objects_dict[short_name]
                json_response = is2.harmony_ping_for_status(job_id)
                self.json_dict[short_name] = json_response

                if update_csv:
                    self.csv_obj.update_record(short_name,
                                               self.bbox,
                                               json_response,
                                               write_file=False,
                                               fail_quietly=False)

                any_json_changed = True

        if any_json_changed and update_csv:
            self.csv_obj.export()

        return self.json_dict


    def are_requests_finished(self,
                              update_csv: bool = False,
                              return_separately: bool = False) -> typing.Union[bool, dict]:
        """Check on the status of an ICESat-2 request on Harmony, return whether it was successful and is finished.

        If 'return_separately', return a dictionary of statuses with dataset short_name as keys, status as values."""

        # Update the requests with new metadata from harmony, if it's not already completed.
        self.update_request_statuses(update_csv=update_csv)

        retvals = {}

        for short_name in sorted(self.json_dict.keys()):
            retvals[short_name] = self.json_dict[short_name]["status"]

        if return_separately:
            return retvals
        else:
            return numpy.all([retvals[short_name] == "successful" for short_name in list(retvals.keys())])


    def download_granules(self,
                          overwrite: bool = False,
                          wait_until_complete: bool = False,
                          sleep_interval_s: int = 1,
                          verbose: bool = True) -> list:
        """Download the granules for the request. NOTE: This does not process them, just downloads them."""
        if wait_until_complete:
            total_s = 0
            if verbose:
                print("Requesting..", end="")
            while not self.are_requests_finished(update_csv=False):
                if verbose:
                    if (total_s % 30) == 0:
                        print()
                        for short_name in sorted(self.json_dict.keys()):
                            print(f"{short_name}:",
                                  f"{len(self.granule_filenames(short_name, existing_only=False))} files,",
                                  f"{int(float(self.json_dict[short_name]["progress"]))}%,",
                                  f"{self.json_dict[short_name]["status"]}")
                    elif (total_s % 5) == 0:
                        print(".", end=" ", flush=True)
                time.sleep(sleep_interval_s)
                total_s += sleep_interval_s

            if verbose:
                print()
                for short_name in sorted(self.json_dict.keys()):
                    print(f"{short_name}:",
                          f"{len(self.granule_filenames(short_name, existing_only=False))} files,",
                          f"{int(float(self.json_dict[short_name]["progress"]))}%,",
                          f"{self.json_dict[short_name]["status"]}")

        total_outfiles = []

        for short_name in sorted(self.json_dict.keys()):
            json = self.json_dict[short_name]
            if "links" not in json.keys():
                continue

            is2 = self.is2_objects_dict[short_name]

            out_dir = self.download_directory

            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            # Get only links that actually link to a file, not ones that cancel or pause the job.
            subset_urls = [record['href'] for record in json["links"] if record['title'].startswith("ATL")]
            outfiles = [os.path.join(out_dir, record['title']) for record in json["links"] if record['title'].startswith("ATL")]

            if len(subset_urls) > 0 and verbose:
                print(f"{short_name}: {len(subset_urls)} files.")

            # Download each file.
            for href, of in zip(subset_urls, outfiles):
                if os.path.exists(of):
                    if overwrite:
                        os.remove(of)
                    else:
                        total_outfiles.append(of)
                        continue

                try:
                    for i in range(10):
                        try:
                            fetch_status = fetches.Fetch(href, headers=is2.headers).fetch_file(of)
                        except ConnectionError:
                            fetch_status = -1

                        if fetch_status == 0 and os.path.exists(of):
                            break
                        time.sleep(1)
                except KeyboardInterrupt as e:
                    # If the process is killed, go ahead and delete any in-progress download.
                    if os.path.exists(of):
                        os.remove(of)
                    raise e

                if fetch_status == 0:
                    total_outfiles.append(of)

        return total_outfiles


    def is_download_complete(self) -> bool:
        """Boolean query if all the files are complete."""

        for dset_name in self.json_dict.keys():
            out_dir = self.download_directory
            json = self.json_dict[dset_name]

            outfiles = [os.path.join(out_dir, record['title']) for record in json["links"] if
                        record['title'].startswith(dset_name)]

            for ofile in outfiles:
                if not os.path.exists(ofile):
                    return False

        return True


    def granule_filenames(self,
                          dataset: str="ATL03",
                          existing_only: bool=True) -> list:
        """Return the filename of each ATL granule of the given dataset name in this request."""

        out_dir = self.download_directory
        json = self.json_dict[dataset]

        outfiles = [os.path.join(out_dir, record['title']) for record in json["links"] if
                    record['title'].startswith(dataset)]

        if existing_only:
            return [ofile for ofile in outfiles if os.path.exists(ofile)]
        else:
            return outfiles



class ICESat2RequestsCSV():
    """A class for keeping track of requests to the NASA Harmony service and re-using queries when possible."""
    def __init__(self, config=None):
        if config is None:
            self.config = utils.configfile.Config()
        else:
            self.config = config
        self.csv_file = self.config.icesat2_requests_csv
        self.df = None

    def create_blank_csv(self,
                         overwrite: bool = False,
                         verbose: bool = True):
        if os.path.exists(self.csv_file):
            if overwrite:
                if verbose:
                    print("Removing old", os.path.basename(self.csv_file))
                os.remove(self.csv_file)
            else:
                return self.csv_file

        # Create a dictionary with a fake value row, turn into a pandas dataframe
        dummy_dict = {"atl_dataset": ["ATL03"],
                      "bbox": [(45.0, 46.0, -100.0, -99.0, 20240101, 20250101),],
                      "creation_date": ["2025-11-14T16:14:43.255Z",],
                      "expiration_date": ["2025-12-14T16:14:43.255Z",],
                      "job_id": ["6215eb03-ac96-4424-8918-2d4d92ecc546",],
                      "json": ["{'username': 'mmacferrin', 'status': 'successful', 'message': 'The job has completed successfully', 'progress': 100, 'createdAt': '2025-11-14T16:14:43.255Z', 'updatedAt': '2025-11-14T16:15:58.518Z', 'dataExpiration': '2025-12-14T16:14:43.255Z', 'links': [{'title': 'STAC catalog', 'href': 'https://harmony.earthdata.nasa.gov/stac/6215eb03-ac96-4424-8918-2d4d92ecc546/', 'rel': 'stac-catalog-json', 'type': 'application/json'}, {'href': 'https://harmony.earthdata.nasa.gov/service-results/harmony-prod-staging/public/6215eb03-ac96-4424-8918-2d4d92ecc546/129657020/ATL03_20241121130755_10122502_006_01_subsetted.h5', 'title': 'ATL03_20241121130755_10122502_006_01_subsetted.h5', 'type': 'application/x-hdf5', 'rel': 'data', 'bbox': [-126.65384, 26.954, -121.97036, 59.54566], 'temporal': {'start': '2024-11-21T13:07:55.700Z', 'end': '2024-11-21T13:16:26.845Z'}}, {'href': 'https://harmony.earthdata.nasa.gov/service-results/harmony-prod-staging/public/6215eb03-ac96-4424-8918-2d4d92ecc546/129657019/ATL03_20240902051911_11722406_006_01_subsetted.h5', 'title': 'ATL03_20240902051911_11722406_006_01_subsetted.h5', 'type': 'application/x-hdf5', 'rel': 'data', 'bbox': [-127.4359, 26.95422, -122.72066, 59.54422], 'temporal': {'start': '2024-09-02T05:19:11.423Z', 'end': '2024-09-02T05:27:41.020Z'}}, {'href': 'https://harmony.earthdata.nasa.gov/service-results/harmony-prod-staging/public/6215eb03-ac96-4424-8918-2d4d92ecc546/129657017/ATL03_20240304135951_11722206_006_01_subsetted.h5', 'title': 'ATL03_20240304135951_11722206_006_01_subsetted.h5', 'type': 'application/x-hdf5', 'rel': 'data', 'bbox': [-127.4372, 26.95473, -122.75229, 59.54589], 'temporal': {'start': '2024-03-04T13:59:50.098Z', 'end': '2024-03-04T14:08:20.304Z'}}, {'href': 'https://harmony.earthdata.nasa.gov/service-results/harmony-prod-staging/public/6215eb03-ac96-4424-8918-2d4d92ecc546/129657021/ATL03_20250220084730_10122602_006_01_subsetted.h5', 'title': 'ATL03_20250220084730_10122602_006_01_subsetted.h5', 'type': 'application/x-hdf5', 'rel': 'data', 'bbox': [-126.65383, 26.95405, -121.97025, 59.54568], 'temporal': {'start': '2025-02-20T08:47:31.100Z', 'end': '2025-02-20T08:56:01.740Z'}}, {'href': 'https://harmony.earthdata.nasa.gov/service-results/harmony-prod-staging/public/6215eb03-ac96-4424-8918-2d4d92ecc546/129657018/ATL03_20240822172808_10122402_006_01_subsetted.h5', 'title': 'ATL03_20240822172808_10122402_006_01_subsetted.h5', 'type': 'application/x-hdf5', 'rel': 'data', 'bbox': [-126.6858, 26.954, -121.97028, 59.54396], 'temporal': {'start': '2024-08-22T17:28:17.157Z', 'end': '2024-08-22T17:36:47.998Z'}}, {'href': 'https://harmony.earthdata.nasa.gov/jobs/6215eb03-ac96-4424-8918-2d4d92ecc546?page=1&limit=2000', 'title': 'The current page', 'type': 'application/json', 'rel': 'self'}], 'labels': [], 'request': 'https://harmony.earthdata.nasa.gov/ogc-api-edr/1.1.0/collections/C2596864127-NSIDC_CPRD/cube?bbox=-124.71%2C48.36%2C-124.68%2C48.44&datetime=2024-03-01T00%3A00%3A00Z%2F2025-03-01T00%3A00%3A00Z', 'numInputGranules': 5, 'jobID': '6215eb03-ac96-4424-8918-2d4d92ecc546', 'originalDataSize': '7.50 GiB', 'outputDataSize': '276.55 MiB', 'dataSizePercentChange': '96.40% reduction'}",]
                      }

        df = pandas.DataFrame.from_dict(dummy_dict)
        df = df.drop(labels=[0], axis='index')

        self.df = df

        self.export()

        return self.csv_file


    def find_matching_request(self,
                              atl_dataset,
                              bbox,
                              auto_clean_csv: bool = False,
                              only_unexpired: bool = True,
                              tolerance=1e-9,
                              return_rows: bool = False) -> typing.Union[dict, pandas.DataFrame, None]:
        """If a matching request exists in previous requests, return the json dict of it, or the whole subset dataframe
        if return_rows is selected. Else return None."""
        if self.df is None:
            self.open()

        assert self.df is not None

        if auto_clean_csv:
            self.clean_csv()

        # Now find all references that are matching, within the tolerance.
        matching_mask = self.df["bbox"].apply(lambda b: self.is_matching_bbox(b, bbox, tolerance=tolerance)) \
                        & (self.df["atl_dataset"] == atl_dataset.upper().strip())

        if only_unexpired and not auto_clean_csv:
            matching_mask = matching_mask & ~self.df["expiration_date"].apply(self.is_expired)

        if numpy.any(matching_mask):
            if return_rows:
                return self.df[matching_mask]
            else:
                return self.read_json(self.df[matching_mask].iloc[0]["json"])
        else:
            return None

    def add_record(self, atl_dataset, query_bbox, json, write_file: bool = True):
        """Add a new record to the database, given the Harmony JSON."""
        if self.df is None:
            self.open()

        if type(query_bbox) is str:
            query_bbox = ast.literal_eval(query_bbox)

        query_bbox = tuple(query_bbox)
        assert len(query_bbox) == 6

        if type(json) == str:
            json = self.read_json(json)

        data_dict = {"atl_dataset": [atl_dataset],
                     "bbox": [query_bbox],
                     "creation_date": [json["createdAt"]],
                     "expiration_date": [json["dataExpiration"]],
                     "job_id": [json["jobID"]],
                     "json": [str(json)]
                     }

        new_df = pandas.DataFrame.from_dict(data_dict, orient='columns')

        self.df = pandas.concat([self.df, new_df], ignore_index=True)

        if write_file:
            self.export()

        return


    def add_records(self, atl_datasets, query_bboxes, jsons, write_file: bool = True):
        """Add a list of new records to the database."""
        assert len(atl_datasets) == len(query_bboxes) == len(jsons)

        for i in range(len(atl_datasets)):
            self.add_record(atl_datasets[i], query_bboxes[i], jsons[i], write_file=False)

        if write_file:
            self.export()

        return


    def update_record(self,
                      atl_dataset: str,
                      query_bbox,
                      json: str,
                      refresh_first: bool = False,
                      write_file: bool = True,
                      fail_quietly: bool = False):
        """Update the record with a new JSON entry.

        If fail_quietly, just ignore if no matching record was found. Else, raise an error."""
        if refresh_first:
            self.open(read_again=True)

        matching_records = self.find_matching_request(atl_dataset,
                                                      query_bbox,
                                                      only_unexpired=False,
                                                      return_rows=True)

        if matching_records is None:
            if fail_quietly:
                return None
            else:
                raise ValueError(f"No matching harmony request found in {os.path.basename(self.csv_file)} "
                                 f"for '{atl_dataset}' in bounding box {str(query_bbox)}")

        if type(json) == str:
            json = ast.literal_eval(json)

        # Find the record with the same job_id
        json_job_id = json["jobID"]
        matching_records = matching_records[matching_records["job_id"] == json_job_id]

        if len(matching_records) == 0:
            if fail_quietly:
                return None
            else:
                raise ValueError(f"No matching harmony request found with jobID '{json_job_id}'")

        self.df.loc[matching_records.index, "json"] = str(json)

        if write_file:
            self.export()

        return self.df


    def update_records(self, atl_datasets, query_bboxes, jsons, write_file: bool = True):
        """Update multiple records with new JSON entries."""
        self.open(read_again=True)
        assert len(atl_datasets) == len(query_bboxes) == len(jsons)

        for i in range(len(atl_datasets)):
            self.update_record(atl_datasets[i], query_bboxes[i], jsons[i], write_file=False)

        if write_file:
            self.export()

        return


    @staticmethod
    def is_matching_bbox(bbox0, bbox1, tolerance=1e-9):
        """Given two ivert 6-tuple bboxes (xmin, xmax, ymin, ymax, tmin, tmax), return whether they are identical."""
        return abs(bbox0[0] - bbox1[0]) <= tolerance and \
                abs(bbox0[1] - bbox1[1]) <= tolerance and \
                abs(bbox0[2] - bbox1[2]) <= tolerance and \
                abs(bbox0[3] - bbox1[3]) <= tolerance and \
                bbox0[4] == bbox1[4] and \
                bbox0[5] == bbox1[5]


    @staticmethod
    def is_expired(dt_string):
        """Return true if a row in the database is expired, False otherwise."""
        ex_datetime = dateparser.parse(dt_string)
        now = datetime.datetime.now(datetime.UTC)
        return now >= ex_datetime


    def open(self,
             read_again: bool = False,
             create_if_nonexistent: bool = True):
        """Read the csv into a dataframe."""
        if self.df is None or read_again:
            if os.path.exists(self.csv_file):
                read_successful = False
                num_tries = 0
                # With parallelizing, sometimes another process is writing this CSV while we're trying to read it.
                # In that case, if the read_csv call errors-out, try again up to 20 times.
                while not read_successful and num_tries < 20:
                    try:
                        self.df = pandas.read_csv(self.csv_file, index_col=False)
                        read_successful = True
                    except (TypeError, pandas.errors.ParserError) as e:
                        num_tries += 1
                        if num_tries >= 20:
                            raise e
                        time.sleep(0.001)
                        continue

                # Read all the bounding boxes as tuples, not as strings.
                self.df["bbox"] = self.df["bbox"].apply(ast.literal_eval)
            elif create_if_nonexistent:
                self.create_blank_csv()
            else:
                raise FileNotFoundError(f"{self.csv_file} not found.")

        return self.df


    def export(self):
        self.df.to_csv(self.csv_file, index=False, header=True)


    def clean_csv(self, verbose=True):
        """Remove any expired requests from the csv file."""
        if self.df is None:
            self.df = self.open()

        # Apply this "is_expired" static method to the whole column.
        expired_mask = self.df["expiration_date"].apply(self.is_expired)

        if numpy.any(expired_mask):
            # Keep only records where the data isn't expired.
            self.df = self.df[~expired_mask]

            # Delete the previous csv, replace with the new one.
            if os.path.exists(self.csv_file):
                os.remove(self.csv_file)

            self.export()

        return self.df

    @staticmethod
    def read_json(json_str) -> dict:
        """Read the JSON text as a dictionary."""
        return ast.literal_eval(json_str)


if __name__ == "__main__":
    req = ICESat2RequestsCSV()
    req.open()
    # req.create_blank_csv(overwrite=True)
    df = req.df
    print(df)
    print(df["bbox"])
    print(type(df["bbox"].loc[0]))
    print(df.iloc[0])
    print(type(df.iloc[0]))
    print(df.index)
