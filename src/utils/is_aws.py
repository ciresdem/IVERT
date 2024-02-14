import os


def is_aws():
    "Return true if running in an Amazon Web Services environment. False otherwise."
    # The Amazon OS 2 EC2 instances we run have a /var/lib/cloud/instance/datasource file,
    # which contains "DataSourceEc2: DataSouceEc2" line. Look for that.
    datasource_path = "/var/lib/cloud/instance/datasource"
    return os.path.exists(datasource_path) and "DataSourceEc2" in open(datasource_path, 'r').read()


if __name__ == "__main__":
    print(is_aws())
