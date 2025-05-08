import os
from ftplib import FTP
from google.cloud import storage
from datetime import datetime
from zoneinfo import ZoneInfo

# FTP server configuration (embedded)
FTP_HOST     = 'ftp.nivoda.net'
FTP_USER     = 'leeladiamondscorporate@gmail.com'
FTP_PASS     = 'r[Eu;9NB'
REMOTE_NAT   = 'Leela Diamond_natural.csv'
REMOTE_LAB   = 'Leela Diamond_labgrown.csv'

# Google Cloud Storage configuration
# Bucket: sitemaps.leeladiamond.com
# Folder path: Sales Analytics
GCS_BUCKET       = 'sitemaps.leeladiamond.com'
GCS_FOLDER       = 'Sales Analytics'

# Service account key will be provided via the GCP_SA_KEY environment variable
# as the raw JSON content of the key file.

def init_gcs_client():
    # Write the service account JSON to a temporary file
    sa_key = os.environ['GCP_SA_KEY']
    key_path = '/tmp/gcp-sa.json'
    with open(key_path, 'w') as f:
        f.write(sa_key)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

    # Instantiate the client (project is inferred from credentials)
    return storage.Client()


def download_file(remote_filename: str, local_path: str):
    """
    Download a file from the configured FTP server to a local path.
    """
    # Use explicit connect/login to avoid passing port as user
    with FTP() as ftp:
        ftp.connect(FTP_HOST, 21)
        ftp.login(FTP_USER, FTP_PASS)
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_filename}', f.write)
    print(f"Downloaded {remote_filename} to {local_path}")


def upload_to_gcs(client, local_path: str, blob_name: str):
    """
    Upload a local file to GCS under the given blob name.
    """
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{GCS_BUCKET}/{blob_name}")


def main():
    # Initialize GCS client
    gcs_client = init_gcs_client()

    # Determine parity (Odd/even) based on IST date
    today_ist = datetime.now(ZoneInfo('Asia/Kolkata')).date()
    parity = 'Odd' if today_ist.day % 2 != 0 else 'even'

    # Local temporary file paths
    local_nat = '/tmp/natural.csv'
    local_lab = '/tmp/labgrown.csv'

    # Download both CSVs
    download_file(REMOTE_NAT, local_nat)
    download_file(REMOTE_LAB, local_lab)

    # Construct blob names with folder and parity
    nat_blob = f"{GCS_FOLDER}/Natural_{parity}.csv"
    lab_blob = f"{GCS_FOLDER}/Labgrown_{parity}.csv"

    # Upload to GCS
    upload_to_gcs(gcs_client, local_nat, nat_blob)
    upload_to_gcs(gcs_client, local_lab, lab_blob)

if __name__ == '__main__':
    main()
