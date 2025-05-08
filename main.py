#!/usr/bin/env python3
import os
from ftplib import FTP
from google.cloud import storage
from datetime import datetime
from zoneinfo import ZoneInfo

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
# These should be set as GitHub Secrets or environment vars in your Actions workflow:
#
# FTP_HOST     = ftp.nivoda.net
# FTP_USER     = leeladiamondscorporate@gmail.com
# FTP_PASS     = r[Eu;9NB
# GCP_PROJECT_ID = your-gcp-project-id        # e.g. "gopal-gems-dia"
# GCP_SA_KEY   = <the full JSON you pasted>    # service account JSON
# GCS_BUCKET   = sitemaps.leeladiamond.com     # your bucket name
#
# (You can also override remote filenames if they ever change)
REMOTE_NAT_FILE = os.environ.get('RAW_NAT_FILE', 'Leela Diamond_natural.csv')
REMOTE_LAB_FILE = os.environ.get('RAW_LAB_FILE', 'Leela Diamond_labgrown.csv')
# ────────────────────────────────────────────────────────────────────────────────

def init_gcs_client():
    """Write the GCP_SA_KEY JSON to disk and initialize a Storage client."""
    key_json = os.environ['GCP_SA_KEY']
    key_path = '/tmp/gcp-sa.json'
    with open(key_path, 'w') as f:
        f.write(key_json)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

    return storage.Client(project=os.environ['GCP_PROJECT_ID'])

def download_file(ftp_host, ftp_user, ftp_pass, remote_file, local_path):
    """FTP-download a single file."""
    with FTP() as ftp:
        ftp.connect(ftp_host, 21)
        ftp.login(ftp_user, ftp_pass)
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f'RETR {remote_file}', f.write)
    print(f"✅ Downloaded {remote_file} → {local_path}")

def upload_to_gcs(client, bucket_name, local_path, blob_name):
    """Upload a local file into GCS under the given blob name."""
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded {local_path} → gs://{bucket_name}/{blob_name}")

def main():
    # 1) Init clients & paths
    ftp_host = os.environ['FTP_HOST']
    ftp_user = os.environ['FTP_USER']
    ftp_pass = os.environ['FTP_PASS']
    gcs_bucket = os.environ['GCS_BUCKET']
    gcs_client = init_gcs_client()

    # 2) Determine parity based on IST date
    today_ist = datetime.now(ZoneInfo('Asia/Kolkata')).date()
    parity = 'Odd' if (today_ist.day % 2) else 'even'

    # 3) Define tmp download paths
    tmp_nat = '/tmp/natural.csv'
    tmp_lab = '/tmp/labgrown.csv'

    # 4) Download from FTP
    download_file(ftp_host, ftp_user, ftp_pass, REMOTE_NAT_FILE, tmp_nat)
    download_file(ftp_host, ftp_user, ftp_pass, REMOTE_LAB_FILE, tmp_lab)

    # 5) Upload to GCS under Sales Analytics/
    base_path = 'Sales Analytics'
    nat_blob = f"{base_path}/Natural_{parity}.csv"
    lab_blob = f"{base_path}/Labgrown_{parity}.csv"

    upload_to_gcs(gcs_client, gcs_bucket, tmp_nat, nat_blob)
    upload_to_gcs(gcs_client, gcs_bucket, tmp_lab, lab_blob)

if __name__ == "__main__":
    main()
