"""Download the financial dataset from Kaggle and upload to ADLS raw container."""
import os
import zipfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def download_dataset(dest_dir: str = "data/raw") -> Path:
    """Download adhoppin/financial-data from Kaggle to dest_dir."""
    import kaggle  # noqa: PLC0415  (lazy import — requires KAGGLE_USERNAME/KEY in env)

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(
        dataset=os.environ["KAGGLE_DATASET"],
        path=str(dest),
        unzip=False,
    )

    zip_path = dest / "financial-data.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest)
    zip_path.unlink()

    print(f"Dataset extracted to {dest}")
    return dest


def upload_to_adls(local_dir: str = "data/raw") -> None:
    """Upload all CSV files from local_dir to ADLS raw container."""
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    account_name = os.environ["ADLS_ACCOUNT_NAME"]
    container = os.environ.get("ADLS_CONTAINER_RAW", "raw")

    credential = DefaultAzureCredential()
    client = DataLakeServiceClient(
        account_url=f"https://{account_name}.dfs.core.windows.net",
        credential=credential,
    )

    fs_client = client.get_file_system_client(container)

    for csv_path in Path(local_dir).glob("**/*.csv"):
        dest_path = f"financial-data/{csv_path.name}"
        file_client = fs_client.get_file_client(dest_path)

        with open(csv_path, "rb") as f:
            data = f.read()

        file_client.upload_data(data, overwrite=True)
        print(f"Uploaded {csv_path.name} → abfss://{container}@{account_name}.dfs.core.windows.net/{dest_path}")


if __name__ == "__main__":
    local_dir = download_dataset()
    upload_to_adls(str(local_dir))
