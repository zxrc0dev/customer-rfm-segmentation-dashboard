import shutil, zipfile, kagglehub
from pathlib import Path

def download_data(dataset_path: str, force: bool = False):
    raw_dir = Path(__file__).resolve().parents[1] / "data" / "01_raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    has_data = any(f for f in raw_dir.iterdir() if f.is_file() and not f.name.startswith('.'))

    if not force and has_data:
        print(f"Data exists in {raw_dir}. Skipping.")
        return

    print(f"Downloading {dataset_path}...")
    downloaded_path = Path(kagglehub.dataset_download(dataset_path, force_download=force))

    if downloaded_path.suffix == ".zip":
        with zipfile.ZipFile(downloaded_path, "r") as z:
            z.extractall(raw_dir)
    elif downloaded_path.is_dir():
        for f in downloaded_path.iterdir():
            if f.is_file(): shutil.copy(f, raw_dir)
    else:
        shutil.copy(downloaded_path, raw_dir)

    print(f"Files saved to: {raw_dir}")
