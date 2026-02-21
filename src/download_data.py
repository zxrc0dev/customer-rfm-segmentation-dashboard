import shutil
import zipfile
import kagglehub

from pathlib import Path


def download_data(path: str, force: bool = False):
    """Download a Kaggle dataset to data/01_raw/.

    Skips the download if the target directory already contains files,
    unless *force* is True.
    """
    project_dir = Path(__file__).resolve().parents[1] / "data" / "01_raw"
    project_dir.mkdir(parents=True, exist_ok=True)

    # Skip download when data already exists (and force is off)
    if not force and any(project_dir.iterdir()):
        print(f"Data already present in {project_dir}, skipping download.")
        return

    cache_path = Path(
        kagglehub.dataset_download(path, force_download=force)
    )

    if cache_path.suffix == ".zip":
        with zipfile.ZipFile(cache_path, "r") as z:
            z.extractall(project_dir)
        print("Extracted to:", project_dir)

    elif cache_path.is_dir():
        for file in cache_path.iterdir():
            if file.is_file():
                shutil.copy(file, project_dir)
        print("Copied to:", project_dir)

    else:
        raise ValueError(f"Unexpected cache_path: {cache_path}")
