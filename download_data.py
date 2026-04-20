"""
Healthcare Data Warehouse — Dataset Downloader
===============================================
Downloads two datasets used in this project:

  1. Kaggle: prasad22/healthcare-dataset
     → ./data/kaggle/

  2. CMS DE-SynPUF: 2008–2010 Medicare synthetic claims
     → ./data/synpuf/sample_XX/

Usage:
  # Download both (default)
  python download_data.py

  # Download only one source
  python download_data.py --source kaggle
  python download_data.py --source synpuf

  # SynPUF options
  python download_data.py --source synpuf --samples 1 2 3
  python download_data.py --source synpuf --samples all
  python download_data.py --source synpuf --samples 1 --file-types beneficiary inpatient

Available --file-types:
  beneficiary, inpatient, outpatient, carrier, pde
"""

import os
import sys
import time
import zipfile
import argparse
import urllib.request
import urllib.error
from pathlib import Path


# ── Constants ────────────────────────────────────────────────────────────────

# Note: CMS serves these files case-sensitively; all paths must be lowercase.
CMS_BASE       = "https://www.cms.gov/research-statistics-data-and-systems/downloadable-public-use-files/synpufs/downloads"
DOWNLOADS_BASE = "http://downloads.cms.gov/files"

# The 2010 beneficiary file for Sample 1 was moved to a different path on CMS.
# All other samples use the standard CMS_BASE path.
CMS_2010_BENE_SAMPLE1 = "https://www.cms.gov/sites/default/files/2020-09/DE1_0_2010_Beneficiary_Summary_File_Sample_1.zip"

ALL_FILE_TYPES = ["beneficiary", "inpatient", "outpatient", "carrier", "pde"]


# ══════════════════════════════════════════════════════════════════════════════
# Kaggle
# ══════════════════════════════════════════════════════════════════════════════

def download_kaggle(output_dir: Path = Path("./data/kaggle")) -> None:
    """
    Download the prasad22/healthcare-dataset from Kaggle via kagglehub.

    Requires either:
      - kaggle.json credentials at ~/.kaggle/kaggle.json, or
      - KAGGLE_USERNAME and KAGGLE_KEY environment variables set.

    Install kagglehub with: pip install kagglehub
    """
    try:
        import kagglehub
    except ImportError:
        print("Error: kagglehub is not installed. Run: pip install kagglehub", file=sys.stderr)
        sys.exit(1)

    print("\nKaggle — prasad22/healthcare-dataset")
    print("─" * 50)

    cache_path = kagglehub.dataset_download("prasad22/healthcare-dataset")
    print(f"Downloaded to cache: {cache_path}")

    output_dir.mkdir(parents=True, exist_ok=True)
    for filename in os.listdir(cache_path):
        src = Path(cache_path) / filename
        dst = output_dir / filename
        if dst.exists():
            print(f"  ✓ Already exists, skipping: {filename}")
        else:
            src.rename(dst)
            print(f"  ✓ Moved: {filename} → {dst}")

    print(f"\nKaggle data ready at: {output_dir.resolve()}")


# ══════════════════════════════════════════════════════════════════════════════
# CMS DE-SynPUF
# ══════════════════════════════════════════════════════════════════════════════

def build_file_list(sample_num: int) -> list[dict]:
    """Return the list of files for a given sample number (1–20)."""
    s = str(sample_num)
    sl = s.lower()  # filenames on CMS server are lowercase

    # The 2010 beneficiary file for Sample 1 lives at a different URL (CMS migration quirk).
    bene_2010_url = (
        CMS_2010_BENE_SAMPLE1
        if sample_num == 1
        else f"{CMS_BASE}/de1_0_2010_beneficiary_summary_file_sample_{sl}.zip"
    )

    return [
        # Beneficiary Summary — one file per year
        {
            "type": "beneficiary",
            "url": f"{CMS_BASE}/de1_0_2008_beneficiary_summary_file_sample_{sl}.zip",
            "filename": f"de1_0_2008_beneficiary_summary_file_sample_{sl}.zip",
        },
        {
            "type": "beneficiary",
            "url": f"{CMS_BASE}/de1_0_2009_beneficiary_summary_file_sample_{sl}.zip",
            "filename": f"de1_0_2009_beneficiary_summary_file_sample_{sl}.zip",
        },
        {
            "type": "beneficiary",
            "url": bene_2010_url,
            "filename": f"de1_0_2010_beneficiary_summary_file_sample_{sl}.zip",
        },
        # Inpatient Claims
        {
            "type": "inpatient",
            "url": f"{CMS_BASE}/de1_0_2008_to_2010_inpatient_claims_sample_{sl}.zip",
            "filename": f"de1_0_2008_to_2010_inpatient_claims_sample_{sl}.zip",
        },
        # Outpatient Claims
        {
            "type": "outpatient",
            "url": f"{CMS_BASE}/de1_0_2008_to_2010_outpatient_claims_sample_{sl}.zip",
            "filename": f"de1_0_2008_to_2010_outpatient_claims_sample_{sl}.zip",
        },
        # Carrier Claims — split A/B; downloads.cms.gov keeps mixed case; sample 11A has .csv.zip quirk
        {
            "type": "carrier",
            "url": (
                f"{DOWNLOADS_BASE}/DE1_0_2008_to_2010_Carrier_Claims_Sample_{s}A.csv.zip"
                if sample_num == 11
                else f"{DOWNLOADS_BASE}/DE1_0_2008_to_2010_Carrier_Claims_Sample_{s}A.zip"
            ),
            "filename": f"de1_0_2008_to_2010_carrier_claims_sample_{sl}a.zip",
        },
        {
            "type": "carrier",
            "url": f"{DOWNLOADS_BASE}/DE1_0_2008_to_2010_Carrier_Claims_Sample_{s}B.zip",
            "filename": f"de1_0_2008_to_2010_carrier_claims_sample_{sl}b.zip",
        },
        # Prescription Drug Events
        {
            "type": "pde",
            "url": f"{DOWNLOADS_BASE}/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_{s}.zip",
            "filename": f"de1_0_2008_to_2010_prescription_drug_events_sample_{sl}.zip",
        },
    ]


def _progress_hook(block_num: int, block_size: int, total_size: int) -> None:
    downloaded = block_num * block_size
    if total_size > 0:
        pct = min(downloaded / total_size * 100, 100)
        mb_done = downloaded / 1_048_576
        mb_total = total_size / 1_048_576
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"\r  [{bar}] {pct:5.1f}%  {mb_done:.1f}/{mb_total:.1f} MB", end="", flush=True)
    else:
        print(f"\r  Downloaded {downloaded / 1_048_576:.1f} MB", end="", flush=True)


def _download_file(url: str, dest_path: Path, retries: int = 3) -> bool:
    """Download a single file, extract it, and delete the zip."""
    # After extraction the zip is deleted — check for the extracted CSV instead
    extracted = dest_path.with_suffix(".csv")
    if extracted.exists():
        print(f"  ✓ Already extracted, skipping: {extracted.name}")
        return True

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")

    for attempt in range(1, retries + 1):
        try:
            print(f"  Downloading: {dest_path.name}")
            urllib.request.urlretrieve(url, tmp_path, reporthook=_progress_hook)
            print()
            tmp_path.rename(dest_path)
            print(f"  Extracting: {dest_path.name}")
            with zipfile.ZipFile(dest_path, "r") as zf:
                zf.extractall(dest_path.parent)
            dest_path.unlink()
            return True
        except urllib.error.HTTPError as e:
            print(f"\n  HTTP {e.code} on attempt {attempt}/{retries}: {url}")
            if e.code == 404:
                print("  File not found — skipping.")
                if tmp_path.exists():
                    tmp_path.unlink()
                return False
        except urllib.error.URLError as e:
            print(f"\n  Network error on attempt {attempt}/{retries}: {e.reason}")
        except Exception as e:
            print(f"\n  Unexpected error on attempt {attempt}/{retries}: {e}")

        if attempt < retries:
            wait = 5 * attempt
            print(f"  Retrying in {wait}s…")
            time.sleep(wait)

    if tmp_path.exists():
        tmp_path.unlink()
    return False


def download_synpuf(
    samples: list[int] | None = None,
    file_types: list[str] | None = None,
    output_dir: Path = Path("./data/synpuf"),
) -> None:
    """
    Download CMS DE-SynPUF files.

    Args:
        samples:    List of sample numbers (1–20). Defaults to [1].
        file_types: Subset of file types to download. Defaults to all types.
        output_dir: Root directory for downloads.
    """
    if samples is None:
        samples = [1]
    if file_types is None:
        file_types = ALL_FILE_TYPES

    wanted = set(file_types)
    output_dir = Path(output_dir)

    print("\nCMS DE-SynPUF — Medicare Synthetic Claims")
    print("─" * 50)
    print(f"Samples    : {samples}")
    print(f"File types : {sorted(wanted)}")
    print(f"Output dir : {output_dir.resolve()}")
    print()

    total = success = failed = skipped = 0

    for sample_num in samples:
        sample_dir = output_dir / f"sample_{sample_num:02d}"
        print(f"── Sample {sample_num} → {sample_dir}")

        for file_info in build_file_list(sample_num):
            if file_info["type"] not in wanted:
                continue
            total += 1
            dest = sample_dir / file_info["filename"]
            if dest.exists():
                skipped += 1
                print(f"  ✓ Already exists: {file_info['filename']}")
                continue
            if _download_file(file_info["url"], dest):
                success += 1
            else:
                failed += 1

        print()

    print("─" * 50)
    print(f"Done.  Total: {total}  |  Downloaded: {success}  |  Skipped: {skipped}  |  Failed: {failed}")
    if failed:
        print(f"\n⚠️  {failed} file(s) failed. Re-run to retry — existing files are skipped automatically.")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_samples(raw: list[str]) -> list[int]:
    if len(raw) == 1 and raw[0].lower() == "all":
        return list(range(1, 21))
    samples = []
    for val in raw:
        try:
            n = int(val)
            if not 1 <= n <= 20:
                raise ValueError
            samples.append(n)
        except ValueError:
            print(f"Invalid sample number '{val}'. Must be 1–20 or 'all'.", file=sys.stderr)
            sys.exit(1)
    return samples


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download healthcare datasets for the data warehouse project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--source",
        choices=["kaggle", "synpuf", "both"],
        default="both",
        help="Which dataset(s) to download (default: both)",
    )
    parser.add_argument(
        "--samples",
        nargs="+",
        default=["1"],
        metavar="N",
        help="SynPUF sample numbers 1–20, or 'all' (default: 1)",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=ALL_FILE_TYPES,
        choices=ALL_FILE_TYPES,
        metavar="TYPE",
        help=f"SynPUF file types to download (default: all). Choices: {', '.join(ALL_FILE_TYPES)}",
    )
    parser.add_argument(
        "--kaggle-dir",
        default="./data/kaggle",
        help="Output directory for Kaggle data (default: ./data/kaggle)",
    )
    parser.add_argument(
        "--synpuf-dir",
        default="./data/synpuf",
        help="Output directory for SynPUF data (default: ./data/synpuf)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.source in ("kaggle", "both"):
        download_kaggle(output_dir=Path(args.kaggle_dir))

    if args.source in ("synpuf", "both"):
        download_synpuf(
            samples=_resolve_samples(args.samples),
            file_types=args.file_types,
            output_dir=Path(args.synpuf_dir),
        )


if __name__ == "__main__":
    main()
