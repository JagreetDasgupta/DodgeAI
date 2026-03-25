"""
file_scanner.py — Recursively scans the SAP O2C data directory.

Groups JSONL part-files by their parent entity directory and returns
a structured manifest of all discovered files.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class FileInfo:
    """Metadata for a single data file."""
    directory: str          # parent entity directory name
    filename: str           # file basename
    full_path: str          # absolute path
    extension: str          # e.g., ".jsonl"
    size_bytes: int         # file size


@dataclass
class EntityManifest:
    """A group of part-files belonging to a single entity."""
    entity_name: str
    files: list[FileInfo] = field(default_factory=list)

    @property
    def total_size_bytes(self) -> int:
        return sum(f.size_bytes for f in self.files)

    @property
    def file_count(self) -> int:
        return len(self.files)


def scan_data_directory(root: Path) -> dict[str, EntityManifest]:
    """
    Recursively scan *root* for data files.

    Returns a dict keyed by entity name (directory name) containing
    an EntityManifest with all discovered part-files.
    """
    manifests: dict[str, EntityManifest] = {}

    for dirpath, _dirnames, filenames in os.walk(root):
        for fname in sorted(filenames):
            fpath = Path(dirpath) / fname
            entity_name = Path(dirpath).name

            # Skip hidden / system files
            if fname.startswith("."):
                continue

            info = FileInfo(
                directory=entity_name,
                filename=fname,
                full_path=str(fpath),
                extension=fpath.suffix,
                size_bytes=fpath.stat().st_size,
            )

            if entity_name not in manifests:
                manifests[entity_name] = EntityManifest(entity_name=entity_name)
            manifests[entity_name].files.append(info)

    return manifests


def print_manifest_summary(manifests: dict[str, EntityManifest]) -> None:
    """Pretty-print a manifest summary to stdout."""
    total_files = sum(m.file_count for m in manifests.values())
    total_bytes = sum(m.total_size_bytes for m in manifests.values())

    print(f"{'=' * 70}")
    print(f"DATA MANIFEST — {len(manifests)} entities, {total_files} files, "
          f"{total_bytes:,} bytes total")
    print(f"{'=' * 70}")

    for name in sorted(manifests):
        m = manifests[name]
        print(f"  {name}: {m.file_count} file(s), {m.total_size_bytes:,} bytes")
