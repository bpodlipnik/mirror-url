"""Validate remote names against the destination's actual filename behavior."""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .exceptions import ConfigError
from .security import PathSafety

COLLISION = "Distinct remote URLs map to the same local filename"


def selected_directory_targets(
    root: Path, suffixes: Iterable[str], *, max_filename_len: int
) -> List[Path]:
    """Validate all selected roots without changing any remote component names."""
    root = PathSafety._resolve_destination_root(root)
    names = FilenameMap(root)
    targets = []
    for suffix in suffixes:
        suffix = suffix.strip("/")
        target = root
        for part in suffix.split("/") if suffix else ():
            if (
                not part
                or part == "."
                or ".." in part
                or part.casefold() == ".mirror-url-state"
                or PathSafety._safe_filename(part, max_len=max_filename_len) != part
            ):
                raise ConfigError(f"Unsafe directory selection path: {suffix!r}")
            target /= part
        PathSafety._resolve_destination_root(target)
        try:
            names.key(target, directory=True)
        except ValueError as error:
            raise ConfigError(
                f"Selected remote directories collide or cannot be mapped: {error}"
            ) from error
        targets.append(target)
    return targets


def validate_move_targets(targets: Iterable[Path]) -> None:
    """Keep every selected payload tree separate from every MOVE archive."""
    targets = list(targets)
    if not targets:
        return
    try:
        archives = [target.with_name(target.name + "_obsolete") for target in targets]
        root = Path(os.path.commonpath([*targets, *archives]))
        names = FilenameMap(root)
        payload_keys = [names.key(target, directory=True) for target in targets]
        archive_keys = [names.key(archive, directory=True) for archive in archives]
        for payload in payload_keys:
            for archive in archive_keys:
                if payload[: len(archive)] == archive or archive[: len(payload)] == payload:
                    raise ValueError("MOVE archive overlaps a selected destination")
    except ValueError as error:
        raise ConfigError(f"Unsafe MOVE archive and destination selection: {error}") from error


def _stat_identity(info: os.stat_result) -> Tuple[int, ...]:
    """Use one timestamp namespace for path and handle identities.

    Windows Python 3.12+ fstat returns change time in st_ctime, while lstat
    retains creation time there. Both expose st_birthtime_ns. Preserve that
    established Windows receipt field; POSIX continues to use change time.
    """
    ctime = info.st_ctime_ns
    if sys.platform == "win32":
        ctime = getattr(info, "st_birthtime_ns", ctime)
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, ctime)


def case_sensitive(directory: Path) -> bool:
    """Probe the actual parent, including per-directory and mounted filesystems.

    Use an exclusively created random file, never a fixed user filename. Remove
    only that unchanged regular, singly linked file; preserve an unexpected entry.
    """
    directory = PathSafety._resolve_destination_root(directory)
    while not directory.exists():
        if directory == directory.parent:
            raise ValueError(f"No existing filename parent: {directory}")
        directory = directory.parent
    if not directory.is_dir():
        raise ValueError("Filename parent is not a directory")
    try:
        descriptor, name = tempfile.mkstemp(prefix=".mirror-url-case-", suffix="-aA", dir=directory)
        try:
            owned = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        probe = Path(name)
        try:
            try:
                alias = probe.with_name(probe.name.swapcase()).lstat()
            except FileNotFoundError:
                return True
            if (alias.st_dev, alias.st_ino) != (owned.st_dev, owned.st_ino):
                raise ValueError("Case probe encountered an unrelated filename; preserve it")
            return False
        finally:
            current = probe.lstat()
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_nlink != 1
                or _stat_identity(current) != _stat_identity(owned)
            ):
                raise ValueError(f"Case probe changed; preserving unexpected entry: {probe}")
            probe.unlink()
    except OSError as error:
        raise ValueError(f"Cannot determine destination case sensitivity: {directory}") from error


class FilenameMap:
    """Check file and directory prefixes without merging different remote names."""

    def __init__(self, root: Path):
        self.root = root
        self.nodes: Dict[Tuple[str, ...], Tuple[Tuple[str, ...], bool]] = {}
        self.folded: Dict[Tuple[Tuple[str, ...], str], str] = {}
        # Path equality folds case on Windows even when a directory itself is
        # case-sensitive. Tuple keys keep separate directories separate there.
        self.sensitivity: Dict[Tuple[str, ...], bool] = {}
        self.existing: Dict[Tuple[str, ...], Optional[Set[str]]] = {}

    def _existing_names(self, parent: Path) -> Optional[Set[str]]:
        key = parent.parts
        if key not in self.existing:
            try:
                with os.scandir(parent) as entries:
                    self.existing[key] = {
                        unicodedata.normalize("NFC", entry.name) for entry in entries
                    }
            except FileNotFoundError:
                self.existing[key] = None
            except OSError as error:
                raise ValueError(f"Cannot inspect destination filenames: {parent}") from error
        return self.existing[key]

    def key(self, local: Path, *, directory: bool = False) -> Tuple[str, ...]:
        """Check component identities; directory roots do not consume a file node."""
        raw = local.relative_to(self.root).parts
        normalized = tuple(unicodedata.normalize("NFC", name) for name in raw)
        for length, name in enumerate(normalized, 1):
            prefix, original = normalized[:length], raw[:length]
            is_file = length == len(raw) and not directory
            previous = self.nodes.get(prefix)
            if previous is not None:
                # Unicode-normalized aliases and file/directory conflicts remain
                # forbidden even on filesystems that could store both spellings.
                if previous != (original, is_file):
                    raise ValueError(COLLISION)
                continue
            parent = self.root.joinpath(*raw[: length - 1])
            folded = (normalized[: length - 1], name.casefold())
            other = self.folded.get(folded)
            if other is not None and other != name:
                if parent.parts not in self.sensitivity:
                    self.sensitivity[parent.parts] = case_sensitive(parent)
                if not self.sensitivity[parent.parts]:
                    raise ValueError(
                        f"{COLLISION}: destination is case-insensitive; "
                        "use a case-sensitive destination to preserve both original names"
                    )
            existing = self._existing_names(parent)
            if (
                existing is not None
                and name not in existing
                and parent.joinpath(raw[length - 1]).exists()
            ):
                raise ValueError(
                    f"{COLLISION}: existing local filename has different capitalization"
                )
            self.nodes[prefix] = (original, is_file)
            self.folded[folded] = name
        return normalized
