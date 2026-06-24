"""CleanupMixin: Obsolete-file cleanup (preview/move/delete policies).

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import logging
import shutil
import time
from pathlib import Path
from typing import Optional, Set
from urllib.parse import unquote, urlparse

from ..decorators import log_performance
from ..enums import CleanupPolicy
from ..security import PathSafety


class CleanupMixin:
    @log_performance("clean_obsolete")
    def clean_obsolete(self, remote_files: Set[str]) -> None:
        if self.config.cleanup_policy == CleanupPolicy.SAFE_NO_DELETE:
            logging.debug("Cleanup skipped: SAFE_NO_DELETE mode")
            return

        is_preview = self.config.cleanup_policy == CleanupPolicy.PREVIEW or self.config.dry_run

        # Check target_dir
        if self.target_dir is None:
            logging.debug("Cleanup skipped: target_dir is None")
            return

        if not self.target_dir.exists():
            logging.debug("Cleanup skipped: target directory does not exist")
            return

        # Check target_parsed
        if self.target_parsed is None:
            logging.debug("Cleanup skipped: target_parsed is None")
            return

        # Delete confirmation
        if (
            self.config.cleanup_policy == CleanupPolicy.DELETE
            and self.config.confirm_delete
            and not is_preview
        ):
            obsolete_count = self._count_obsolete_files(remote_files)
            if obsolete_count > 0:
                response = (
                    input(f"⚠️ Confirm deletion of {obsolete_count} files? [yes/N]: ")
                    .strip()
                    .lower()
                )
                if response != "yes":
                    logging.info("Deletion cancelled by user")
                    return

        # Build expected files set
        expected: Set[Path] = set()
        for url in remote_files:
            try:
                url_path = urlparse(url).path
                target_path = self.target_parsed.path

                if url_path.startswith(target_path):
                    rel = unquote(url_path[len(target_path) :].lstrip("/"))
                    local = PathSafety.safe_join(
                        self.target_dir,
                        *rel.split("/"),
                        max_depth=self.config.max_depth,
                        max_filename_len=self.config.max_filename_len,
                    )
                    if local and PathSafety.is_subpath(self.target_dir, local):
                        expected.add(local)
            except Exception as e:
                logging.debug(f"Error building expected path for {url}: {e}")
                continue

        # Initialize counters
        files_would_delete = 0
        dirs_would_delete = 0
        moved_files = 0
        moved_dirs = 0
        deleted_files = 0
        deleted_dirs = 0
        failed_operations = 0
        prefix = self._get_prefix()

        # FIX v2.0.2: For preview mode, just show what would be deleted
        if is_preview:
            logging.info(f"{prefix}🔍 PREVIEW MODE - Scanning for obsolete files...")

            # Check files
            try:
                for item in self.target_dir.rglob("*"):
                    if item.is_file():
                        if item not in expected:
                            files_would_delete += 1
                            if is_preview:
                                logging.info(f"[PREVIEW] Would delete: {item}")
                            # ... rest of logic ...
            except (RuntimeError, PermissionError, FileNotFoundError):
                logging.warning(
                    "Symlink loop or permission error detected during cleanup. Skipping."
                )

            # Check empty directories
            for item in sorted(
                self.target_dir.rglob("*"), key=lambda p: len(p.parts), reverse=True
            ):
                if item.is_dir() and item != self.target_dir:
                    try:
                        is_empty = not any(item.iterdir())
                        if is_empty:
                            dirs_would_delete += 1
                            logging.info(f"[PREVIEW] Would delete empty directory: {item}")
                    except (FileNotFoundError, PermissionError):
                        continue

            # Store preview counts in metrics
            self.metrics.metrics["files_would_delete"] = files_would_delete
            self.metrics.metrics["dirs_would_delete"] = dirs_would_delete

            logging.info("-" * 50)
            logging.info("🔍 PREVIEW SUMMARY:")
            logging.info(f"  Files that would be deleted: {files_would_delete}")
            logging.info(f"  Directories that would be deleted: {dirs_would_delete}")
            logging.info("  No actual deletions performed (dry-run/preview mode)")
            logging.info("-" * 50)

            return

        # Setup for DELETE or MOVE modes
        obsolete_dir: Optional[Path] = None
        if self.config.cleanup_policy == CleanupPolicy.MOVE:
            obsolete_dir = self.target_dir.parent / f"{self.target_dir.name}_obsolete"
            try:
                obsolete_dir.mkdir(parents=True, exist_ok=True)
                logging.info(f"📦 Obsolete files will be moved to: {obsolete_dir}")
            except Exception as e:
                logging.error(f"Failed to create obsolete directory {obsolete_dir}: {e}")
                logging.warning("Falling back to DELETE mode")
                self.config.cleanup_policy = CleanupPolicy.DELETE

        logging.info(f"{prefix}Scanning for obsolete files...")

        # Collect files and directories to process
        files_to_process = []
        dirs_to_check = []
        for item in self.target_dir.rglob("*"):
            if item.is_file():
                files_to_process.append(item)
            elif item.is_dir():
                dirs_to_check.append(item)

        # Process files
        for item in files_to_process:
            if item in expected:
                continue

            if self.config.cleanup_policy == CleanupPolicy.MOVE and obsolete_dir:
                try:
                    rel_path = item.relative_to(self.target_dir)
                    dest = obsolete_dir / rel_path
                    dest.parent.mkdir(parents=True, exist_ok=True)

                    if dest.exists():
                        timestamp = int(time.time() * 1000)
                        dest = dest.with_name(f"{dest.stem}_{timestamp}{dest.suffix}")
                    shutil.move(
                        str(item), str(dest)
                    )  # FIX: Handles cross-filesystem moves without OSError
                    self.cache_manager.cleanup_file_metadata(item)

                    if hasattr(self, "fs_cache"):
                        self.fs_cache.invalidate(item)

                    moved_files += 1
                    logging.info(f"Moved obsolete: {item} → {dest}")
                except Exception as e:
                    logging.error(f"Failed to move {item}: {e}")
                    failed_operations += 1
            else:  # DELETE mode
                try:
                    item.unlink()
                    self.cache_manager.cleanup_file_metadata(item)

                    if hasattr(self, "fs_cache"):
                        self.fs_cache.invalidate(item)

                    deleted_files += 1
                    logging.info(f"Deleted obsolete: {item}")
                except Exception as e:
                    logging.error(f"Failed to delete {item}: {e}")
                    failed_operations += 1

        logging.info(f"{prefix}Cleaning up empty directories...")
        changed = True
        iteration = 0
        max_iterations = 10

        while changed and iteration < max_iterations:
            changed = False
            iteration += 1

            for item in sorted(dirs_to_check, key=lambda p: len(p.parts), reverse=True):
                if not item.is_dir() or item == self.target_dir:
                    continue

                try:
                    if not item.exists():
                        continue

                    is_empty = not any(item.iterdir())
                    if not is_empty:
                        continue

                    if (
                        self.config.cleanup_policy == CleanupPolicy.MOVE
                        and obsolete_dir is not None
                    ):
                        try:
                            rel_path = item.relative_to(self.target_dir)
                            dest = obsolete_dir / rel_path
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            # FIX: shutil.move() already relocates the
                            # directory to `dest`. The previous code followed
                            # it with `item.rename(dest)`, but `item` no longer
                            # exists at that point, so rename() raised
                            # FileNotFoundError — which was caught below and
                            # silently fell through to rmdir(), leaving
                            # moved_dirs uncounted and `changed` unset (stalling
                            # the empty-dir cleanup loop). Removed the redundant
                            # rename so the success path runs.
                            if dest.exists():
                                timestamp = int(time.time() * 1000)
                                dest = dest.with_name(f"{dest.name}_{timestamp}")
                            shutil.move(str(item), str(dest))
                            moved_dirs += 1
                            logging.info(f"Moved obsolete dir: {item} → {dest}")
                            changed = True
                        except Exception:
                            try:
                                item.rmdir()
                                deleted_dirs += 1
                                changed = True
                                logging.info(f"Removed empty dir: {item}")
                            except Exception as e:
                                logging.debug(f"Error removing directory {item}: {e}")
                    else:
                        try:
                            item.rmdir()
                            deleted_dirs += 1
                            changed = True
                            logging.info(f"Removed empty dir: {item}")
                        except Exception as e:
                            logging.debug(f"Error removing directory {item}: {e}")
                except Exception:
                    pass

        logging.info(f"{prefix}Cleaning up stale cache metadata...")
        try:
            stale_count = self.cache_manager.cleanup_stale_metadata(expected)
            if stale_count > 0:
                logging.info(f"Removed {stale_count} stale metadata entries")
        except Exception as e:
            logging.warning(f"Failed to cleanup stale metadata: {e}")

        logging.info("-" * 50)

        if self.config.cleanup_policy == CleanupPolicy.MOVE:
            if moved_files > 0 or moved_dirs > 0:
                logging.info("📦 MOVE COMPLETE:")
                logging.info(f"  Files moved: {moved_files}")
                logging.info(f"  Directories moved: {moved_dirs}")
                logging.info(f"  Destination: {obsolete_dir}")
            else:
                logging.info("📦 No obsolete files to move")

            # Store move counts in metrics
            self.metrics.metrics["files_moved"] = moved_files
            self.metrics.metrics["dirs_moved"] = moved_dirs
        else:  # DELETE mode
            if deleted_files > 0 or deleted_dirs > 0:
                logging.info("🗑️ DELETE COMPLETE:")
                logging.info(f"  Files deleted: {deleted_files}")
                logging.info(f"  Directories deleted: {deleted_dirs}")
            else:
                logging.info("🗑️ No obsolete files to delete")

            # Store delete counts in metrics
            self.metrics.metrics["files_deleted"] = deleted_files
            self.metrics.metrics["dirs_deleted"] = deleted_dirs

        if failed_operations > 0:
            logging.warning(f"⚠️ {failed_operations} operations failed during cleanup")

        # Store failed operations count in metrics
        self.metrics.metrics["cleanup_failed_operations"] = failed_operations

        logging.info("-" * 50)

    def _count_obsolete_files(self, remote_files: Set[str]) -> int:
        """Count obsolete files for preview."""
        expected = set()

        for url in remote_files:
            try:
                url_path = urlparse(url).path
                target_path = self.target_parsed.path

                if url_path.startswith(target_path):
                    rel = unquote(url_path[len(target_path) :].lstrip("/"))
                    local = PathSafety.safe_join(
                        self.target_dir,
                        *rel.split("/"),
                        max_depth=self.config.max_depth,
                        max_filename_len=self.config.max_filename_len,
                    )

                    if local and PathSafety.is_subpath(self.target_dir, local):
                        expected.add(local)
            except Exception:
                continue

        count = 0
        for item in self.target_dir.rglob("*"):
            if item.is_file() and item not in expected:
                count += 1

        return count
