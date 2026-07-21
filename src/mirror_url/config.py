"""Pydantic config models + loaders.

Migrated verbatim from ``mirror_url.py``:
``ConfigSchema`` (orig. 9538-9576), ``validate_config_file`` (orig. 9578-9601),
``expand_env_vars`` (orig. 9603-9650), ``MirrorConfig`` (orig. 13439-13837),
``load_config_from_args`` (orig. 13842-13954).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
from pathlib import Path
from re import error as re_error
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .constants import (
    ADAPTIVE_ASYNC_ENABLED,
    ADAPTIVE_ERROR_THRESHOLD,
    ADAPTIVE_START_CONCURRENCY,
    AUTO_CONCURRENCY_ENABLED,
    BATCH_SIZE,
    CHUNK_TIMEOUT_MULTIPLIER,
    CONTENT_HASH_THRESHOLD,
    DEFAULT_ASYNC_WORKERS,
    DEFAULT_CACHE_MAX_AGE_DAYS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RGET_LIST_MAX_AGE,
    DEFAULT_TIMEOUT,
    DEFAULT_WORKERS,
    FS_CACHE_TTL_SECONDS,
    HTML_CACHE_MAX_AGE_HOURS,
    MAX_BATCH_SIZE,
    MAX_CACHE_AGE_DAYS,
    MAX_CHUNKS_PER_FILE,
    MAX_DIRECTORY_DEPTH,
    MAX_FILENAME_LENGTH,
    MAX_PARALLEL_CHUNKS_TOTAL,
    MAX_SYMLINK_DEPTH,
    MAX_SYMLINKS_PER_DIR,
    MAX_TIMEOUT,
    MAX_WORKERS_HARD_LIMIT,
    MEMORY_CACHE_MAX_SIZE,
    MIN_TIMEOUT,
    PARALLEL_DOWNLOAD_ENABLED,
    PARALLEL_SCAN_THRESHOLD,
    REQUEST_DELAY,
    STREAMING_MIN_FILE_SIZE_MB,
    SYMLINK_BOMB_THRESHOLD,
    TARGET_BATCH_TIME_SECONDS,
)
from .enums import CleanupPolicy, ScanMode
from .exceptions import ConfigError
from .utils import trim_url


def expand_env_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expand environment variables in config values recursively.

    Expands ${VAR} patterns anywhere in string values, not just entire values.

    Examples:
        >>> os.environ['MIRROR_URL'] = 'https://example.com'
        >>> expand_env_vars({'base_url': '${MIRROR_URL}/files/'})
        {'base_url': 'https://example.com/files/'}

        >>> expand_env_vars({'path': '/home/${USER}/docs'})
        {'path': '/home/username/docs'}

    Args:
        config_dict: Configuration dictionary with possible ${VAR} placeholders

    Returns:
        Dictionary with environment variables expanded
    """

    def expand_value(value: Any) -> Any:
        """Helper to expand variables in a value."""
        if isinstance(value, str):
            # Find all ${VAR} patterns
            pattern = r"\$\{([^}]+)\}"

            def replace_var(match: re.Match) -> str:
                var_name = match.group(1)
                # Return environment variable value if found, otherwise keep original placeholder
                return os.environ.get(var_name, match.group(0))

            # Replace all occurrences of ${VAR} with their environment values
            return re.sub(pattern, replace_var, value)
        return value

    expanded = {}
    for key, value in config_dict.items():
        if isinstance(value, dict):
            expanded[key] = expand_env_vars(value)
        elif isinstance(value, list):
            expanded[key] = [
                expand_env_vars({str(i): v})[str(i)] if isinstance(v, dict) else expand_value(v)
                for i, v in enumerate(value)
            ]
        else:
            expanded[key] = expand_value(value)

    return expanded


class ConfigSchema(BaseModel):
    """Strict configuration schema for validation"""

    base_url: str
    dest_path: str
    log_path: str
    dir_suffix: Optional[str] = ""
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=MAX_WORKERS_HARD_LIMIT)
    timeout: int = Field(default=DEFAULT_TIMEOUT, ge=MIN_TIMEOUT, le=MAX_TIMEOUT)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0, le=10)
    retry_delay: int = Field(default=DEFAULT_RETRY_DELAY, ge=1, le=60)
    cache_max_age: int = Field(default=DEFAULT_CACHE_MAX_AGE_DAYS, ge=0, le=MAX_CACHE_AGE_DAYS)
    max_depth: int = Field(default=MAX_DIRECTORY_DEPTH, ge=1, le=100)
    max_filename_len: int = Field(default=MAX_FILENAME_LENGTH, ge=1, le=512)
    bandwidth_limit: Optional[float] = Field(default=None, gt=0, le=1000)
    trusted_server: bool = False
    missing_files: bool = False
    security_validation: bool = True
    http2: bool = True
    cleanup_policy: str = "safe"
    # NEW v3.0.0 fields
    parallel_downloads: bool = Field(default=PARALLEL_DOWNLOAD_ENABLED)
    max_chunks_per_file: int = Field(default=MAX_CHUNKS_PER_FILE, ge=1, le=20)
    min_chunk_size_mb: int = Field(default=10, ge=1, le=100)
    max_parallel_chunks_total: int = Field(default=MAX_PARALLEL_CHUNKS_TOTAL, ge=10, le=200)
    chunk_assembly_dir: Optional[str] = None

    @field_validator("base_url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v.rstrip("/")

    @field_validator("cleanup_policy")
    @classmethod
    def validate_cleanup_policy(cls, v: str) -> str:
        allowed = ["safe", "preview", "delete", "move"]
        if v not in allowed:
            raise ValueError(f"cleanup_policy must be one of {allowed}")
        return v


def validate_config_file(config_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Validate configuration file against schema.

    Args:
        config_path: Path to config file

    Returns:
        Tuple of (valid, error_message)
    """
    try:
        with open(config_path) as f:
            if config_path.suffix.lower() in [".yaml", ".yml"]:
                config_data = yaml.safe_load(f)
            else:
                config_data = json.load(f)

        config_data = expand_env_vars(config_data)
        ConfigSchema(**config_data)
        return True, None
    except ValidationError as e:
        return False, f"Validation error: {e}"
    except Exception as e:
        return False, f"Config load error: {e}"


class MirrorConfig(BaseModel):
    """Configuration for MirrorURL with Pydantic v2 validation and parallel downloads"""

    base_url: str
    dest_path: Path
    log_path: Path
    print_logs: bool = False
    _silent: bool = False
    dir_suffix: str = ""
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=MAX_WORKERS_HARD_LIMIT)
    timeout: int = Field(default=DEFAULT_TIMEOUT, ge=MIN_TIMEOUT, le=MAX_TIMEOUT)
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: int = DEFAULT_RETRY_DELAY
    debug: bool = False
    dry_run: bool = False
    file_filters: List[str] = Field(default_factory=list)
    exclude_dirs: List[str] = Field(default_factory=list)
    cleanup_policy: CleanupPolicy = CleanupPolicy.SAFE_NO_DELETE
    quick: bool = False
    no_rget_list: bool = False
    rget_list_max_age: int = DEFAULT_RGET_LIST_MAX_AGE
    force_rget_list: bool = False
    no_cache: bool = False
    refresh_cache: bool = False
    cache_max_age: int = Field(default=DEFAULT_CACHE_MAX_AGE_DAYS, ge=0, le=MAX_CACHE_AGE_DAYS)
    no_etag: bool = False
    missing_files: bool = False
    use_shared_log: bool = False
    scan_mode: ScanMode = ScanMode.ADAPTIVE
    parallel_threshold: int = PARALLEL_SCAN_THRESHOLD
    benchmark: bool = False
    http2: bool = True
    stats: bool = False
    max_depth: int = Field(default=MAX_DIRECTORY_DEPTH, ge=1, le=100)
    max_filename_len: int = Field(default=MAX_FILENAME_LENGTH, ge=1, le=512)
    safe_urls: bool = True
    confirm_delete: bool = False
    quiet: bool = False
    verbose: bool = False
    metrics_json: Optional[Path] = None
    progress_bar: bool = False
    async_metadata: bool = True
    async_workers: int = Field(default=DEFAULT_ASYNC_WORKERS, ge=1, le=200)
    content_hash_small_files: bool = True
    trusted_server: bool = False
    request_delay: float = Field(default=REQUEST_DELAY, ge=0.001, le=1.0)
    cache_html: bool = True
    html_cache_max_age: int = Field(default=HTML_CACHE_MAX_AGE_HOURS, ge=1, le=168)
    hash_algorithm: str = Field(
        default="md5",
        pattern="^(md5|sha256|blake2b)$",
        description="Hash algorithm for file integrity checks",
    )
    adaptive_async: bool = ADAPTIVE_ASYNC_ENABLED
    adaptive_error_threshold: float = ADAPTIVE_ERROR_THRESHOLD
    adaptive_start_concurrency: int = ADAPTIVE_START_CONCURRENCY
    security_validation: bool = True
    circuit_breaker_enabled: bool = True
    bandwidth_limit: Optional[float] = Field(default=None, gt=0)
    enable_resume: bool = True
    max_concurrent_downloads: int = Field(default=10, ge=1, le=50)  # Increased for v3.0.2
    download_queue_size: int = Field(default=1000, ge=100)
    handle_symlinks: bool = False
    symlink_mode: str = "skip"
    circuit_breaker_downloads: bool = Field(default=True)
    max_symlink_depth: int = Field(default=MAX_SYMLINK_DEPTH, ge=1, le=50)
    max_symlinks_per_dir: int = Field(default=MAX_SYMLINKS_PER_DIR, ge=1, le=1000)
    symlink_bomb_threshold: int = Field(default=SYMLINK_BOMB_THRESHOLD, ge=100, le=100000)
    adaptive_batch_processing: bool = Field(default=True)
    initial_batch_size: int = Field(default=BATCH_SIZE, ge=10, le=1000)
    max_batch_size: int = Field(default=MAX_BATCH_SIZE, ge=10, le=2000)
    target_batch_time: float = Field(default=TARGET_BATCH_TIME_SECONDS, ge=0.1, le=5.0)
    memory_cache_size: int = Field(default=MEMORY_CACHE_MAX_SIZE, ge=1000, le=1000000)
    use_disk_backed_sets: bool = Field(default=False)
    disk_cache_dir: Optional[Path] = None
    fast_parsing_fallback: bool = Field(default=True)
    http2_pipelining: bool = Field(default=True)
    connection_pool_prewarm: bool = Field(default=True)
    fs_cache_ttl: float = Field(default=FS_CACHE_TTL_SECONDS, ge=0.1, le=30.0)

    # NEW v3.0.0 fields
    # Bounds enforced by model_validator (raising ConfigError) when parallel mode is on.
    max_chunks_per_file: int = Field(default=MAX_CHUNKS_PER_FILE)
    min_chunk_size_mb: int = Field(default=10)
    max_parallel_chunks_total: int = Field(default=MAX_PARALLEL_CHUNKS_TOTAL)
    chunk_assembly_dir: Optional[Path] = Field(default=None)
    chunk_timeout_multiplier: float = Field(default=CHUNK_TIMEOUT_MULTIPLIER, ge=1.0, le=3.0)
    # NEW v3.0.6 fields
    auto_concurrency: bool = Field(default=AUTO_CONCURRENCY_ENABLED)
    health_check_port: int = Field(default=8080, ge=1024, le=65535)
    # NEW v3.0.6
    use_shared_thread_pool: bool = Field(
        default=False, description="Use shared thread pool for all operations"
    )  # NEW: Default to dedicated pools

    # NEW v3.0.7: Download mode fields
    parallel_downloads: bool = Field(default=False)
    streaming_parallel: bool = Field(default=False)
    sequential_downloads: bool = Field(default=False)

    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=False,
    )

    # Parallel download optimization flags
    parallel_optimization_mode: str = Field(
        default="balanced",
        pattern="^(conservative|balanced|aggressive)$",
        description="Optimization mode: conservative (safe), balanced (default), aggressive (max speed)",
    )
    disable_rate_scaling: bool = Field(
        default=False, description="Disable rate limiter scaling for parallel downloads"
    )
    use_dedicated_download_pool: bool = Field(
        default=True, description="Use dedicated thread pool for downloads instead of shared"
    )

    # NEW: Auto-selection configuration
    auto_select_method: bool = Field(
        default=True, description="Automatically select best download method"
    )
    force_method: Optional[str] = Field(
        default=None,
        description="Force specific method: sequential, parallel_files, streaming_parallel, traditional_parallel",
    )
    force_disk_type: Optional[str] = Field(
        default=None, description="Force disk type: ssd, hdd, nvme"
    )
    manual_network_speed_mbps: Optional[float] = Field(
        default=None, description="Manually specify network speed in Mbps"
    )

    # NEW: Method-specific tuning
    parallel_files_min_files: int = Field(
        default=3, description="Minimum files to use parallel files mode"
    )
    streaming_min_file_size_mb: int = Field(
        default=100, description="Minimum file size in MB for streaming parallel"
    )
    streaming_min_files: int = Field(default=4, description="Minimum files for streaming parallel")
    traditional_min_files: int = Field(
        default=3, description="Minimum files for traditional parallel"
    )

    @field_validator("base_url", mode="before")
    @classmethod
    def trim_base_url(cls, v: Any) -> Any:
        if isinstance(v, str):
            return trim_url(v).rstrip("/")
        return v

    @model_validator(mode="after")
    def validate_download_modes(self) -> MirrorConfig:
        # Ensure only one download mode is active
        modes = [self.parallel_downloads, self.streaming_parallel, self.sequential_downloads]
        if sum(modes) > 1:
            raise ConfigError("Cannot enable multiple download modes simultaneously.")
        return self

    @model_validator(mode="after")
    def validate_and_normalize(self) -> MirrorConfig:
        # 1. Normalize base URL (strip whitespace & trailing slashes)
        url = str(self.base_url or "").strip().rstrip("/")
        if not url:
            raise ConfigError("base_url cannot be empty")
        if not url.startswith(("http://", "https://")):
            raise ConfigError(f"base_url must start with http:// or https://: {url}")

        parsed = urlparse(url)
        if not parsed.netloc:
            raise ConfigError(f"base_url missing hostname: {url}")

        # Use object.__setattr__ to bypass Pydantic's frozen model protection
        object.__setattr__(self, "base_url", url)

        # 2. Validate regex patterns in file_filters
        for pattern in self.file_filters:
            if not pattern.startswith("."):
                try:
                    re.compile(pattern)
                except re_error as e:
                    raise ConfigError(f"Invalid regex pattern '{pattern}': {e}")

        # Also validate exclude_dirs patterns if they contain regex
        for pattern in self.exclude_dirs:
            if "*" in pattern or "?" in pattern or "[" in pattern:
                try:
                    re.compile(pattern.replace("*", ".*").replace("?", "."))
                except re_error as e:
                    raise ConfigError(f"Invalid exclude_dir pattern '{pattern}': {e}")

        # 3. Validate parallel download settings
        if self.parallel_downloads or self.streaming_parallel:
            if self.min_chunk_size_mb < 1:
                raise ConfigError("min_chunk_size_mb must be >= 1")
            if self.min_chunk_size_mb > 100:
                raise ConfigError("min_chunk_size_mb must be <= 100")

            if self.max_chunks_per_file < 1:
                raise ConfigError("max_chunks_per_file must be >= 1")
            if self.max_chunks_per_file > 20:
                raise ConfigError("max_chunks_per_file must be <= 20")

            if self.max_parallel_chunks_total < 10:
                raise ConfigError("max_parallel_chunks_total must be >= 10")
            if self.max_parallel_chunks_total > 200:
                raise ConfigError("max_parallel_chunks_total must be <= 200")

            if self.min_chunk_size_mb < 5:
                logging.warning("Very small chunk size may increase overhead")

        # 4. Validate max_concurrent_downloads
        if self.max_concurrent_downloads < 1:
            raise ConfigError("max_concurrent_downloads must be >= 1")
        if self.max_concurrent_downloads > 50:
            raise ConfigError("max_concurrent_downloads must be <= 50")

        # 5. Check chunk assembly directory
        if self.chunk_assembly_dir:
            # Resolve to absolute path for consistent checking
            chunk_dir = self.chunk_assembly_dir.resolve()

            # Check if path is absolute (recommended)
            if not chunk_dir.is_absolute():
                logging.warning(
                    f"chunk_assembly_dir is relative: {chunk_dir} - using relative to CWD"
                )

            if chunk_dir.exists():
                # Directory exists - check writability
                if not os.access(str(chunk_dir), os.W_OK):
                    raise ConfigError(f"chunk_assembly_dir exists but is not writable: {chunk_dir}")
            else:
                # Directory doesn't exist - check parent is writable
                parent = chunk_dir.parent
                if not parent.exists():
                    raise ConfigError(f"Parent directory does not exist: {parent}")
                if not os.access(str(parent), os.W_OK):
                    raise ConfigError(f"Parent directory not writable: {parent}")

            # Check disk space on appropriate path
            check_path = chunk_dir if chunk_dir.exists() else chunk_dir.parent
            try:
                usage = shutil.disk_usage(str(check_path))
                min_free_mb = 100
                if usage.free < min_free_mb * 1024 * 1024:
                    logging.warning(
                        f"Low free space in {check_path}: {usage.free / (1024 * 1024):.1f}MB free "
                        f"(< {min_free_mb}MB recommended for chunk assembly)"
                    )
            except OSError as e:
                raise ConfigError(f"Cannot check disk space for {check_path}: {e}")

        return self

    @field_validator("cleanup_policy", mode="before")
    @classmethod
    def validate_cleanup_policy(cls, v: Any) -> CleanupPolicy:
        if isinstance(v, CleanupPolicy):
            return v
        if isinstance(v, str):
            try:
                return CleanupPolicy(v)
            except ValueError:
                return CleanupPolicy.SAFE_NO_DELETE
        return CleanupPolicy.SAFE_NO_DELETE

    @field_validator("scan_mode", mode="before")
    @classmethod
    def validate_scan_mode(cls, v: Any) -> ScanMode:
        if isinstance(v, ScanMode):
            return v
        if isinstance(v, str):
            try:
                return ScanMode(v)
            except ValueError:
                return ScanMode.ADAPTIVE
        return ScanMode.ADAPTIVE

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], silent: bool = False) -> MirrorConfig:
        if "dest_path" in config_dict and isinstance(config_dict["dest_path"], str):
            config_dict["dest_path"] = Path(config_dict["dest_path"])
        if "log_path" in config_dict and isinstance(config_dict["log_path"], str):
            config_dict["log_path"] = Path(config_dict["log_path"])
        if "disk_cache_dir" in config_dict and isinstance(config_dict["disk_cache_dir"], str):
            config_dict["disk_cache_dir"] = Path(config_dict["disk_cache_dir"])
        if "chunk_assembly_dir" in config_dict and isinstance(
            config_dict["chunk_assembly_dir"], str
        ):
            config_dict["chunk_assembly_dir"] = Path(config_dict["chunk_assembly_dir"])

        config = cls.model_validate(config_dict)
        config._silent = silent
        return config

    @classmethod
    def from_yaml(cls, yaml_path: Path, silent: bool = False) -> MirrorConfig:
        # 1️⃣ SYMLINK SECURITY CHECK (MUST run BEFORE .resolve())
        original_path = yaml_path
        if original_path.is_symlink():
            try:
                target = original_path.readlink()
                # Resolve target to catch relative symlinks pointing to sensitive dirs
                resolved_target = target.resolve()
                # Block symlinks to critical system directories
                sensitive_roots = ("/etc/", "/proc/", "/sys/", "/root/", "/var/lib/")
                if any(str(resolved_target).startswith(root) for root in sensitive_roots):
                    raise ConfigError(
                        f"Unsafe symlink in config path: {yaml_path} -> {resolved_target}"
                    )
            except Exception as e:
                raise ConfigError(f"Cannot resolve symlink {yaml_path}: {e}")

        # 2️⃣ NORMALIZE & VERIFY PATH (OS permissions handle read access)
        yaml_path = yaml_path.resolve()
        if not yaml_path.is_file():
            raise ConfigError(f"Config file not found or not a regular file: {yaml_path}")

        try:
            # ✅ FIX: Explicit UTF-8 encoding for cross-platform compatibility
            with open(yaml_path, encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)

            # ✅ FIX: Handle empty YAML files explicitly
            if config_dict is None or not isinstance(config_dict, dict):
                raise ConfigError("Config file must contain a valid YAML dictionary")

            config_dict = expand_env_vars(config_dict)

            # ⛔ REMOVED: Overzealous character blocking.
            # yaml.safe_load() is explicitly designed to be safe from code execution.
            # Blocking characters like '$', '(', ')' breaks valid URLs, paths, and
            # user-defined patterns. Security belongs at execution boundaries, not config load.

            # 3️⃣ REQUIRE CORE FIELDS
            required = ["base_url", "dest_path", "log_path"]
            missing = [f for f in required if f not in config_dict]
            if missing:
                raise ConfigError(f"Missing required fields: {missing}")

            return cls.from_dict(config_dict, silent=silent)

        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax in {yaml_path}: {e}")
        except ConfigError:
            raise  # ✅ Re-raise ConfigErrors as-is (prevents double-wrapping)
        except OSError as e:
            raise ConfigError(f"OS-level error reading {yaml_path}: {e}")
        except Exception as e:
            raise ConfigError(f"Failed to load config from {yaml_path}: {e}")

    @classmethod
    def validate(cls, config: MirrorConfig) -> List[str]:
        warnings = []

        if config.workers > 20:
            warnings.append("High worker count may cause server issues")
        if config.cache_max_age > 30:
            warnings.append("Long cache age may miss remote updates")
        if config.hash_algorithm == "md5":
            warnings.append("⚠️ MD5 hash algorithm is deprecated, consider using sha256")
        if config.content_hash_small_files:
            warnings.append(f"🔐 Content hash: files <{CONTENT_HASH_THRESHOLD / 1024:.0f}KB")

        if config.cleanup_policy == CleanupPolicy.DELETE:
            warnings.append("⚠️ DELETE MODE: Obsolete file deletion ENABLED")
        elif config.cleanup_policy == CleanupPolicy.MOVE:
            warnings.append("📦 MOVE MODE: Obsolete files moved to _obsolete folder")
        elif config.cleanup_policy == CleanupPolicy.PREVIEW:
            warnings.append("🔍 PREVIEW MODE: Showing what would be deleted/moved")
        else:
            warnings.append("✅ SAFE MODE: Obsolete file cleanup DISABLED")

        if config.safe_urls:
            warnings.append("🔒 URL sanitization enabled")
        if config.confirm_delete and config.cleanup_policy == CleanupPolicy.DELETE:
            warnings.append("🔐 Interactive confirmation required")
        if config.quiet:
            warnings.append("🔇 Quiet mode enabled")
        if config.verbose:
            warnings.append("🔊 Verbose mode enabled")
        if config.metrics_json:
            warnings.append(f"📊 Metrics export: {config.metrics_json}")
        if config.async_metadata:
            warnings.append(f"⚡ Async metadata {config.async_workers} workers")
        if config.trusted_server:
            warnings.append("⚡ Trusted server mode: Faster rate limiting (10ms delay)")
        if config.cache_html:
            warnings.append(f"📦 HTML caching enabled ({config.html_cache_max_age}h)")
        if config.adaptive_async:
            warnings.append(
                f"🔄 Adaptive async enabled (start={config.adaptive_start_concurrency})"
            )
        if config.bandwidth_limit:
            warnings.append(f"⏱️ Bandwidth limit: {config.bandwidth_limit} MB/s")
        if config.enable_resume:
            warnings.append("↩️ Resume capability enabled")
        if config.handle_symlinks:
            warnings.append(f"🔗 Symlink handling enabled (mode: {config.symlink_mode})")
        if config.adaptive_batch_processing:
            warnings.append(
                f"📈 Adaptive batch processing enabled (initial={config.initial_batch_size})"
            )
        if config.use_disk_backed_sets:
            warnings.append(f"💾 Disk-backed sets enabled (max memory: {config.memory_cache_size})")
        if config.fast_parsing_fallback:
            warnings.append("⚡ Fast parsing fallback enabled")
        if config.connection_pool_prewarm:
            warnings.append("🔥 Connection pool pre-warming enabled")

        # NEW v3.0.0 warnings
        if config.parallel_downloads:
            warnings.append(
                f"🚀 Parallel downloads enabled (max {config.max_chunks_per_file} chunks, {config.min_chunk_size_mb}MB min)"
            )
            if config.max_chunks_per_file > 8:
                warnings.append("⚠️ High chunk count may be excessive for most servers")

        # NEW v3.0.2: Warning for high concurrent downloads
        if config.max_concurrent_downloads > 20:
            warnings.append("⚠️ Very high concurrent downloads may overwhelm your network")

        return warnings


def load_config_from_args(args: argparse.Namespace, silent: bool = False) -> MirrorConfig:
    """Load configuration from command line arguments"""
    config_dict = {
        "base_url": args.url.rstrip("/"),
        "dest_path": args.dest_path,
        "log_path": args.log_path,
        "workers": args.workers,
        "timeout": args.timeout,
        "max_retries": args.max_retries,
        "retry_delay": args.retry_delay,
        "debug": args.debug,
        "print_logs": args.print_logs,
        # NOTE: 'quiet' and 'verbose' are set further down via getattr(...)
        # with safe defaults; duplicate keys here were dead (last one wins).
        "dry_run": args.dry_run,
        "file_filters": args.filter if args.filter else [],
        "exclude_dirs": args.exclude_dir or [],
        "cleanup_policy": args.cleanup,
        "quick": args.quick,
        "no_rget_list": args.no_rget_list,
        "rget_list_max_age": args.rget_list_max_age,
        "force_rget_list": args.force_rget_list,
        "no_cache": args.no_cache,
        "refresh_cache": args.refresh_cache,
        "cache_max_age": args.cache_max_age,
        "no_etag": getattr(args, "no_etag", False),
        "missing_files": getattr(args, "missing_files", False),
        "hash_algorithm": getattr(args, "hash_algorithm", "md5"),
        "use_shared_log": bool(args.log_file),
        "benchmark": args.benchmark,
        "http2": args.http2,
        "stats": args.stats,
        "max_depth": args.max_depth,
        "max_filename_len": args.max_filename_len,
        "safe_urls": getattr(args, "safe_urls", True),
        "confirm_delete": getattr(args, "confirm_delete", False),
        "quiet": getattr(args, "quiet", False),
        "verbose": getattr(args, "verbose", False),
        "metrics_json": getattr(args, "metrics_json", None),
        "progress_bar": getattr(args, "progress_bar", False),
        "async_metadata": getattr(args, "async_metadata", True),
        "async_workers": getattr(args, "async_workers", DEFAULT_ASYNC_WORKERS),
        "content_hash_small_files": getattr(args, "content_hash_small_files", True),
        "trusted_server": getattr(args, "trusted_server", False),
        "request_delay": getattr(args, "request_delay", REQUEST_DELAY),
        "cache_html": getattr(args, "cache_html", True),
        "html_cache_max_age": getattr(args, "html_cache_max_age", HTML_CACHE_MAX_AGE_HOURS),
        "adaptive_async": getattr(args, "adaptive_async", ADAPTIVE_ASYNC_ENABLED),
        "adaptive_error_threshold": getattr(
            args, "adaptive_error_threshold", ADAPTIVE_ERROR_THRESHOLD
        ),
        "adaptive_start_concurrency": getattr(
            args, "adaptive_start_concurrency", ADAPTIVE_START_CONCURRENCY
        ),
        "security_validation": getattr(args, "security_validation", True),
        "circuit_breaker_enabled": getattr(args, "circuit_breaker_enabled", True),
        "bandwidth_limit": getattr(args, "bandwidth_limit", None),
        "enable_resume": getattr(args, "enable_resume", True),
        "max_concurrent_downloads": getattr(
            args, "max_concurrent_downloads", 10
        ),  # Increased default
        "download_queue_size": getattr(args, "download_queue_size", 1000),
        "handle_symlinks": getattr(args, "handle_symlinks", False),
        "symlink_mode": getattr(args, "symlink_mode", "skip"),
        "circuit_breaker_downloads": getattr(args, "circuit_breaker_downloads", True),
        "max_symlink_depth": getattr(args, "max_symlink_depth", MAX_SYMLINK_DEPTH),
        "max_symlinks_per_dir": getattr(args, "max_symlinks_per_dir", MAX_SYMLINKS_PER_DIR),
        "symlink_bomb_threshold": getattr(args, "symlink_bomb_threshold", SYMLINK_BOMB_THRESHOLD),
        "adaptive_batch_processing": getattr(args, "adaptive_batch_processing", True),
        "initial_batch_size": getattr(args, "initial_batch_size", BATCH_SIZE),
        "max_batch_size": getattr(args, "max_batch_size", MAX_BATCH_SIZE),
        "target_batch_time": getattr(args, "target_batch_time", TARGET_BATCH_TIME_SECONDS),
        "memory_cache_size": getattr(args, "memory_cache_size", MEMORY_CACHE_MAX_SIZE),
        "use_disk_backed_sets": getattr(args, "use_disk_backed_sets", False),
        "disk_cache_dir": getattr(args, "disk_cache_dir", None),
        "fast_parsing_fallback": getattr(args, "fast_parsing_fallback", True),
        "http2_pipelining": getattr(args, "http2_pipelining", True),
        "connection_pool_prewarm": getattr(args, "connection_pool_prewarm", True),
        "fs_cache_ttl": getattr(args, "fs_cache_ttl", FS_CACHE_TTL_SECONDS),
        # NEW v3.0.0 arguments
        "max_chunks_per_file": getattr(args, "max_chunks", MAX_CHUNKS_PER_FILE),
        "min_chunk_size_mb": getattr(args, "min_chunk_size", 10),
        "max_parallel_chunks_total": getattr(
            args, "max_parallel_chunks", MAX_PARALLEL_CHUNKS_TOTAL
        ),
        "chunk_assembly_dir": getattr(args, "chunk_assembly_dir", None),
        "chunk_timeout_multiplier": getattr(
            args, "chunk_timeout_multiplier", CHUNK_TIMEOUT_MULTIPLIER
        ),
        "auto_concurrency": getattr(args, "auto_concurrency", AUTO_CONCURRENCY_ENABLED),
        "health_check_port": getattr(args, "health_check_port", 8080),
        "parallel_files_min_files": getattr(args, "parallel_files_min_files", 3),
        "streaming_min_file_size_mb": getattr(
            args, "streaming_min_size", STREAMING_MIN_FILE_SIZE_MB
        ),
        "streaming_min_files": getattr(args, "streaming_min_files", 4),
        "traditional_min_files": getattr(args, "traditional_min_files", 3),
        # NEW: Download mode flags
        "parallel_downloads": getattr(args, "parallel_downloads", False),
        "streaming_parallel": getattr(args, "streaming_parallel", False),
        "sequential_downloads": getattr(args, "sequential_downloads", False),
    }

    # At the end of config_dict creation in load_config_from_args():
    if not hasattr(args, "cleanup"):
        config_dict["cleanup_policy"] = CleanupPolicy.SAFE_NO_DELETE

    if hasattr(args, "scan_mode") and args.scan_mode:
        try:
            config_dict["scan_mode"] = ScanMode(args.scan_mode)
        except ValueError:
            config_dict["scan_mode"] = ScanMode.ADAPTIVE

    # Ensure only one mode is selected
    mode_count = sum(
        [
            config_dict["parallel_downloads"],
            config_dict["streaming_parallel"],
            config_dict["sequential_downloads"],
        ]
    )

    if mode_count > 1:
        raise ConfigError(
            "Cannot specify multiple download modes. Choose one: "
            "--parallel-downloads, --streaming-parallel, or --sequential-downloads"
        )
    config = MirrorConfig.from_dict(config_dict, silent=silent)

    return config


__all__ = [
    "ConfigSchema",
    "MirrorConfig",
    "validate_config_file",
    "expand_env_vars",
    "load_config_from_args",
]
