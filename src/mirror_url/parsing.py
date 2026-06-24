"""HTML link extraction and adaptive batching.

Migrated verbatim from ``mirror_url.py`` (orig. lines 2951-3093):
``AdaptiveBatchProcessor``, ``extract_links_fast``, ``should_use_fast_parser``.
"""

from __future__ import annotations

from collections import deque
from threading import RLock
from typing import List, Optional, Union

from .compat import LXML_AVAILABLE
from .constants import (
    BATCH_ADJUSTMENT_FACTOR,
    BATCH_SAMPLE_SIZE,
    BATCH_SIZE,
    FAST_PARSE_MIN_CONTENT_LENGTH,
    MAX_BATCH_SIZE,
    MIN_BATCH_SIZE,
    TARGET_BATCH_TIME_SECONDS,
)


class AdaptiveBatchProcessor:
    """Dynamically adjust batch sizes based on performance"""

    def __init__(
        self,
        initial_batch: int = BATCH_SIZE,
        min_batch: int = MIN_BATCH_SIZE,
        max_batch: int = MAX_BATCH_SIZE,
        target_time: float = TARGET_BATCH_TIME_SECONDS,
        adjustment_factor: float = BATCH_ADJUSTMENT_FACTOR,
    ):
        """
        Initialize adaptive batch processor.

        Args:
            initial_batch: Initial batch size
            min_batch: Minimum batch size
            max_batch: Maximum batch size
            target_time: Target processing time per batch
            adjustment_factor: Factor for adjusting batch size
        """
        self.batch_size = initial_batch
        self.min_batch = min_batch
        self.max_batch = max_batch
        self.target_time = target_time
        self.adjustment_factor = adjustment_factor
        self.processing_times = deque(maxlen=BATCH_SAMPLE_SIZE)
        self.items_processed = deque(maxlen=BATCH_SAMPLE_SIZE)
        self.lock = RLock()

    def record_batch(self, processing_time: float, items: int) -> None:
        """
        Record batch processing metrics.

        Args:
            processing_time: Time taken to process batch
            items: Number of items in batch
        """
        with self.lock:
            self.processing_times.append(processing_time)
            self.items_processed.append(items)
            self._adjust_batch_size()

    def _adjust_batch_size(self) -> None:
        """Adjust batch size based on recent performance"""
        if len(self.processing_times) < 2:
            return
        total_time = sum(self.processing_times)
        total_items = sum(self.items_processed)
        if total_items == 0:
            return
        avg_time_per_item = total_time / total_items
        if avg_time_per_item > 0:
            optimal_batch = int(self.target_time / avg_time_per_item)
            optimal_batch = max(self.min_batch, min(self.max_batch, optimal_batch))
            new_size = int(
                self.batch_size * (1 - self.adjustment_factor)
                + optimal_batch * self.adjustment_factor
            )
            self.batch_size = max(self.min_batch, min(self.max_batch, new_size))

    def get_batch_size(self) -> int:
        """Get current recommended batch size"""
        with self.lock:
            return self.batch_size

    def reset(self) -> None:
        """Reset to initial settings"""
        with self.lock:
            self.batch_size = BATCH_SIZE
            self.processing_times.clear()
            self.items_processed.clear()


# ============================================================================
# FAST HTML PARSING UTILITIES
# ============================================================================
def extract_links_fast(html_content: Union[bytes, str]) -> List[str]:
    """
    Extract links from HTML without full DOM parsing.

    Args:
        html_content: HTML content as bytes or string

    Returns:
        List of extracted links
    """
    if isinstance(html_content, str):
        html_content = html_content.encode("utf-8", errors="ignore")
    links = []
    start = 0
    href_pattern = b'href="'
    href_pattern2 = b"href='"

    # Extract double-quoted hrefs
    while True:
        pos = html_content.find(href_pattern, start)
        if pos == -1:
            break
        pos += len(href_pattern)
        end_pos = html_content.find(b'"', pos)
        if end_pos == -1:
            break
        href = html_content[pos:end_pos].decode("utf-8", errors="ignore")
        if href and not href.startswith(("#", "javascript:", "mailto:")):
            links.append(href)
        start = end_pos + 1

    # Extract single-quoted hrefs
    start = 0
    while True:
        pos = html_content.find(href_pattern2, start)
        if pos == -1:
            break
        pos += len(href_pattern2)
        end_pos = html_content.find(b"'", pos)
        if end_pos == -1:
            break
        href = html_content[pos:end_pos].decode("utf-8", errors="ignore")
        if href and not href.startswith(("#", "javascript:", "mailto:")):
            links.append(href)
        start = end_pos + 1

    return links


def should_use_fast_parser(content_length: Optional[int], config) -> bool:
    """
    Determine whether to use the fast parser (StringZilla-based) or lxml.

    Policy:
    - If lxml isn't available, the fast parser is the only option.
    - If the document is large, prefer the fast parser for speed.
    - Otherwise, prefer lxml for correctness. ``config.fast_parsing_fallback``
      is used as a fallback when lxml fails at runtime, NOT as a primary
      preference, so it is intentionally NOT consulted here.

    Args:
        content_length: Length of content in bytes
        config: MirrorConfig instance (kept for API stability)

    Returns:
        True if fast parser should be used
    """
    if not LXML_AVAILABLE:
        return True
    if content_length and content_length > FAST_PARSE_MIN_CONTENT_LENGTH:
        return True
    return False


__all__ = ["AdaptiveBatchProcessor", "extract_links_fast", "should_use_fast_parser"]
