"""Enable ``python -m mirror_url``.

Delegates to the CLI entry point once ``cli`` is populated.
"""

from __future__ import annotations


def _run() -> None:
    try:
        from .cli import main
    except ImportError as exc:  # pragma: no cover - pre-migration guard
        raise SystemExit(
            "mirror_url.cli is not populated yet. During the refactor, run the "
            "original script directly: `python mirror_url.py ...`"
        ) from exc
    main()


if __name__ == "__main__":
    _run()
