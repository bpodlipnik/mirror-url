# Compare mirror and download modes

`benchmark_modes.py` runs the installed `mirror-url` CLI. Python 3.10 or newer
and MirrorURL 3.9.9 or newer are required; the script itself uses only the
Python standard library. Activate the environment containing MirrorURL first.

Preview the experiment without running MirrorURL or creating files:

```sh
python scripts/benchmark_modes.py --dry-run
```

Run the comparison against the three previously tested videos:

```sh
python scripts/benchmark_modes.py --pairs 3
```

Defaults are the directory
`https://star.mps.mpg.de/data/sami-2021-10-31/`, the three literal video filenames,
both backends and streaming parallel downloads. SHA-256 is disabled; add
`--verify-content` to enable it equally in both modes. No progress bars are
rendered during measurement. Each CLI's complete output goes to `console.log`.

The default experiment makes **24 measured downloads and two unmeasured cache
warmups**. With the current dataset, that transfers and retains approximately
**6.50 GiB** of payloads. Use `--pairs 1` for a shorter eight-run check, or
`--cache fresh` to omit warmups and reused-cache runs. Repeating only one backend
is supported with `--backends httpx` or `--backends aiohttp`. Change strategy
with `--strategy sequential-downloads` or `--strategy parallel-downloads`.

Each invocation gets a fresh, empty destination with the same relative layout.
Mirror uses an anchored filename filter and a depth limit of one. For another
dataset, supply `--url ROOT --url-list FILE`: the list must contain **literal
filenames directly in that root**, one per line, including spaces. Nested paths
and full URLs are rejected. The script checks the exact downloaded file set and
sizes before accepting a result; a missing, extra or changed-size file stops
the experiment. It does not fetch expected hashes independently.

Modes alternate first position between pairs; backend and cache-state order
also reverse. All other CLI defaults are left in place. The `fresh` condition
starts without a persistent cache. The `reused` mirror condition copies the
same cache from an unmeasured mirror warmup into each new log directory and
requires that directory signatures were loaded. Download mode has no listing
cache; its repeated measurements provide the paired baseline for each mirror
condition. **HTML memory caches start fresh in every CLI process.** This does
not clear OS caches or remote server/CDN caches.

Results go into a new `mirror-url-benchmark-TIMESTAMP` directory. An existing
output directory is refused; `--output NEW_DIRECTORY` selects its location.
Nothing is deleted automatically. Each run retains its payloads, command,
console, cache manifests and parsed summary. Failures and Ctrl-C stop further
runs and retain partial evidence. A timeout terminates and reaps the script's
own CLI child. `--run-timeout` defaults to 900 seconds per invocation.

Read `status.json` first: only `completed` means the whole experiment passed.
Then compare:

- `summary.md`: medians by backend, mode and cache condition.
- `summary.json`: median, minimum and maximum for job time, download time,
  process wall time and throughput.
- `comparisons.json`: complete matched pairs; a download/mirror total-time
  ratio below one means download mode was faster in that comparison.
- `results.csv` and `results.json`: individual successful measurements and
  phase/stage timings; JSON also retains warnings and raw summary fields.
- `run-NNN/result.json` and `console.log`: each command, result and diagnostics.

Each command enables `--metrics`. Job and phase timings come from the same
shared CLI summary in both modes,
rounded to milliseconds; displayed throughput has two decimal places. The
separate process wall clock includes interpreter startup and CLI exit.
`--metrics-json` is deliberately omitted because it starts a health server.
Transfer-stage worker sums overlap and must not be added together
as elapsed time. Warnings remain in each result for review; failed runs never
enter the medians. Interpret incomplete experiments as partial evidence.

Errors and warnings are identified by their log-record level. Filenames containing
`[ERROR]`, `[CRITICAL]`, `[WARNING]` or `Traceback` remain ordinary data; actual
error records and unprefixed traceback headers still invalidate a run.
