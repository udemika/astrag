# Verification ledger

## Before refactor

- Three principal files matched the last archive byte-for-byte.
- Python compilation passed but did not establish runtime correctness.
- Node syntax checking failed for extracted `analyze-ui.js` on HTML closing tags; 22910 NUL bytes and 2818 replacement characters were observed.
- Complete original HTML inline script passed Node syntax checking.

## Environment constraints

Windows, Python 3.11. REA Windows Ghidra refused ELF with `target_format_unsupported`. No Linux distribution is installed under WSL; Docker, native Bash, shellcheck and ffprobe were not found. No original Astra process was run. Ubuntu deployment, actual decoder playback, hardware features and comparative performance remain unverified.

## Revision checks

- `ASTRA_TEST_URL=http://127.0.0.1:18080 python -m unittest discover -v`: **35 tests passed**, no skips (10 backend, 18 engine, 6 extractor, 1 real backend loopback).
- Loopback integration created a real UDP input and two simultaneous HTTP viewers, verified identical first TS batches and packet framing, then removed the stream.
- Engine tests additionally cover local HTTP error/retry, actual UDP/RTP receive, actual UDP output, cancellation, queue overflow, CC/PCR, PAT/PMT and synthetic HLS. Multicast membership is mock-verified, not tested on a routed multicast network.
- The atomic-save failure test deliberately logs an injected `OSError: disk failure`; this is expected and rollback assertions pass.
- Restored `www/index.html` equals the original 431910 bytes. SHA256: `67640126902fbfcbda0a093fb4d53dae8428b9422fff668075d6fe24f1917aa6`. Original inline script passed Node syntax checking.
- Browser opened the actual restored panel against the new backend, registered 20 modules and authenticated its control WebSocket with auth disabled. Dashboard and General Settings rendered. Initial dashboard telemetry error was reproduced, corrected, regression-tested, and did not recur in the repeated two-viewer run.
- Browser still reports missing `/mod.js`; `/favicon.png` also returns 404. No original resource bytes were recovered for these dependencies. This is an explicit incomplete dependency, not silently replaced content.
- Old split JS/CSS are preserved locally as legacy user artifacts but excluded from the clean release, whose UI is the validated original inline HTML.
- Follow-up completed-process log exposed an additional telemetry redaction failure on integer PID dictionary keys. Both key checks now use `str(k).lower()`; a nested integer-key/secret regression test was added. Rerun: 36 tests discovered, 35 passed, 1 live-backend test skipped because the temporary server was stopped. Earlier live transport pass remains recorded above; it did not cover this telemetry failure.
- No native source decompilation, decoded HLS video test, PCR-paced output validation, original-server comparison, Linux install, Bash syntax validation, or throughput benchmark was performed. Unit tests do not establish these properties.
