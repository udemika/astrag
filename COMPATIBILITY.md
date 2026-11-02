# Compatibility ledger

This is an independent implementation, not recovered Astra source or a certified full clone.

## Evidence and baseline

- Original: `C:\Users\akzam\Music\PLAYLIST\Rea1\astra`.
- Baseline installation archive retained in `backups/baseline/astra_clone_project.tar.gz`; original file hashes in `backups/baseline/inventory.json`.
- Embedded HTML observed at file offset `0x723740` through `0x78ce66` (exclusive), 431910 bytes. Strict UTF-8, complete HTML. Inline script passed Node syntax checking before changes.
- Frontend contracts observed directly in extracted JavaScript: `control/event/`, auth `login:password`, top-level scoped events, named command payloads and nested removal flags.
- REA Ghidra open attempted again during this revision: rejected with `target_format_unsupported`, `Windows Ghidra P0 accepts native PE and explicitly selected raw 6502 targets only.` Hopper unsupported on this host; IDA not configured. No native pseudocode was recovered. This is a REA Windows provider limitation, not a general Ghidra ELF limitation.

## Matrix

| Feature | Original evidence | Independent implementation / verification |
|---|---|---|
| Embedded UI | Complete HTML bytes in ELF `.data` | Extraction manifest and script syntax tests; browser test recorded separately |
| Control commands | Requests and handlers in original UI | Contract tests against named payloads and top-level events |
| TS PID/AFC/CC/PCR | MPEG-TS format; native original algorithm unknown | Independent parser; synthetic packet tests, not native architecture parity |
| HTTP/UDP/RTP input | URL handling visible in UI; original native internals unknown | asyncio/httpx implementation; local integration tests required |
| HTTP fanout | Original UI playback URLs | Per-subscriber bounded queues; multi-client test required |
| HLS | UI settings and playback references | Independent RAM TS segmentation; decoder/keyframe compatibility needs media tests |
| Native pacing / libuv | Prior reports only; no recovered implementation | Python asyncio is not claimed to reproduce native timing or throughput |
| Persistence and reload | UI load/upload/config requests | Atomic application state; lifecycle tests |
| Auth | UI Basic header and WebSocket auth message | Independent enforcement and tests; not original password storage recovery |
| DVB scan/tuning | UI and binary module-name strings | Not implemented; must return unsupported, not fake success |
| Conditional access / softcam | UI configuration forms | Configuration compatibility does not implement descrambling |
| RTSP/SRT | UI URL options | Not implemented by basic HTTP/UDP engine |
| Ubuntu service | Original deployment request | Installer review is not an Ubuntu installation test |
| Full Astra parity | No complete feature inventory or comparative captures | Not established |

## Acceptance still required

Run on Ubuntu 22.04 and 24.04 with actual MPEG-TS sources and the original server under appropriate authorization. Compare output decoding, PAT/PMT, PCR timing, packet loss, continuity, multiple viewers, reconnect/failover, reload, memory growth and throughput. Hardware DVB and conditional-access functionality require separate implementations and real devices/test environments. No benchmark or full-clone claim is justified by Python compilation or unit tests alone.
