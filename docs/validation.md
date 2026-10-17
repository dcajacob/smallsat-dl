# Download validation

These observations were recorded on October 17, 2026, using Python 3.13.7
and curl_cffi 0.16.3 on Linux. They establish the behavior of those runs,
not a permanent server quota or a guarantee of future availability.

## Transport and rate checks

- The initial transport comparison stopped on its first request when HTTPX
  received HTTP 403. It did not reach a curl_cffi request.
- After a 20-minute cooldown, curl_cffi downloaded the same 144,514-byte PDF
  successfully. A single later HTTPX control received HTTP 403 again. This
  supports the transport choice for that environment; it is not a throughput
  comparison between two successful clients.
- A short curl_cffi probe accepted all 18 PDF requests across 15, 20, and
  30 downloads per minute. Repeated URLs and server caching limit that sample.
- The sustained run accepted 150 distinct PDFs across 24 sessions, totaling
  142,144,865 bytes. Every response matched its expected size and SHA-256.
  The run took about five minutes, with a measured cadence of 29.94 downloads
  per minute and a minimum start gap of 2.000 seconds. No blocks occurred.
- Each sustained-run download followed one explicitly validated archive
  redirect: 150 downloads required 300 HTTP GETs. The default limit meters
  logical file downloads, so redirects can increase the physical request rate.

The controlled scripts require explicit execution, cap their request counts,
and stop at the first block or invalid response. Known acquisition scripts
were inactive during the sustained test; other shared-IP traffic cannot be
ruled out. The default was raised from 15 to 30 downloads per minute after
the sustained run passed. Lower rates remain selectable.

## Archive acquisitions

- **2026:** 451 source files were present after the run: 450 PDFs and one
  keynote video, totaling about 4.03 GB. The downloader fetched 177 missing
  files and skipped 274 existing files. No download failures or blocks occurred.
- **1987–2025:** 6,764 source files were present, totaling about 50.05 GB.
  The downloader fetched 75 missing files and skipped 6,689 existing targets.
  No download failures or blocks occurred. Existing files saved under old
  extensions were reused when their formats could be verified.
- Offline metadata replays found every source target present without making
  network requests. All 15 videos from 1987–2025 were readable by ffprobe.
- Presentation conversion produced 206 PDF copies. Three presentations could
  not be converted: one triggered a LibreOffice error and two had damaged ZIP
  structures. One integrity-check refetch confirmed the same damaged source
  bytes. All original presentation files were preserved.

Existing files are normally skipped by path; a normal resume is not a full
checksum audit. Atomic temporary writes protect downloads produced by this
version from becoming final files before a transfer completes. Conversion
failures produce a nonzero command exit status even if downloads succeeded.

Downloaded conference material, raw diagnostics, inventories, and local
analysis are excluded from the repository. The source distribution includes
the controlled probe scripts so its regression suite remains runnable.
