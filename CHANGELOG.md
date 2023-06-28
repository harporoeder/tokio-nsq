# Change log

## [0.14.0] - 2023-06-227
### Changed:
- Removed 'mut' requirement for NSQ message 'touch' command

Thanks taufik-rama!

## [0.13.2] - 2023-06-21
### Fixed:
- Fixed all modern clippy warnings and run cargo fmt

## [0.13.1] - 2023-06-12
### Fixed:
- Updated 'built' dependency to '0.6'. Thanks taufik-rama!
- Added 'Accept' header for NSQ Lookup requests

## [0.13.0] - 2022-02-02
### Changed:
- Switched to bounded instead of unbounded queues (limit of 10 thousand items)

Thanks polachok!

## [0.12.2] - 2022-01-23
### Changed:
- Replace the deprecated library failure with anyhow / thiserror.

Thanks polachok!

## [0.12.1] - 2022-01-21
### Changed:
- Switch to buffered IO to improve performance
- Switch Mutex to RWLock where optimal to improve performance
- Miscellaneous refactoring

Thanks polachok!

## [0.12.0] - 2021-03-31
### Fixed:
- Incorrect wire format for the REQ command
### Changed:
- Refactored and updated to tokio 1.4.

Thanks paulfariello!
