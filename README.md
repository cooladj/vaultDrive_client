# VaultDrive Client

A cross-platform client for mounting remote drives QUIC.

> ⚠️ **This project is under active development**

## Overview

VaultDrive provides a custom, cross-platform implementation of virutal drive over QUIC for Windows and Unix systems. The client uses platform-native filesystem APIs to create virtual drive mounts that integrate seamlessly with your operating system.

| Platform | Technology |
|----------|------------|
| Windows  | WinFSP     |
| Unix     | FUSE       |

## Requirements

- A running VaultDrive Server instance (not yet open source)
- Windows: WinFSP installed
- Unix: FUSE installed

## Getting Started

1. Ensure you have access to a VaultDrive Server running on the target device
2. Install the appropriate filesystem driver for your platform
3. Mount your remote drive

## TODO

- [ ] Fix Windows bug causing JetBrains safe-write operations to report failure despite successful writes
- [ ] Complete FUSE implementation testing
- [ ] UI improvements
- [ ] Code cleanup and refactoring

## License

This project is licensed under the GNU Affero General Public License v3.0 - see the [LICENSE](LICENSE) file for details.
