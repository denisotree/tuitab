# Building a .deb package

## Prerequisites

```sh
sudo apt-get install debhelper devscripts cargo rustc
```

## Build

From the repo root:

```sh
cp -r packaging/debian debian
dpkg-buildpackage -us -uc -b
```

The `.deb` file is created one directory above the repo root.

## Install

```sh
sudo dpkg -i ../tuitab_0.1.0-1_amd64.deb
```

## Clean up

```sh
rm -rf debian
dh_clean
```

## Notes

- The build requires a Rust toolchain. On Debian/Ubuntu you can install it via:
  `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`
  or via the `rustup` package from the official repository.
- For CI/CD, consider building with `cross` for cross-compilation to `aarch64`.
- The package installs three aliases: `/usr/bin/tuitab`, `/usr/bin/ttab`, and `/usr/bin/tt`.
