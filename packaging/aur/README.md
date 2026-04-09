# Arch Linux AUR / pacman packaging

## AUR submission

1. Create an AUR account at https://aur.archlinux.org
2. Add your SSH key to your AUR account
3. Clone the AUR package skeleton:

   ```sh
   git clone ssh://aur@aur.archlinux.org/tuitab.git aur-tuitab
   ```

4. Copy `PKGBUILD` (source build) into `aur-tuitab/`
5. Generate `.SRCINFO`:

   ```sh
   cd aur-tuitab
   makepkg --printsrcinfo > .SRCINFO
   ```

6. Update the `sha256sums` entry with the real checksum:

   ```sh
   curl -sL https://github.com/denisotree/tuitab/archive/refs/tags/v0.1.0.tar.gz | sha256sum
   ```

7. Commit and push:

   ```sh
   git add PKGBUILD .SRCINFO
   git commit -m "Initial release v0.1.0"
   git push
   ```

## Binary AUR package (tuitab-bin)

Repeat the same steps for `PKGBUILD-bin` using the `tuitab-bin` AUR package name.
This package downloads pre-built binaries, so users don't need a Rust toolchain.

## Installing locally (for testing)

```sh
cd packaging/aur
makepkg -si
```

## Updating for a new release

1. Bump `pkgver` and `pkgrel=1` in PKGBUILD
2. Update `sha256sums` with the new release tarball checksum
3. Regenerate `.SRCINFO` and push

## pacman (unofficial user repo)

If you want to host your own pacman-compatible repo (e.g. for Manjaro or EndeavourOS), you can use `repo-add` after building:

```sh
makepkg -s
repo-add tuitab.db.tar.gz tuitab-0.1.0-1-x86_64.pkg.tar.zst
```

Then host the `*.db`, `*.files`, and `*.pkg.tar.zst` files on a web server and share the repo URL.
