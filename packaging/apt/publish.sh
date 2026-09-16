#!/usr/bin/env bash
# Add .deb files to a signed APT repository and regenerate its indexes.
#
#   publish.sh <dir with new .deb files> <repository root>
#
# The repository root is what GitHub Pages serves under /apt.  The signing key
# must already be in the gpg keyring; APT_GPG_KEY_ID names it.  Only the newest
# KEEP_VERSIONS versions stay in the pool — Pages caps a site at 1 GB and every
# release adds two ~20 MB packages.
set -euo pipefail

debs=$1
repo=$2
key=${APT_GPG_KEY_ID:?APT_GPG_KEY_ID must name the signing key}
keep=${KEEP_VERSIONS:-3}
suite=stable
archs="amd64 arm64"

pool="$repo/pool/main/t/tuitab"
mkdir -p "$pool"
shopt -s nullglob
new=("$debs"/*.deb)
[ ${#new[@]} -gt 0 ] || { echo "no .deb files in $debs" >&2; exit 1; }
cp "${new[@]}" "$pool/"

# tuitab_<version>-<rev>_<arch>.deb → drop every version older than the newest $keep
for old in $(ls "$pool" | sed -E 's/^tuitab_([^_]+)_.*/\1/' | sort -uV | head -n -"$keep"); do
    rm -f "$pool"/tuitab_"$old"_*.deb
done

cd "$repo"
for arch in $archs; do
    dir="dists/$suite/main/binary-$arch"
    mkdir -p "$dir"
    apt-ftparchive --arch "$arch" packages pool > "$dir/Packages"
    gzip -9kf "$dir/Packages"
done

# Old signatures must not be hashed into the new Release.
rm -f "dists/$suite/Release" "dists/$suite/InRelease" "dists/$suite/Release.gpg"
apt-ftparchive \
    -o APT::FTPArchive::Release::Origin=tuitab \
    -o APT::FTPArchive::Release::Label=tuitab \
    -o APT::FTPArchive::Release::Suite="$suite" \
    -o APT::FTPArchive::Release::Codename="$suite" \
    -o APT::FTPArchive::Release::Architectures="$archs" \
    -o APT::FTPArchive::Release::Components=main \
    release "dists/$suite" > Release.tmp
mv Release.tmp "dists/$suite/Release"

sign() { gpg --batch --yes --pinentry-mode loopback --local-user "$key" "$@"; }
sign --clearsign -o "dists/$suite/InRelease" "dists/$suite/Release"
sign --detach-sign --armor -o "dists/$suite/Release.gpg" "dists/$suite/Release"
gpg --batch --yes --export "$key" > tuitab.gpg

echo "published: $(ls pool/main/t/tuitab | tr '\n' ' ')"
