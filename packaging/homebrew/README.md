# Homebrew Tap Setup

## Creating the tap repository

1. Create a new GitHub repository named **`homebrew-tuitab`** under your account:
   `https://github.com/denisotree/homebrew-tuitab`

2. Inside that repo create `Formula/tuitab.rb` with the contents of `tuitab.rb` from this directory.

3. After creating a GitHub Release (tag `v0.1.0`):
   - Download the release tarballs
   - Run `shasum -a 256 <tarball>` for each one
   - Replace every `FILL_IN_AFTER_RELEASE` placeholder in the formula with the real sha256

## User installation

```sh
brew tap denisotree/tuitab
brew install tuitab
```

## Updating the formula for a new release

1. Update `url` + `sha256` in the `stable` block
2. Update each `on_macos`/`on_linux` block with the new release URLs and checksums
3. Commit and push to `homebrew-tuitab`

## Testing the formula locally

```sh
brew install --build-from-source Formula/tuitab.rb
brew test tuitab
brew audit --new tuitab
```
