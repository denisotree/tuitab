# Homebrew formula for tuitab.
#
# To publish this formula, create a tap repository:
#   https://github.com/denisotree/homebrew-tuitab
# and place this file at Formula/tuitab.rb inside that repo.
#
# Users install via:
#   brew tap denisotree/tuitab
#   brew install tuitab
#
# Before a release: replace sha256 values with the real checksums from
# `shasum -a 256 <tarball>` for each GitHub Release asset.

class Tuitab < Formula
  desc "Terminal tabular data explorer — CSV/JSON/YAML/TOML/Parquet/Excel/SQLite viewer"
  homepage "https://github.com/denisotree/tuitab"
  license "Apache-2.0"
  head "https://github.com/denisotree/tuitab.git", branch: "master"

  on_macos do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.1/tuitab-v0.11.1-aarch64-apple-darwin.tar.gz"
      sha256 "ab6123e921bd4080722e51ba717d044548ce413f4cb810cd670c27d98c9cfae1"
      version "0.11.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.1/tuitab-v0.11.1-x86_64-apple-darwin.tar.gz"
      sha256 "b828641ec42579854134a1aa491fdbcfddfc05ebc716effd55128833fdb5b57c"
      version "0.11.1"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.1/tuitab-v0.11.1-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "a8f5ad6df8ed41b8d0b423a90224a48cc54a6d5998ba3f0956d74644b0fe3ea5"
      version "0.11.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.1/tuitab-v0.11.1-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "3b36f2871dbc60150af10d9e66f917d9ec56cbc2e965715f1d01759555f502d9"
      version "0.11.1"
    end
  end

  def install
    bin.install "tuitab"
    bin.install_symlink bin/"tuitab" => "ttab"
    bin.install_symlink bin/"tuitab" => "ttb"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/tuitab --version")
    assert_match version.to_s, shell_output("#{bin}/ttab --version")
    assert_match version.to_s, shell_output("#{bin}/ttb --version")
  end
end
