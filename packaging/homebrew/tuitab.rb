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
  desc "Terminal tabular data explorer — CSV/JSON/Parquet/Excel/SQLite viewer"
  homepage "https://github.com/denisotree/tuitab"
  license "Apache-2.0"
  head "https://github.com/denisotree/tuitab.git", branch: "master"

  on_macos do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.0/tuitab-v0.3.0-aarch64-apple-darwin.tar.gz"
      sha256 "c92e050744f041351dddf4bdbf28caa821114cf301eac6baa859143be223337a"
      version "0.3.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.0/tuitab-v0.3.0-x86_64-apple-darwin.tar.gz"
      sha256 "9a96791617de5935a4cd9da8803e1f5c3a8f80f092e5b7d4c04cd8cab3e2738d"
      version "0.3.0"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.0/tuitab-v0.3.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "3412236042e92b5e4d4bde218936d4b16fcac5a9ad357890a52fa3764bbdd007"
      version "0.3.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.0/tuitab-v0.3.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "8a3a61d97767d709e43a173cf688bed0c6af298a3ca953f879ed9eaacc7b4ffe"
      version "0.3.0"
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
