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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.1/tuitab-v0.3.1-aarch64-apple-darwin.tar.gz"
      sha256 "e717e4e3d041cf23f8546d1eae367f48ba520a35204de0b33eea217c296d0535"
      version "0.3.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.1/tuitab-v0.3.1-x86_64-apple-darwin.tar.gz"
      sha256 "b7126bd3f9542ce1a21e5557f506a3f036ae85bbede52c50f532ef080d4bf183"
      version "0.3.1"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.1/tuitab-v0.3.1-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "4f1d5020da994a0f4b93ed02ad8427f8623d7042ae47de8c6ec0cf360b0036ec"
      version "0.3.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.1/tuitab-v0.3.1-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "7f8e9cc4a114e0babe4b7059474d9adac459d5bfc15049030404db6fe3e1cbd7"
      version "0.3.1"
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
