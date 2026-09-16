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
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.6/tuitab-v0.9.6-aarch64-apple-darwin.tar.gz"
      sha256 "39174cfe300228e5b2e7e30b49740c62c070fc96b6362efae1d32cdd112378d4"
      version "0.9.6"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.6/tuitab-v0.9.6-x86_64-apple-darwin.tar.gz"
      sha256 "1815d4190757d9e23e48e68339832233c3b93405fb713313c7d92fb4abdfeed7"
      version "0.9.6"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.6/tuitab-v0.9.6-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "015c8c02d7621b780f875d5c0f4938178d09d1fc96f55e4306b86fb37f05e33f"
      version "0.9.6"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.6/tuitab-v0.9.6-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "47c8e1655f4d2da0ede2ed1cbefe650cd45f02697b6ac91db2e5a6c476aca996"
      version "0.9.6"
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
