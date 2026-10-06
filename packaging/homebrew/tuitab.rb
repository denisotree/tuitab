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
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.1/tuitab-v0.10.1-aarch64-apple-darwin.tar.gz"
      sha256 "69308cefacb9851e431ae1e1d57345d8f3eff9d8d1430ecc93e173144ebe2286"
      version "0.10.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.1/tuitab-v0.10.1-x86_64-apple-darwin.tar.gz"
      sha256 "e34479c3948924dc209d423afbe404d994f67e976916943266697bdce7a91db7"
      version "0.10.1"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.1/tuitab-v0.10.1-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "96f701d33da17ef4e841b213a4a42071cecdd9c679fd90a7a38c1c5c83789ccd"
      version "0.10.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.1/tuitab-v0.10.1-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "17158b18b9d70ef1da1e2b4fc7fd4def591c82dabc09c8fd12d11a3b6e4a26b7"
      version "0.10.1"
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
