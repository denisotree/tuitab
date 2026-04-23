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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.3/tuitab-v0.3.3-aarch64-apple-darwin.tar.gz"
      sha256 "5539b10fdf75f057531f1afdd3017b0d2aa72ddc5ae077e176ecd0c851a3f66b"
      version "0.3.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.3/tuitab-v0.3.3-x86_64-apple-darwin.tar.gz"
      sha256 "d8c6e07c52463b7160d1f19044db98dceb12a531c36699a79ad614bb6be80199"
      version "0.3.3"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.3/tuitab-v0.3.3-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "10f1b6c6a4dc121d3612f2fd7853958056c6bc4da5952957f0936ece15765658"
      version "0.3.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.3/tuitab-v0.3.3-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "ba52fb2acdfc6d87c488ca18339dc0d301a7c8e20bb15d8a36d26c9ca342ba63"
      version "0.3.3"
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
