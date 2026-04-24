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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.6/tuitab-v0.3.6-aarch64-apple-darwin.tar.gz"
      sha256 "5c854b42dd6fa323bd3701d587c12a3b308693b1e84bd277b6e8484309caafa0"
      version "0.3.6"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.6/tuitab-v0.3.6-x86_64-apple-darwin.tar.gz"
      sha256 "32875b5d2ade18cf39be75f89d404d0e2d55f1571c4aff6fdb31e44003752e11"
      version "0.3.6"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.6/tuitab-v0.3.6-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "43fc42e6517427eccef4ebb33dc3d93343d3115aa28b7b44bc3a2db33141e14f"
      version "0.3.6"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.6/tuitab-v0.3.6-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "e7c6493428de9942655153837e17cb35422540f6368763f62772d8a8fd46d661"
      version "0.3.6"
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
