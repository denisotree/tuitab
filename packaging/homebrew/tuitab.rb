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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.5/tuitab-v0.3.5-aarch64-apple-darwin.tar.gz"
      sha256 "5d604256368cb94e23a2f5846f078296744c21073c2489f384cd8a18bf3b2fe9"
      version "0.3.5"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.5/tuitab-v0.3.5-x86_64-apple-darwin.tar.gz"
      sha256 "0d524d03b4e0f30afb04cb7cf2dfcc75db659b31ad6ee4450871811724144ece"
      version "0.3.5"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.5/tuitab-v0.3.5-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "d87fe7cdfe208f1b1318c62f511fa1e58a943444e274cf6318391d95335760b9"
      version "0.3.5"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.5/tuitab-v0.3.5-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "3dcbe6125744134857b49a039a2c82d276c7a2b05f8287f29beb5811a2a32247"
      version "0.3.5"
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
