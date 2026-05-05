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
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.2/tuitab-v0.4.2-aarch64-apple-darwin.tar.gz"
      sha256 "7930613c5b2489d12fac1d5b793543cea3057a6eabf22579aef03674414c3c9d"
      version "0.4.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.2/tuitab-v0.4.2-x86_64-apple-darwin.tar.gz"
      sha256 "e999e3c8d47bb2a2ee5d6747aa4382f0fda754e2cba6aa859fc6439f04a71cb1"
      version "0.4.2"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.2/tuitab-v0.4.2-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "3ee41e29639cfae3d8c78d17687775caff72e226b57dd227a7d79c5742c7a29c"
      version "0.4.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.2/tuitab-v0.4.2-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "2889311efd8e107d6e83786e17f88b74671eec5db41173edce43be8d8c45f158"
      version "0.4.2"
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
