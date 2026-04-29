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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.8/tuitab-v0.3.8-aarch64-apple-darwin.tar.gz"
      sha256 "9062b72730b8e4bdfa1b4404521d46b0c22620e524ec5d1a628aaa64052beb53"
      version "0.3.8"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.8/tuitab-v0.3.8-x86_64-apple-darwin.tar.gz"
      sha256 "41963a0fd06400e41eff42f4a7c21766865393295534195861bde9385ea28916"
      version "0.3.8"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.8/tuitab-v0.3.8-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "91a596e4c50f7f2a84e070c14c1e4de2af0786f831645378a2e3a388b6c5bb2e"
      version "0.3.8"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.8/tuitab-v0.3.8-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "9c268d0ff9759c17cc467f09f5a53b0e2ed5a6471cfe202bc3536d83ff5cf775"
      version "0.3.8"
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
