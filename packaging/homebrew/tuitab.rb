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
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.3/tuitab-v0.1.3-aarch64-apple-darwin.tar.gz"
      sha256 "3288b36cc8e4c8ae105f2ed1d285779d20c64352087078c7d62460502e83c8ea"
      version "0.1.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.3/tuitab-v0.1.3-x86_64-apple-darwin.tar.gz"
      sha256 "a7d179648026dfeb7e1cb9b4c6f5c1cd38c0cb816d1d7fb0706ae23f86e14f08"
      version "0.1.3"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.3/tuitab-v0.1.3-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "b700bcedd1315d7cf330b6755b9696669e5923f8454974503ff7855e2dd1d19a"
      version "0.1.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.3/tuitab-v0.1.3-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "7db3ce736324f3ec9a2c33e8eac55870ef174ac84c53c690833f24180f36142a"
      version "0.1.3"
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
