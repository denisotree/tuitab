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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.7/tuitab-v0.3.7-aarch64-apple-darwin.tar.gz"
      sha256 "ad30f13681d99b17d6440311cafb30ce402a1b366fd4b04a9338ed872f1d6278"
      version "0.3.7"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.7/tuitab-v0.3.7-x86_64-apple-darwin.tar.gz"
      sha256 "ba2036278d13a65c7e6156dd0371a42f3f0850fcf3b04590a89604b5700ccfb2"
      version "0.3.7"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.7/tuitab-v0.3.7-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "b2ee5ac2b1f0405b01d620ac43ad08eaec6944f31b973b20b7ac60ab36ed785e"
      version "0.3.7"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.7/tuitab-v0.3.7-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "aa183209e7a4391ba762a6070ca16f5ec7a16cf090da957bc3391199971324da"
      version "0.3.7"
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
