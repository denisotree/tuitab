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
      url "https://github.com/denisotree/tuitab/releases/download/v0.2.0/tuitab-v0.2.0-aarch64-apple-darwin.tar.gz"
      sha256 "15785b6ac641a2421eccf124df49010bc73a01488b1d1fd8d61d305e7c4a2ccb"
      version "0.2.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.2.0/tuitab-v0.2.0-x86_64-apple-darwin.tar.gz"
      sha256 "0d2a11a987ca3755350e02e2f21e78b5ba71656f0c00e621cea2661115e568d3"
      version "0.2.0"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.2.0/tuitab-v0.2.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "2cb98e552aa22bbe103843038101a2aa373a92dd4884f2cbf6bd922e0cf5f881"
      version "0.2.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.2.0/tuitab-v0.2.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "da4ed5be2ef4d6fa1f2d0304832cc5fb09595a3ddf990ca15f8a32c511dbb21c"
      version "0.2.0"
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
