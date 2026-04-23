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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.2/tuitab-v0.3.2-aarch64-apple-darwin.tar.gz"
      sha256 "FILL_IN_AFTER_RELEASE"
      version "0.3.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.2/tuitab-v0.3.2-x86_64-apple-darwin.tar.gz"
      sha256 "FILL_IN_AFTER_RELEASE"
      version "0.3.2"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.2/tuitab-v0.3.2-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "FILL_IN_AFTER_RELEASE"
      version "0.3.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.2/tuitab-v0.3.2-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "FILL_IN_AFTER_RELEASE"
      version "0.3.2"
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
