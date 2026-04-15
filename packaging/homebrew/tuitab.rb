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
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.5/tuitab-v0.1.5-aarch64-apple-darwin.tar.gz"
      sha256 "3ac6b029d56d41880163044d0b4e8fef555bbfd901d20516e671d0f80723c9cf"
      version "0.1.5"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.5/tuitab-v0.1.5-x86_64-apple-darwin.tar.gz"
      sha256 "80d434257ea9dec8b4f8330ea95b69a3fe4c31b39d87093c5a95b972a951acdc"
      version "0.1.5"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.5/tuitab-v0.1.5-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "5f94bf42d7001abaebb217f6b2c5f75ccfcb2fdbc09948071980608d28fd798b"
      version "0.1.5"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.5/tuitab-v0.1.5-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "98ada82325a3a598118b7ea023432c7d1f8bd8dd43ec60d822092fb3928ce7e7"
      version "0.1.5"
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
