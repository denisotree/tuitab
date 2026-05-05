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
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.1/tuitab-v0.4.1-aarch64-apple-darwin.tar.gz"
      sha256 "a0fc25e8d24458e4c25d92284997f0025f23c2d030dd4e0252c87ed5520d910a"
      version "0.4.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.1/tuitab-v0.4.1-x86_64-apple-darwin.tar.gz"
      sha256 "c0e112517da7df3b6ff30a24c36c5fafc84cb709686d76f82bf8853d31f52f9a"
      version "0.4.1"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.1/tuitab-v0.4.1-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "267fa961cab1367315206d4e566561a2be38ef52521a3ff1ba7e67ee6be45f20"
      version "0.4.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.1/tuitab-v0.4.1-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "42c7a32539fbcf999b6a26f094a6b84d6fbd481fefba84ad7c7d4468626d0ea5"
      version "0.4.1"
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
