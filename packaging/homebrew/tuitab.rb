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
  desc "Terminal tabular data explorer — CSV/JSON/YAML/TOML/Parquet/Excel/SQLite viewer"
  homepage "https://github.com/denisotree/tuitab"
  license "Apache-2.0"
  head "https://github.com/denisotree/tuitab.git", branch: "master"

  on_macos do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.7/tuitab-v0.9.7-aarch64-apple-darwin.tar.gz"
      sha256 "7e6fca010a6dff8f4a6224444c9558007ccc8b23779cba3c0928abd22d3b3331"
      version "0.9.7"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.7/tuitab-v0.9.7-x86_64-apple-darwin.tar.gz"
      sha256 "83830aa1cb2952a6be5dc9ed732519bcc912aced5ce5c967db8256734b2062e6"
      version "0.9.7"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.7/tuitab-v0.9.7-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "343f90d53f376f3c8f546097b0385cb1a71ca97a47c1f2d93abe47f01a45ed69"
      version "0.9.7"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.9.7/tuitab-v0.9.7-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "a7358f24c4318fa95a32e2daa021d87c45a8923ecf0d7c3007c1ed6b497d5858"
      version "0.9.7"
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
