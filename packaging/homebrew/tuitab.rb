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
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.2/tuitab-v0.11.2-aarch64-apple-darwin.tar.gz"
      sha256 "502e716011b9034ed6b5d037931cdbabaffb65a9d4551b5d5d5e1441acb45f53"
      version "0.11.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.2/tuitab-v0.11.2-x86_64-apple-darwin.tar.gz"
      sha256 "c338953a0664f1dfc0559b1bd7c367329a22126e4d2785b3cf2b0ce133881d5f"
      version "0.11.2"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.2/tuitab-v0.11.2-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "db8300b8f3a65c1a17e616d1868c939ee465e8113f2214f5f8ea010e47f73bbc"
      version "0.11.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.2/tuitab-v0.11.2-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "3de7c51e8996d3b2dfe55c9f63df2e7af30efd632cbfcff053df4621eb926681"
      version "0.11.2"
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
