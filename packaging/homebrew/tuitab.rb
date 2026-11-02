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
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.0/tuitab-v0.11.0-aarch64-apple-darwin.tar.gz"
      sha256 "6fa99052d5f19f4ec73f37e5cc61d349ba821829a2873840b1ba28cea7b17fde"
      version "0.11.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.0/tuitab-v0.11.0-x86_64-apple-darwin.tar.gz"
      sha256 "cf78ce21edee2fd3ca021af720dc1ad60afbcf43640c69e2b44dfefe08e1b6af"
      version "0.11.0"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.0/tuitab-v0.11.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "5c5c2acfb346f09f848eb634e37cc6189bbe48eb424d7f087ec3420c3b112318"
      version "0.11.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.0/tuitab-v0.11.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "4bc46bceadbaf93a51f299d702ec844cc9ec698b4b62ad444ac8b34d6620af60"
      version "0.11.0"
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
