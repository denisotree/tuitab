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
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.0/tuitab-v0.10.0-aarch64-apple-darwin.tar.gz"
      sha256 "497e80f46ac0ab482d98cd89e90549df20bdfc82af8ea5a1272504b07f0efcf4"
      version "0.10.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.0/tuitab-v0.10.0-x86_64-apple-darwin.tar.gz"
      sha256 "ad3ca4c33fdb2000d3f115ee4b8ff5285f93c1b0a99dbe26f78256ba1cbd0d97"
      version "0.10.0"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.0/tuitab-v0.10.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "0dfa8c79bf87557845bbfb0e68d3c36d8bad3036c2a8dc24976282b6fabff5f7"
      version "0.10.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.0/tuitab-v0.10.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "a3bce0d9fb1919f97b5904316db16ca37ef776f53b80dd1589dc62347572fad3"
      version "0.10.0"
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
