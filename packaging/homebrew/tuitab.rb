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
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.2/tuitab-v0.10.2-aarch64-apple-darwin.tar.gz"
      sha256 "f021c881ea881f5aa2b68781b6b6ed56e822058fbca1b0bff96ca61436ef8941"
      version "0.10.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.2/tuitab-v0.10.2-x86_64-apple-darwin.tar.gz"
      sha256 "c4348967180bf836f3b95becf2e9f4665c12644c879d6e488670a8b87ba02c81"
      version "0.10.2"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.2/tuitab-v0.10.2-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "9201cd43a04d1a89b7ef23f72f51a246b740248d9ef33d8046cb8bc5732c6909"
      version "0.10.2"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.10.2/tuitab-v0.10.2-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "e9628e2d65a5245851a3a4f07b1db16577abeb17a67d2864cb591088d2a42337"
      version "0.10.2"
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
