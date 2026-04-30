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
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.0/tuitab-v0.4.0-aarch64-apple-darwin.tar.gz"
      sha256 "638da9a211d839af607c034b06e7dd516257596d6d16717edf4246faa2d92790"
      version "0.4.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.0/tuitab-v0.4.0-x86_64-apple-darwin.tar.gz"
      sha256 "306b45af8c512a864cf4083e38f88c6364e4ccd067d75d138c946453e6393834"
      version "0.4.0"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.0/tuitab-v0.4.0-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "18a18260536013786d6f2512b2989fbb8ffc2bf060d71a32cd32e54bc6a34dfc"
      version "0.4.0"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.4.0/tuitab-v0.4.0-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "765e77ee29a651bb6dec621ea3feb9e73250f2e5de2a275dea6b690c53bb0035"
      version "0.4.0"
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
