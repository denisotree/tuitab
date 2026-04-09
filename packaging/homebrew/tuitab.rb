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
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.1/tuitab-v0.1.1-aarch64-apple-darwin.tar.gz"
      sha256 "32fa88ea3e349e41ad838d6fdb8b617098348da1e0fd9fbf5eedc18c2c4658c0"
      version "0.1.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.1/tuitab-v0.1.1-x86_64-apple-darwin.tar.gz"
      sha256 "afdafcd695ccc459098d4b1c40a40ac7e23de5f03a7ba5d737159581b1e6bc3f"
      version "0.1.1"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.1/tuitab-v0.1.1-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "2a1cf4dcd7415f8532e87c4a8707b8d948f4f274b84699da69ef44afb6027584"
      version "0.1.1"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.1.1/tuitab-v0.1.1-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "c54975320295a98db1a2420b255bb961eacc8cd9cb7c6f44ceb92d30196484b1"
      version "0.1.1"
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
