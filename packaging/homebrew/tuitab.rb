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
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.3/tuitab-v0.11.3-aarch64-apple-darwin.tar.gz"
      sha256 "41bf195fc93b8515fa7da4e96ce6eda1644c396191804b4e0be9b17af3f3c2f3"
      version "0.11.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.3/tuitab-v0.11.3-x86_64-apple-darwin.tar.gz"
      sha256 "e67f285e1f77a961b0dbbd7aeb7a0f2dcee7a2e36e1025bee98ab8723cb9d59b"
      version "0.11.3"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.3/tuitab-v0.11.3-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "71b8f281d6187050cb473485f42866e646fdb52ac346db42e81f8fa7fc224f7c"
      version "0.11.3"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.11.3/tuitab-v0.11.3-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "5a06ed2aa29e56669e41a11aaa1d87d5b4368ad42111526b1b6823e75ba7049a"
      version "0.11.3"
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
