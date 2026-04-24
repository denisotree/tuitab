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
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.4/tuitab-v0.3.4-aarch64-apple-darwin.tar.gz"
      sha256 "2d1f543508bc7bfabd2c975609ce23fc642dc8111694044d1cfef8f46ae476ac"
      version "0.3.4"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.4/tuitab-v0.3.4-x86_64-apple-darwin.tar.gz"
      sha256 "a1a47f3cc3b831baeabee835db809794912025ae4ac3087d69d0f48c693ea624"
      version "0.3.4"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.4/tuitab-v0.3.4-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "55bf4971bc3a273bf25fdf2f734366083e3ab93e731a5bc238d9bbd5df64e180"
      version "0.3.4"
    end
    on_intel do
      url "https://github.com/denisotree/tuitab/releases/download/v0.3.4/tuitab-v0.3.4-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "436398fd1caff9c095714ec641b72ae05a3650ef44a5448cbca39f62d31e1030"
      version "0.3.4"
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
