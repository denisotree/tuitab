//! The GitHub Actions workflows that publish releases.

/// publish.sh builds the APT indexes with apt-ftparchive, from apt-utils, which the
/// ubuntu-latest image stopped carrying with Ubuntu 26.04 (the 0.11.2 APT job died with
/// "apt-ftparchive: command not found"). The workflow installs it itself, before the
/// step that needs it.
#[test]
fn the_apt_job_installs_apt_ftparchive_before_using_it() {
    let workflow = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(".github/workflows/apt.yml"),
    )
    .unwrap();
    let install = workflow
        .find("apt-get install -y --no-install-recommends apt-utils")
        .expect("apt.yml does not install apt-utils");
    let build = workflow.find("packaging/apt/publish.sh").unwrap();
    assert!(install < build, "apt-utils is installed after publish.sh runs");
}
