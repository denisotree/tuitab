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
    assert!(
        install < build,
        "apt-utils is installed after publish.sh runs"
    );
}

fn read(path: &str) -> String {
    std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path))
        .unwrap_or_else(|e| panic!("{path}: {e}"))
}

/// tuitab is frozen: Dependabot must not open version-bump PRs every week. Its security
/// alerts are a repository setting and stay on without this file.
#[test]
fn dependabot_version_updates_are_off() {
    assert!(!std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github/dependabot.yml")
        .exists());
}

/// Nothing is pushed to a frozen repository, so cargo audit runs on a schedule too — that
/// is how a new advisory such as RUSTSEC-2026-0459 gets noticed. GitHub suspends the
/// schedule of a public repository after 60 days without activity, so the workflow
/// re-enables itself each run, which resets that counter. CI calls the same workflow, so
/// the list of ignored advisories lives in one place.
#[test]
fn cargo_audit_runs_weekly_and_keeps_its_schedule_alive() {
    let audit = read(".github/workflows/audit.yml");
    assert!(audit.contains("schedule:"), "audit.yml has no schedule");
    assert!(audit.contains("workflow_call:"), "CI cannot call audit.yml");
    assert!(audit.contains("cargo audit"));
    assert!(
        audit.contains("gh workflow enable audit.yml"),
        "the schedule is not kept alive"
    );
    assert!(audit.contains("actions: write"));

    let ci = read(".github/workflows/ci.yml");
    assert!(ci.contains("uses: ./.github/workflows/audit.yml"));
    assert!(
        !ci.contains("cargo audit"),
        "ci.yml keeps its own copy of the audit"
    );
}

/// SECURITY.md says which release is supported and where everything else goes.
#[test]
fn the_security_policy_names_the_frozen_release() {
    let policy = read("SECURITY.md");
    assert!(policy.contains("0.11.2"));
    assert!(policy.contains("https://github.com/denisotree/rowq"));
    assert!(!policy.contains("| latest"));
}

/// A new issue is offered the bug template and a link to rowq, never a blank form; a
/// frozen release takes no feature requests.
#[test]
fn new_issues_are_pointed_at_rowq() {
    assert!(!std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(".github/ISSUE_TEMPLATE/feature_request.md")
        .exists());
    let config = read(".github/ISSUE_TEMPLATE/config.yml");
    assert!(config.contains("blank_issues_enabled: false"));
    assert!(config.contains("https://github.com/denisotree/rowq/issues"));
}
