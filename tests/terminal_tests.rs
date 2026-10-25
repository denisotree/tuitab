//! Started without a terminal, tuitab says so and exits instead of panicking.

#[cfg(unix)]
#[test]
fn without_a_terminal_tuitab_exits_with_an_error_not_a_panic() {
    use std::os::unix::process::CommandExt;
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_tuitab"));
    cmd.arg("test_data/sample.csv")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    // A new session has no controlling terminal, so `/dev/tty` cannot be opened either —
    // the state of a cron job, a CI step, or a shell escape that runs without one.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }
    let mut child = cmd.spawn().unwrap();
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(30) {
            let _ = child.kill();
            panic!("tuitab kept running without a terminal");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut stderr).unwrap();

    assert!(!status.success());
    assert!(!stderr.contains("panicked"), "{}", stderr);
    assert!(stderr.contains("needs a terminal"), "{}", stderr);
}
