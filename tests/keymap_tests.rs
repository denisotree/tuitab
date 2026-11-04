//! Every key the key layer answers is described, and every described key is answered.
//!
//! `zJ` went into `handle_key_event` and the docs but not into the `z` hint or `?`,
//! so a person pressing `z` never learned it existed. The `?` popup and the prefix
//! hints are built from one table, `tuitab::ui::popup::SECTIONS`; this checks that
//! table against the key layer in both directions, and the docs against both.

use crossterm::event::{KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers};
use tuitab::event::handle_key_event;
use tuitab::types::{Action, AppMode};
use tuitab::ui::popup::SECTIONS;

const PREFIXES: [(char, AppMode); 4] = [
    ('g', AppMode::GPrefix),
    ('z', AppMode::ZPrefix),
    ('y', AppMode::YPrefix),
    ('S', AppMode::SPrefix),
];

fn press(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
    KeyEvent {
        code,
        modifiers,
        kind: KeyEventKind::Press,
        state: KeyEventState::NONE,
    }
}

/// One key as the help writes it: `j`, `Ctrl+F`, `Enter`, `←`.
fn single(name: &str) -> Option<KeyEvent> {
    let named = match name {
        "Enter" => KeyCode::Enter,
        "Esc" => KeyCode::Esc,
        "Home" => KeyCode::Home,
        "End" => KeyCode::End,
        "PageUp" => KeyCode::PageUp,
        "PageDown" => KeyCode::PageDown,
        "Backspace" => KeyCode::Backspace,
        "Tab" => KeyCode::Tab,
        "←" => KeyCode::Left,
        "→" => KeyCode::Right,
        "↑" => KeyCode::Up,
        "↓" => KeyCode::Down,
        _ => {
            if let Some(c) = name.strip_prefix("Ctrl+") {
                let mut chars = c.chars();
                let (Some(c), None) = (chars.next(), chars.next()) else {
                    return None;
                };
                return Some(press(
                    KeyCode::Char(c.to_ascii_lowercase()),
                    KeyModifiers::CONTROL,
                ));
            }
            let mut chars = name.chars();
            let (Some(c), None) = (chars.next(), chars.next()) else {
                return None;
            };
            KeyCode::Char(c)
        }
    };
    Some(press(named, KeyModifiers::NONE))
}

/// What a key as the help writes it does — `zJ` is `z`, then `J` in the z-prefix.
fn resolve(name: &str) -> Option<Action> {
    if let Some(key) = single(name) {
        return Some(handle_key_event(key, AppMode::Normal, false));
    }
    let mut chars = name.chars();
    let first = chars.next()?;
    let (_, mode) = PREFIXES.iter().find(|(p, _)| *p == first)?;
    let rest = single(chars.as_str())?;
    Some(handle_key_event(rest, *mode, false))
}

fn ignored(action: &Action) -> bool {
    matches!(
        action,
        Action::None
            | Action::CancelGPrefix
            | Action::CancelZPrefix
            | Action::CancelYPrefix
            | Action::CancelSPrefix
            | Action::EnterGPrefix
            | Action::EnterZPrefix
            | Action::EnterYPrefix
            | Action::EnterSPrefix
    )
}

/// Every key the key layer answers in Normal mode and in each prefix, by what it does.
fn bound() -> Vec<(Action, Vec<String>)> {
    let mut names: Vec<String> = (0x21u8..0x7f).map(|b| (b as char).to_string()).collect();
    names.extend(
        [
            "Enter",
            "Esc",
            "Home",
            "End",
            "PageUp",
            "PageDown",
            "Backspace",
            "Tab",
            "←",
            "→",
            "↑",
            "↓",
        ]
        .map(String::from),
    );
    let mut all: Vec<String> = names.clone();
    all.extend(('a'..='z').map(|c| format!("Ctrl+{}", c.to_ascii_uppercase())));
    for (p, _) in PREFIXES {
        all.extend(names.iter().map(|n| format!("{p}{n}")));
    }

    let mut out: Vec<(Action, Vec<String>)> = Vec::new();
    for name in all {
        let action = resolve(&name).expect("every generated name parses");
        if ignored(&action) {
            continue;
        }
        match out.iter_mut().find(|(a, _)| *a == action) {
            Some((_, keys)) => keys.push(name),
            None => out.push((action, vec![name])),
        }
    }
    out
}

fn described() -> Vec<&'static str> {
    SECTIONS
        .iter()
        .flat_map(|s| s.keys.iter())
        .flat_map(|k| k.keys.iter().copied())
        .collect()
}

#[test]
fn every_bound_key_is_in_the_help() {
    let help = described();
    let missing: Vec<String> = bound()
        .into_iter()
        .filter(|(_, keys)| !keys.iter().any(|k| help.contains(&k.as_str())))
        .map(|(action, keys)| format!("{action:?}: {}", keys.join(" ")))
        .collect();
    assert!(
        missing.is_empty(),
        "not in the help:\n{}",
        missing.join("\n")
    );
}

#[test]
fn every_key_in_the_help_does_something() {
    let dead: Vec<&str> = described()
        .into_iter()
        .filter(|k| resolve(k).is_none_or(|a| ignored(&a)))
        .collect();
    assert!(
        dead.is_empty(),
        "the help names keys that do nothing: {dead:?}"
    );
}

/// How the docs write a key: `zJ`, and `Shift+S` `r` for the S-prefix.
fn in_doc(doc: &str, key: &str) -> bool {
    // A markdown table cell escapes its pipes.
    if doc.contains(&format!("`{key}`")) || doc.contains(&format!("`{}`", key.replace('|', "\\|")))
    {
        return true;
    }
    key.strip_prefix('S')
        .filter(|r| !r.is_empty())
        .is_some_and(|r| doc.contains(&format!("`Shift+S` `{r}`")))
}

#[test]
fn every_bound_key_is_in_the_docs() {
    for path in ["docs/en/keybindings.md", "docs/ru/keybindings.md"] {
        let doc = std::fs::read_to_string(path).unwrap();
        let missing: Vec<String> = bound()
            .into_iter()
            .filter(|(_, keys)| !keys.iter().any(|k| in_doc(&doc, k)))
            .map(|(action, keys)| format!("{action:?}: {}", keys.join(" ")))
            .collect();
        assert!(missing.is_empty(), "not in {path}:\n{}", missing.join("\n"));
    }
}

/// The keys a docs table lists for Normal mode and its prefixes — its first cell's
/// backticked tokens. Tables for other modes (the chart, text prompts) are skipped.
fn doc_keys(doc: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut other_mode = false;
    for line in doc.lines() {
        if let Some(title) = line.strip_prefix("## ") {
            other_mode = ["Charts", "Input", "Режим графика", "Режимы ввода"]
                .iter()
                .any(|t| title.starts_with(t));
            continue;
        }
        let Some(cell) = line
            .strip_prefix("| `")
            .map(|r| r.split(" | ").next().unwrap_or(r))
        else {
            continue;
        };
        if other_mode {
            continue;
        }
        let cell = format!("`{cell}").replace("\\|", "|");
        let tokens: Vec<&str> = cell.split('`').skip(1).step_by(2).collect();
        // `Shift+S` `r` is one chord.
        let mut i = 0;
        while i < tokens.len() {
            match (tokens[i], tokens.get(i + 1)) {
                ("Shift+S", Some(r)) => {
                    out.push(format!("S{r}"));
                    i += 2;
                }
                (t, _) => {
                    out.push(t.to_string());
                    i += 1;
                }
            }
        }
    }
    out
}

#[test]
fn every_key_in_the_docs_does_something() {
    for path in ["docs/en/keybindings.md", "docs/ru/keybindings.md"] {
        let doc = std::fs::read_to_string(path).unwrap();
        let keys = doc_keys(&doc);
        for read in ["zJ", "Ctrl+S", "Sr", "|", "gg"] {
            assert!(
                keys.iter().any(|k| k == read),
                "{path}: {read} was not read: {keys:?}"
            );
        }
        let dead: Vec<String> = keys
            .into_iter()
            .filter(|k| {
                let k = k.replace("Shift+1", "!").replace("Shift+U", "U");
                resolve(&k).is_none_or(|a| ignored(&a))
            })
            .collect();
        assert!(
            dead.is_empty(),
            "{path} names keys that do nothing: {dead:?}"
        );
    }
}
