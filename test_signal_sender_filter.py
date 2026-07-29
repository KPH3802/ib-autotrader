#!/usr/bin/env python3
"""Standalone proof of the signal-email sender filter (added 2026-07-29).

This repo has no test harness, so this is a self-contained script: run it directly,
it prints PASS/FAIL per case and exits non-zero on any failure. It does NOT connect
to Gmail, does NOT read credentials, and prints no address — the sender is compared
by SHA-256 fingerprint prefix only (A13).

WHAT IS BEING PROVEN. Every IMAP search selected on SUBJECT + SINCE across the whole
INBOX with no sender restriction. Once general mail is forwarded into this mailbox:

  * CROWD-OUT — SEARCH returns ascending sequence order and the readers take the
    TAIL (msg_ids[-3:] / [-2:], the most recently arrived matches). A forwarded mail
    matching a loose fragment and arriving after the scanner's mail takes those
    slots, so the real signal is never fetched. It presents as "no signals today".
  * WATCHDOG BLINDNESS — check_scanner_watchdog() only asks whether ANYTHING matched
    "PEAD"; one forwarded mail with that string suppresses a real silence alert.

Signal INJECTION was never possible (each reader re-checks the strict
"<NAME> BULL:/BEAR:" form before parsing), and case 6 pins that that guard is intact.

    $ python3 test_signal_sender_filter.py
"""

import hashlib
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent / "ib_autotrader.py"
TEXT = SRC.read_text()

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print(("  PASS  " if ok else "  FAIL  ") + name + (f"\n          {detail}" if detail else ""))


def _fp(v):
    return hashlib.sha256(str(v).encode()).hexdigest()[:8]


print("signal sender-filter proof —", SRC.name)
print()

# --- 1. every signal search carries the FROM clause ------------------------- #
# Parsed from the source rather than executed: running the real searches would
# need live credentials and a network round-trip, which this must not do.
search_lines = [ln.strip() for ln in TEXT.splitlines() if "mail.search(None" in ln]
predicate_lines = [ln for ln in search_lines if "SUBJECT" in ln or "q)" in ln]
check("every mail.search call site was found",
      len(search_lines) >= 8, f"found {len(search_lines)} search call sites")

# The multi-line predicates (8-K x2, dividend) build their f-string on the NEXT line.
blocks = re.findall(r"mail\.search\(\s*None,\s*\n?\s*(.+?)\)\n", TEXT)
inline = re.findall(r"mail\.search\(None,\s*(.+?)\)\n", TEXT)
all_predicates = [b for b in blocks + inline if "SUBJECT" in b]
missing = [p for p in all_predicates if "_signal_from_clause()" not in p]
check("every SUBJECT predicate includes _signal_from_clause()",
      not missing, f"unfiltered: {missing}" if missing else
      f"{len(all_predicates)} predicates, all filtered")

# The watchdog builds its query into `q` first.
q_line = next((ln for ln in TEXT.splitlines() if ln.strip().startswith("q = (")), "")
check("the watchdog query is filtered too (the no-second-guard path)",
      "_signal_from_clause()" in q_line, q_line.strip() or "q assignment not found")

# --- 2. the sender resolves, and to the RIGHT identity ---------------------- #
sys.path.insert(0, str(SRC.parent))
try:
    import config
except Exception as exc:                                    # pragma: no cover
    print(f"  SKIP  config.py not importable ({type(exc).__name__}) — cannot check identity")
    config = None

if config is not None:
    imap_user = getattr(config, "IMAP_USER", "")
    email_sender = getattr(config, "EMAIL_SENDER", "")
    check("SIGNAL_SENDER is bound to config.IMAP_USER",
          "SIGNAL_SENDER = getattr(config, \"IMAP_USER\"" in TEXT,
          f"IMAP_USER fingerprint {_fp(imap_user)}")
    # The scanners send FROM their own config.EMAIL_SENDER, which is this mailbox.
    # ib_execution's own EMAIL_SENDER is a DIFFERENT outbound address — filtering on
    # it would discard every real signal, so the distinction is load-bearing.
    check("the filter does NOT use ib_execution's own EMAIL_SENDER",
          _fp(imap_user) != _fp(email_sender),
          f"IMAP_USER {_fp(imap_user)} != EMAIL_SENDER {_fp(email_sender)} "
          f"(sender domain {str(email_sender).split('@')[-1]})")

    for name in ("pead_scanner", "cot_scanner", "si_scanner"):
        cfg = SRC.parent.parent / name / "config.py"
        if not cfg.exists():
            continue
        import importlib.util
        spec = importlib.util.spec_from_file_location(f"cfg_{name}", cfg)
        m = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(m)
        except Exception:
            continue
        check(f"{name} sends from the address the filter allows",
              _fp(getattr(m, "EMAIL_SENDER", "")) == _fp(imap_user),
              f"{name}.EMAIL_SENDER fp={_fp(getattr(m, 'EMAIL_SENDER', ''))}")

# --- 3. the clause is well-formed IMAP ------------------------------------- #
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location("_ib", SRC)
_mod = _ilu.module_from_spec(_spec)
try:
    _spec.loader.exec_module(_mod)
    clause = _mod._signal_from_clause()
    check("clause is a well-formed IMAP FROM term ending in a space",
          re.fullmatch(r'FROM "[^"]+" ', clause) is not None,
          f'shape: FROM "<{_fp(clause)}>" (value not printed)')
    built = f'({clause}SUBJECT "COT" SINCE "01-Jan-2026")'
    check("a built predicate is balanced and keeps SUBJECT + SINCE",
          built.count("(") == built.count(")") and "SUBJECT" in built and "SINCE" in built,
          'shape: (FROM "..." SUBJECT "COT" SINCE "01-Jan-2026")')
    # Unset sender must NOT silently match nothing — that would read as a quiet
    # market and stop all trading with no error.
    _saved = _mod.SIGNAL_SENDER
    _mod.SIGNAL_SENDER = ""
    _mod.system_warnings.clear()
    fallback = _mod._signal_from_clause()
    check("an unset sender falls back to unfiltered AND warns loudly",
          fallback == "" and any("SIGNAL_SENDER unset" in w for w in _mod.system_warnings),
          f"warnings raised: {len(_mod.system_warnings)}")
    _mod.SIGNAL_SENDER = _saved
except Exception as exc:
    check("module imports and clause builds", False, f"{type(exc).__name__}: {exc}")

# --- 4. the injection guard is still intact -------------------------------- #
guards = re.findall(r"if '([A-Z0-9 ]+:)' not in subj", TEXT)
check("strict subject guards still present on the parsing paths",
      len(guards) >= 5, f"guards: {sorted(set(guards))}")

print()
failed = [n for n, ok, _ in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
if failed:
    print("FAILED: " + ", ".join(failed))
sys.exit(1 if failed else 0)
