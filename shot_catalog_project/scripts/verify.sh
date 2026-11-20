#!/usr/bin/env bash
#
# One-command verification. After dependencies are installed (scripts/bootstrap.sh),
# this finishes within ~80 seconds on the reference machine and prints readable
# PASS/FAIL sections covering every required behavior:
#
#   1. automated tests: out-of-order/duplicate delivery, mixed v1/v2 revisions,
#      withdraw/republish, cross-page batch atomicity, kill-and-restart recovery;
#   2. end-to-end mixed-version demo with live HTTP queries;
#   3. a cross-page same-batch revision on a 3,000-shot batch (15+ log pages;
#      scaled down from 100k only to honor the 80s budget). The full 100k case
#      runs on demand via: python3 -I scripts/demo_large_batch.py
#
# Usage:  scripts/verify.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PYLIBS="${PYLIBS:-/workspace/pylibs}"
SITE="$PYLIBS/lib/python3.11/site-packages"
export PYTHONPATH="$ROOT/src:$SITE"
PY=python3

section() { printf "\n\033[1;36m== %s ==\033[0m\n" "$1"; }
fail() { printf "\033[1;31m%s\033[0m\n" "FAILED: $1"; exit 1; }

cd "$ROOT"
START=$(date +%s)

section "1/4 automated tests: representative node from every required scenario"
# One focused test per required behavior keeps the run inside the 80s budget;
# the FULL suite (39 tests) is available anytime via: python3 -I run_tests.py
"$PY" -I run_tests.py -q \
  tests/test_log_format.py::test_v1_duration_seconds_convert_to_milliseconds \
  tests/test_log_format.py::test_explicit_null_clears_but_still_present \
  tests/test_sync_semantics.py::test_full_sync_matches_expected_history \
  tests/test_sync_semantics.py::test_v1_revision_cannot_erase_v2_repair_note \
  tests/test_sync_semantics.py::test_withdrawn_shot_is_unretrievable \
  tests/test_sync_semantics.py::test_republished_shot_is_retrievable \
  tests/test_batch_atomicity.py::test_half_staged_batch_is_completely_invisible \
  tests/test_batch_atomicity.py::test_batch_spans_many_pages_and_commits_as_one \
  tests/test_recovery.py::test_reopen_continues_from_durable_prefix \
  tests/test_recovery.py::test_sigkill_then_restart_resumes \
  tests/test_http_api.py::test_shot_and_film_endpoints \
  tests/test_sparse_subscription.py::test_only_subscribed_films_are_transferred \
  tests/test_sparse_subscription.py::test_cross_film_batch_publishes_subscribed_survivors_atomically \
  tests/test_sparse_subscription.py::test_append_subscription_backfills_only_the_new_film \
  tests/test_sparse_subscription.py::test_backfill_interrupt_resumes_from_durable_progress \
  || fail "representative pytest subset"

section "2/4 end-to-end mixed-version demo"
"$PY" -I scripts/demo.py || fail "mixed-version demo"

section "3/4 cross-page same-batch revision (3,000 shots, 15+ pages)"
"$PY" -I scripts/demo_large_batch.py 3000 || fail "large-batch demo"

section "4/4 sparse subscriptions: transfer, cross-film batch, backfill resume"
"$PY" -I scripts/demo_sparse_subscription.py || fail "sparse-subscription demo"

ELAPSED=$(( $(date +%s) - START ))
printf "\n\033[1;32mALL CHECKS PASSED in %ds (budget 80s)\033[0m\n" "$ELAPSED"
[ "$ELAPSED" -le 80 ] || printf "\033[1;33mnote: over the 80s reference budget on this host\033[0m\n"
