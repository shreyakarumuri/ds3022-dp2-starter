# Project 2 checkpoints

Work through these in order and tick them off in your own copy. They are the same whether you are in class or working on your own.
Update **PROGRESS.md** (done / next / blocked) at the end of every work session.

## Checkpoint A (Lesson 9, Mon 05 Oct): queue up and visible

- [x] Virtual environment active, `.env` has your `UVA_ID`, and `python kit/smoke_test.py` prints `Emulator OK`
- [x] `populate_queue` POSTs to the scatter API and logs your queue URL
- [x] `get_counts` and `monitor_queue` log the visible, not visible and delayed counts until Delayed reaches 0
- [x] The flow run is **Completed** and visible in the Prefect dashboard; screenshot saved in `docs/`
- [x] DAG sketch saved as `docs/dag.jpg`; polling strategy explained in your README
- [x] `PROGRESS.md` updated and everything pushed

## Checkpoints B to D: required, target for Lesson 10 (Thu 08 Oct)

Aim to finish B, C and D in Lesson 10. If you do not finish in class, complete them before Lesson 11 (Mon 19 Oct). All three are graded.

## Checkpoint B: collect without losing anything

- [ ] `collect_messages` receives until 21 fragments are stored or a timeout is reached, logging progress
- [ ] Uses `MessageAttributeNames=["All"]` and `resp.get("Messages", [])`
- [ ] Each fragment is inserted into `raw.fragments` (DuckDB) **before** `delete_message`
- [ ] After a run, all three queue counters are 0 (no dangling messages)
- [ ] Clear failure message if fewer than 21 fragments arrive before the timeout

## Checkpoint C: dbt quality gate

- [ ] `dbt/models/staging/stg_fragments.sql` (integer `order_no`, cleaned `word`, safe against duplicates)
- [ ] `dbt/models/staging/schema.yml` with generic tests
- [ ] A singular test in `dbt/tests/` for "exactly 21 fragments, no gaps"
- [ ] `dbt/models/marts/assembled_phrase.sql` returns the ordered phrase
- [ ] You made the tests **fail on purpose** (delete a row, duplicate a row in `raw.fragments`) and then pass again; screenshots in `docs/`
- [ ] The flow's dbt task raises on failure, so submission never runs on a bad phrase

## Checkpoint D: submit and verify

- [ ] `submit_solution` sends `uvaid`, `phrase`, `platform="prefect"` to `dp2-submit` and logs the HTTP status (200)
- [ ] `python kit/check_submission.py` prints `PASS`; receipt code pasted in your README
- [ ] `git status` shows no `.env`, `.duckdb`, `dbt/target/` or log files

## Checkpoint E: optional stretch (any order, before Lesson 11)

Not graded on its own. The first item is a recommended rehearsal for the required evidence run in Checkpoint F.

- [ ] Rehearse a full-delay run (`DP2_FAST=0`, restart the scatter API) once, so you know it works before Lesson 11
- [ ] Flow served with `.serve()` and triggered from the dashboard, with the POST guarded so a scheduled run does not re-scatter
- [ ] README peer test: a classmate set up and ran your project from the README alone
- [ ] Same code run against ElasticMQ by only switching emulators

## Checkpoint F (Lesson 11, Mon 19 Oct): evidence run and chaos round

Details are given in class. Everything here counts towards the robustness part of the grade.

- [ ] Full-delay evidence run (`DP2_FAST=0`) completed; dashboard screenshot and receipt code in README
- [ ] Chaos mode (`DP2_CHAOS=1`): two consecutive runs end in PASS
- [ ] Duplicate messages are counted once
- [ ] Malformed messages are logged or stored **and deleted**; all queue counters are 0 at the end
- [ ] README section "Chaos round": what broke, why, and how you fixed it
- [ ] Repo link and self-assessed rubric submitted by **23:59, Mon 19 Oct**
