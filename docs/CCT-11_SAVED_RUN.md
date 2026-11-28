# Replay a recorded Boutique fault

Merul packaged the final reserved productcatalog delay trial, `cct-probe-cct10-test-20261128c-02`, from the [published campaign receipt](evidence/CCT-10-validation-20261128.json). The [HTML demo](demo/index.html) and [JSON summary](demo/summary.json) are included. The default replay needs Python 3.9 or later and works offline.

```sh
python3 -m analytics.saved_run
python3 -m unittest discover -s tests -p 'test_saved_run.py'
```

Open `outputs/cct11-demo/index.html`. Compare the two generated files with `docs/demo/` using `diff -ru docs/demo outputs/cct11-demo` in a clean output directory.

1. Follow the caller-to-callee graph. A retained browse, cart and checkout trace verified all 4, 2 and 9 required relationships respectively. Repeated pairs collapse into one directed edge. The graph shows the verified subset of those three examples. Additional edges and call frequencies need the raw trace records.
2. Follow the controller events and the three measured phases. Across 5,415 calls before the delay, 1,350 during the observed delay and 5,400 after recovery, median catalog RPC latency was 0.274, 106.930 and 0.282 ms. Calls crossing phase boundaries are excluded. The controller records seconds and the monitor records milliseconds.
3. Read the control intervals and health-label totals. The independent rule raised one alarm before injection and zero after recovery plus 30 seconds. Over the trial the two label channels agreed on 16 affected and 478 healthy service-windows. Another 36 had unknown labels in at least one channel.
4. Compare the five forecasts at 30 seconds. All scored F1 0.0 under the supplementary policy frozen before this trial. Mean scorable coverage was 86.7%.

### Five-second view from the team's handoff

The public receipt omits the individual five-second label assignments. For the service-by-service timeline, use the extracted `cct-validation-20261128-04` folder from the November 28 handoff. The November 25 shopping outputs ZIP predates this campaign.

```sh
python3 -m analytics.saved_run --campaign-dir /path/to/cct-validation-20261128-04 --output outputs/cct11-detail
```

The command reads just `runs/cct-probe-cct10-test-20261128c-02/heldout-label-validation.json`, checks the SHA-256 against the published campaign manifest and displays the independent labels, including unknowns. A teammate can supply just that file under the same relative path. The public reproduction covers the fault, verified graph, phase and control timeline, and recovery. Reproduction of the detailed label matrix with the original handoff remains pending on Merul's side.

### What the missed forecasts tell us

The no-additional-impact baseline always predicts an empty set. Its final-run receipt reports 53 decisions and 42 empty/empty cutoffs at 30 seconds. The remaining **11 cutoffs contained at least one scorable onset**. These overlapping horizons can include the same service and onset. Each method had 59 censored service/cutoff instances and zero mean F1. The original conservative policy had 53 empty/empty cutoffs and undefined F1 for that baseline. Both policies retain their original denominators.

The observer logged seven retried or missed frame ends because reference requests were still in flight. Six frame ends fell inside the observed delay. A retry can succeed later. The public counts alone cannot attribute any individual miss to a late frame. The four graph methods expand from services already classified as affected. If no eligible caller can be reached from that set, their forecast is empty even when a service is approaching its own threshold.

For the next campaign Merul implemented an opt-in `--near-limit` comparator in `scripts/validate_cct10.py`. At a 30-second horizon it warns on a currently healthy service when the larger of its observed p95-to-limit or error-fraction-to-limit ratios reaches 0.8. This fixed candidate can fire before any affected graph neighbor is visible. It can also introduce false alarms. The normal five baselines remain reproducible. The runner writes the prospective plan, split and code hashes before capture, records actual decision times, and scores all six methods against the same independent outcome channel under observed-pending-v2. Retain the original conservative scores separately.

Mithil and Merul should review the rule together and reserve fresh whole-run IDs before collection. Report per-run TP/FP/FN, precision, recall, F1, coverage, empty cutoffs and signed lead times from the saved evaluation rows. Examine missing frames, eligibility, the first confirmed affected service and the reverse path for each missed onset. Count improvements and extra false alarms on the same eligible examples. The new method has implementation tests only; the displayed trial informed its design.

### Merul's independent reproduction

Merul checked the November 25 original shopping receipt against the published replay. All 36 SERVER spans, 42 metric points and 14 selected logs matched, and the 10 five-second service rows reproduced. The first pod gauge arrived 20.199 seconds after that interval. The historical rows retain missing readiness/restart values and incomplete coverage. The same review recomputed all four matched tracing-overhead deltas, including p95 from 21 to 32 ms in one off/on pair. See the [review receipt](evidence/CCT-Merul-independent-review-20261128.json).

```sh
python3 -m pip install -r processing/requirements-verify.txt
python3 -m observability.review_recorded
# If the original November 25 measured directory is available:
python3 -m observability.review_recorded --measured /path/to/measured
```

Continuous ingestion delay belongs to Param's live validation. Additional fault types and targets belong to Komalika's experiments. Each teammate can reproduce the public demo from a fresh checkout and record their own result.

