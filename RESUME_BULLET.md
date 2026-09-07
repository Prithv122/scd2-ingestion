# Resume Bullets — scd2-ingestion

Form: **action → technical specifics → measured outcome.** Numbers or it doesn't go on the resume.

---

## Bullets

- Designed and implemented an order-independent SCD Type 2 merge for late-arriving and
  out-of-order dimensional data, proven (not just tested) via a randomized property test
  applying the same 4,115-event stream in 500 independent random arrival orders with
  zero mismatches in the final history.
- Diagnosed a real order-dependence bug in an initial incremental-merge design — found
  by a randomized property test that failed 100/100 trials despite passing every
  hand-written unit test — traced it to a structural flaw (committing to a version
  boundary from partial state), and redesigned around a buffer-then-sort-then-rebuild
  approach that is correct by construction.
- Built a DuckDB-backed analytical layer (point-in-time `as_of` queries, gap detection
  via window functions, category-change-frequency analysis) surfacing that all 50
  synthetic entities in the test catalog experienced at least one delete/recreate
  lifecycle cycle (579 cycles total) — the "boring" case most SCD2 implementations don't
  test against.
- Found and fixed a second bug via coverage-driven testing: a pandas dtype-inference gap
  (an all-null date column silently typed as INTEGER) that broke DuckDB's parameterized
  point-in-time queries for any entity with exactly one open version.

## Which roles this supports

- [x] Data Engineer
- [ ] Data Analyst / Python Developer
- [ ] Data Scientist / ML
- [ ] AI Engineer (LLM/NLP/CV)

## Keywords this project earns

Slowly Changing Dimension Type 2 (SCD2), idempotent ingestion, late-arriving /
out-of-order data, property-based / randomized testing, DuckDB, point-in-time queries,
bitemporal data modeling, pandas dtype handling.

---

### Bad vs good

❌ "Built a machine learning model to predict customer churn using Python."
✅ "Built a churn classifier on 240k accounts (LightGBM, 1:40 class imbalance) with isotonic calibration and cost-sensitive thresholding, lifting precision@10% from 0.31 to 0.58 over the business's existing rules baseline."

The second one is answerable in an interview. The first invites the question you can't answer.
