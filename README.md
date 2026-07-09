# OrientExpress

This repository holds three versions of the Data Science course project (a query engine over DOAJ journal data and Scimago category/area data, combining a graph database and a relational database).

## Structure

- **`OrientExpressLegacy/`** — the original submission, kept untouched as a reference point.
- **`OrientExpressAtakanFixed/`** — Atakan's submission.
- **`OrientExpressCeydaFixed/`** — Ceyda's submission.

## What changed

Upon revision of the professor's feedback, every reported issue was fixed, the legacy version was kept as-is for reference, and both Atakan's and Ceyda's code were updated with the new functions requested:

**Fixes applied to both `AtakanFixed` and `CeydaFixed`:**
- `getPublisher()` now returns a plain string instead of an undocumented `Publisher` object
- journal identifiers now capture both ISSN and EISSN
- `getJournalsWithLicense` no longer returns zero results, and correctly matches multi-valued license fields
- category listing methods (`getAllCategories`, `getCategoriesWithQuartile`, `getCategoriesAssignedToAreas`) no longer return duplicates
- mashup queries (`getJournalsInCategoriesWithQuartile`, `getJournalsInAreasWithLicense`, `getDiamondJournalsInAreasAndCategoriesWithQuartile`) no longer over-count journals
- `getEntityById` now correctly returns `Area` objects (previously only `Journal`/`Category` were reachable) and populates a journal's `getCategories()` as expected

**New functions:**
- `AtakanFixed`: `getJournalNotPublishedBy`, `getJournalWithCategories`, `getMultiCategoriesJournalsAvoidingPuglishers`
- `CeydaFixed`: `getAreasByName`, `getJournalsWithSealAndNoAPC`, `getDiamondJournalsInAreas`

## Tested and confirmed

Both fixed versions were run end-to-end against a live Blazegraph instance with the full exemplar dataset, and pass the official course test suite (`test.py`) in full: 5/5 tests, `OK`.
