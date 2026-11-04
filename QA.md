# Questions and Answers

Use this file to record project questions, confirmed answers, and supporting
references. Append new entries with a unique question number and a date in
`YYYY-MM-DD` format. Mark unanswered questions as **Pending** rather than guessing.
Update an entry and its date when its answer changes.

## Q1: Where does the Mastercard stock dataset come from?

**Date:** 2026-11-04

**Answer:** The dataset is sourced from Kaggle:
[MasterCard Stock Data - Latest and Updated](https://www.kaggle.com/datasets/kalilurrahman/mastercard-stock-data-latest-and-updated).
The local input file is `data/Mastercard_stock.csv`.

## Q2: What does each row represent?

**Date:** 2026-11-04

**Answer:** Each row represents Mastercard stock trading activity for a specific
trading day.

## Q3: Which columns are available in the local CSV?

**Date:** 2026-11-04

**Answer:** The local CSV contains `Date`, `Open`, `High`, `Low`, `Close`,
`Volume`, `Dividends`, and `Stock Splits`. Column definitions and handling
guidance are documented in [AGENTS.md](AGENTS.md#dataset-schema).

## Q4: Should the data folder be committed to Git?

**Date:** 2026-11-04

**Answer:** No. The root `data/` folder is excluded by the `/data/` rule in
[.gitignore](.gitignore). Keep source downloads and generated data out of
version control.

## Q5: Which dataset is used in the assignment?

**Date:** 2026-11-04

**Answer:** The assignment uses the **MasterCard Stock Data - Latest and Updated**
dataset from Kaggle, available at
[this dataset page](https://www.kaggle.com/datasets/kalilurrahman/mastercard-stock-data-latest-and-updated).
The local dataset file is `data/Mastercard_stock.csv`.

## Q6: Which file format does the dataset use?

**Date:** 2026-11-04

**Answer:** The dataset uses **CSV (Comma-Separated Values)** format. It is stored
as `Mastercard_stock.csv`, with a header row containing the column names and
subsequent rows containing comma-separated trading records.

## Q7: What information does the dataset contain?

**Date:** 2026-11-04

**Answer:** The dataset contains historical daily trading information for
Mastercard stock. Each row represents one trading day and includes:

- **Date:** The trading date for the record.
- **Open:** The stock's opening price for that day.
- **High:** The highest stock price reached during the day.
- **Low:** The lowest stock price reached during the day.
- **Close:** The stock's final trading price at the end of the day.
- **Volume:** The total number of shares traded during the day.

The local CSV also includes `Dividends` and `Stock Splits` fields.

## Template for new entries

```markdown
## Q<number>: <Question>

**Date:** YYYY-MM-DD

**Answer:** <Confirmed answer, or Pending>

**Reference:** <Relevant file, link, or decision, if available>
```
