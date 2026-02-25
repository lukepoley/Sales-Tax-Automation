# Sales Tax Automation & Audit Preparation Tool

## Project Overview

This project is a high-performance Python-based automation tool designed to streamline the extraction, filtering, and reporting of Joint Interest Billing (JIB) data for tax compliance. It automates the identification of high-value transactions and maps them to digital invoice repositories, significantly reducing the manual effort required for quarterly sales tax audits.

### Key Features

* **Batch Processing:** Processes multiple months of financial data in a single run, generating separate, standardized `.xlsx` reports for each period.
* **Embedded SQL Logic:** Utilizes an in-memory SQLite database to perform complex window functions (`LAG`, `DENSE_RANK`, `PARTITION BY`) for data deduplication and sequence numbering.
* **Dynamic Hyperlinking:** Automatically generates platform-aware hyperlinks (Dropbox and Local F: Drive) for instantaneous access to digital invoice images based on vendor and invoice metadata.
* **Intelligent Data Loading:** Features a "smart load" algorithm that scans raw Excel files to find header rows, allowing for inconsistent source file formatting.
* **Cross-Platform Compatibility:** Designed to function seamlessly across Windows and Linux environments.

---

## Technical Stack

* **Language:** Python 3, Rust
* **Data Manipulation:** Pandas, Polars
* **Database Engine:** SQLite (SQLAlchemy/sqlite3) for Python
* **Excel Engine:** XlsxWriter 

---

## How to Use

1. **Install EXE:**
- Go To The Releases Tab
- Download and Run .exe
* Path to the Invoice Reference/Cross-walk file.
* Target folder for the generated reports.
