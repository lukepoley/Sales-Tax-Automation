import pandas as pd
import os
import sys
import platform
import subprocess
import traceback
import logging
from datetime import datetime

# ---------------------------------------------------------------------------
# Logging – always write a log file next to the EXE / script so errors are
# never lost when the console window closes on Windows.
# ---------------------------------------------------------------------------
def _setup_logging():
    # Resolve a writable directory: same folder as the EXE (frozen) or script
    if getattr(sys, 'frozen', False):
        base_dir = os.path.dirname(sys.executable)
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))

    log_path = os.path.join(base_dir, "SalesTaxGenerator.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info(f"Log file: {log_path}")
    return log_path

LOG_PATH = _setup_logging()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def clean_path(path: str) -> str:
    """Remove surrounding quotes / whitespace from a path the user typed."""
    if not path:
        return ""
    return path.strip().strip('"').strip("'")


def open_folder(path: str):
    """Cross-platform folder-open helper."""
    try:
        if platform.system() == "Windows":
            os.startfile(path)
        elif platform.system() == "Darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception as exc:
        logging.warning(f"Could not open folder '{path}': {exc}")


def parse_months(month_input: str) -> list:
    """Parse '1-3' or '1, 2, 6' or '4' into a sorted list of integers."""
    months = []
    try:
        month_input = str(month_input).strip()
        if '-' in month_input:
            parts = month_input.split('-')
            start, end = int(parts[0]), int(parts[1])
            months = list(range(start, end + 1))
        elif ',' in month_input:
            months = [int(m.strip()) for m in month_input.split(',')]
        else:
            months = [int(month_input)]
    except ValueError:
        logging.warning(f"Invalid month input '{month_input}'. Defaulting to month 1.")
        months = [1]
    return sorted(m for m in months if 1 <= m <= 12)


def load_data(path: str, keyword: str = None) -> pd.DataFrame:
    """Load CSV or Excel, return a DataFrame with normalised column names."""
    logging.info(f"Loading: {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()

    if ext == '.csv':
        try:
            df = pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin1', low_memory=False)

    elif ext in ('.xlsx', '.xls', '.xlsm'):
        xl = pd.ExcelFile(path)
        target_sheet = xl.sheet_names[0]
        if keyword:
            for sheet in xl.sheet_names:
                if keyword.lower() in sheet.lower():
                    target_sheet = sheet
                    break

        # Smart-scan first 50 rows to find the real header row
        raw = pd.read_excel(path, sheet_name=target_sheet, header=None, nrows=50)
        start_row = 0
        header_keywords = {'owner', 'txn gross amt', 'vendor', 'invoice #', 'invoice no'}
        for r_idx, row in raw.iterrows():
            row_vals = {str(v).strip().lower() for v in row}
            if row_vals & header_keywords:
                start_row = r_idx
                break

        df = pd.read_excel(path, sheet_name=target_sheet, skiprows=start_row)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    # Normalise column names to snake_case
    df.columns = [
        str(c).strip().lower()
          .replace(' ', '_')
          .replace('#', 'no')
          .replace('.', '_')
        for c in df.columns
    ]
    return df


# ---------------------------------------------------------------------------
# Core processing
# ---------------------------------------------------------------------------

def process_sales_tax(months, year, jib_path, ir_path, output_dir):

    # 1. Load data
    try:
        df_jib = load_data(jib_path, keyword="JIB")
        df_ir  = load_data(ir_path,  keyword="Combined")
    except Exception as exc:
        logging.error(f"Error loading data: {exc}")
        return

    os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # 2. Pre-process JIB
    # ------------------------------------------------------------------
    date_col  = next((c for c in df_jib.columns if 'inv_date' in c), 'txn_inv_date')
    gross_col = next((c for c in df_jib.columns if 'gross_amt' in c), 'txn_gross_amt')
    inv_col_jib = next((c for c in df_jib.columns if 'invoice_no' in c), 'txn_invoice_no')

    if date_col in df_jib.columns:
        df_jib[date_col] = pd.to_datetime(df_jib[date_col], errors='coerce')
    else:
        logging.warning(f"Date column '{date_col}' not found. Available: {list(df_jib.columns)}")

    def clean_currency(x):
        if isinstance(x, str):
            x = x.replace('(', '-').replace(')', '').replace(',', '').replace('$', '')
        return pd.to_numeric(x, errors='coerce')

    if gross_col in df_jib.columns:
        df_jib[gross_col] = df_jib[gross_col].apply(clean_currency)
        df_jib = df_jib.dropna(subset=[gross_col])

    if inv_col_jib in df_jib.columns:
        df_jib[inv_col_jib] = (
            df_jib[inv_col_jib].astype(str).str.strip()
            .str.replace(r'\.0$', '', regex=True)
        )

    # ------------------------------------------------------------------
    # 3. Pre-process Invoice Reference
    # ------------------------------------------------------------------
    inv_col_ir = next((c for c in df_ir.columns if 'invoice_no' in c), 'invoice_no')
    img_cols   = [c for c in df_ir.columns if 'related_file' in c]

    if inv_col_ir in df_ir.columns:
        df_ir[inv_col_ir] = (
            df_ir[inv_col_ir].astype(str).str.strip()
            .str.replace(r'\.0$', '', regex=True)
        )
        df_ir_grouped = df_ir.groupby(inv_col_ir)[img_cols].first().reset_index()
    else:
        logging.warning("Invoice Number column not found in Invoice Reference file.")
        df_ir_grouped = pd.DataFrame()

    # ------------------------------------------------------------------
    # 4. Per-month loop
    # ------------------------------------------------------------------
    q_map = {1:1,2:1,3:1, 4:2,5:2,6:2, 7:3,8:3,9:3, 10:4,11:4,12:4}

    for month in months:
        logging.info(f"--- Processing Month {month}, Year {year} ---")

        # Filter by date
        if date_col in df_jib.columns:
            mask = (df_jib[date_col].dt.month == month) & (df_jib[date_col].dt.year == year)
            df_month = df_jib[mask].copy()
        else:
            df_month = df_jib.copy()

        if df_month.empty:
            logging.info(f"No data found for Month {month}; skipping.")
            continue

        # Sort: Vendor → Invoice → Property → Billing → Gross (desc)
        possible_cols = {
            'vendor':  ['name_1', 'vendor_name', 'vendor'],
            'invoice': [inv_col_jib],
            'property':['property', 'prop'],
            'billing': ['billing_cat', 'bill_cat'],
        }
        sort_cols  = []
        sort_asc   = []
        for key, candidates in possible_cols.items():
            for c in candidates:
                if c in df_month.columns:
                    sort_cols.append(c)
                    sort_asc.append(True)
                    break

        if gross_col in df_month.columns:
            sort_cols.append(gross_col)
            sort_asc.append(False)

        if sort_cols:
            df_month = df_month.sort_values(by=sort_cols, ascending=sort_asc)

        # Aggregate invoice totals and filter ≥ $2,000
        if inv_col_jib in df_month.columns and gross_col in df_month.columns:
            df_month['Inv_Total'] = df_month.groupby(inv_col_jib)[gross_col].transform('sum')
            df_filtered = df_month[
                (df_month['Inv_Total'] >= 2000) | (df_month['Inv_Total'] <= -2000)
            ].copy()
        else:
            df_filtered = df_month.copy()

        # Drop GJ / PE invoice types
        if inv_col_jib in df_filtered.columns:
            df_filtered = df_filtered[
                ~df_filtered[inv_col_jib].str.upper().str.startswith(('GJ', 'PE'), na=False)
            ]

        # Drop specific low-value vendors
        excluded_vendors = [
            "J R CONSTRUCTION", "MONTEZUMA WELL SERVICE", "MARYBOY",
            "NELSON'S WELDING & ROUSTABOUT", "3G CONSULTING"
        ]
        vendor_col = next(
            (c for c in df_filtered.columns if c in ('name_1', 'vendor_name', 'vendor')), None
        )
        if vendor_col and 'Inv_Total' in df_filtered.columns:
            mask_excl = (
                (df_filtered['Inv_Total'].abs() < 3500) &
                df_filtered[vendor_col].astype(str).str.upper()
                    .apply(lambda x: any(v in x for v in excluded_vendors))
            )
            df_filtered = df_filtered[~mask_excl]

        if df_filtered.empty:
            logging.info(f"No transactions met the $2,000 threshold for Month {month}.")
            continue

        # Re-sort: Inv_Total desc first
        final_sort  = ['Inv_Total'] + sort_cols
        final_asc   = [False]       + sort_asc
        if 'Inv_Total' in df_filtered.columns:
            df_filtered = df_filtered.sort_values(by=final_sort, ascending=final_asc)

        # Flag first occurrence of each invoice
        if inv_col_jib in df_filtered.columns:
            df_filtered['is_first_row'] = ~df_filtered.duplicated(subset=[inv_col_jib], keep='first')
        else:
            df_filtered['is_first_row'] = True

        # Merge image references
        if (not df_ir_grouped.empty
                and inv_col_jib in df_filtered.columns
                and inv_col_ir in df_ir_grouped.columns):
            df_merged = pd.merge(
                df_filtered, df_ir_grouped,
                left_on=inv_col_jib, right_on=inv_col_ir, how='left'
            )
        else:
            df_merged = df_filtered.copy()

        df_merged = df_merged.reset_index(drop=True)

        # ------------------------------------------------------------------
        # 5. Hyperlink columns
        # ------------------------------------------------------------------
        current_q  = q_map.get(month, 1)
        next_q     = (current_q % 4) + 1
        next_q_year = year if current_q < 4 else year + 1

        user_name     = "brend"
        base_dropbox  = f"C:\\Users\\{user_name}\\Dropbox\\Images MP-BC-AP R4Q2"
        base_f_drive  = "F:\\Images MP-BC-AP R4Q2"

        paths = {
            f"Dropbox Link Image {{i}} Q{current_q}":   f"{base_dropbox}\\{year} Q{current_q} Invoices\\",
            f"Dropbox Link Image {{i}} Q{next_q}":      f"{base_dropbox}\\{next_q_year} Q{next_q} Invoices\\",
            f"F Drive Link Image {{i}} Q{current_q}":   f"{base_f_drive}\\{year} Q{current_q} Invoices\\",
            f"F Drive Link Image {{i}} Q{next_q}":      f"{base_f_drive}\\{next_q_year} Q{next_q} Invoices\\",
        }

        def make_hyperlink(row, img_col, base_path):
            if not row.get('is_first_row', False):
                return 0
            img_val = row.get(img_col)
            if pd.isna(img_val) or str(img_val).strip() == '':
                return 0
            full_path = base_path + str(img_val).strip()
            return f'=HYPERLINK("{full_path}", "{str(img_val).strip()}")'

        existing_img_cols = [c for c in df_merged.columns if 'related_file' in c]
        for idx, col_name in enumerate(existing_img_cols[:4], start=1):
            for label_tmpl, base_path in paths.items():
                label = label_tmpl.replace("{i}", str(idx))
                df_merged[label] = df_merged.apply(
                    lambda r, c=col_name, p=base_path: make_hyperlink(r, c, p), axis=1
                )

        # ------------------------------------------------------------------
        # 6. Sequence numbers
        # ------------------------------------------------------------------
        if inv_col_jib in df_merged.columns:
            df_merged['For_Seq_No'] = (
                (df_merged[inv_col_jib] != df_merged[inv_col_jib].shift()).cumsum()
            )
        else:
            df_merged['For_Seq_No'] = range(1, len(df_merged) + 1)

        df_merged['Seq_No_Pad'] = df_merged['For_Seq_No'].apply(lambda x: f"{int(x):03d}")

        # ------------------------------------------------------------------
        # FIX: Build Tax_Comm_Image without fragile Series multiplication
        # ------------------------------------------------------------------
        def build_tax_comm_filename(row):
            """Return the PDF filename only for the first row of each invoice."""
            if not row.get('is_first_row', False):
                return 0
            seq_pad = row['Seq_No_Pad']
            return f"S{year}{month:02d}-{seq_pad}.pdf"

        df_merged['Tax_Comm_Image'] = df_merged.apply(build_tax_comm_filename, axis=1)

        # ------------------------------------------------------------------
        # 7. Assemble final columns
        # ------------------------------------------------------------------
        out_cols      = ['For_Seq_No', 'Seq_No_Pad']
        original_cols = [c for c in df_jib.columns if c in df_merged.columns]
        link_cols     = [c for c in df_merged.columns if 'Link Image' in c]

        empty_tax_cols = [
            "UT + SJ Combined Sales Tax", "Utah State Sales Tax",
            "San Juan County Sales Tax",  "Other local Utah tax",
            "Other entity collecting tax","Sum of UT Tx Excl Chrgd by N.N.",
            "NNOGC Entity Tx Pd Amt",     "Poley Team Notes",
        ]
        for c in empty_tax_cols:
            df_merged[c] = ""

        seen, final_cols_ordered = set(), []
        for c in (out_cols + original_cols + link_cols + ['Tax_Comm_Image'] + empty_tax_cols):
            if c in df_merged.columns and c not in seen:
                final_cols_ordered.append(c)
                seen.add(c)

        df_final = df_merged[final_cols_ordered].copy()

        # Rename key columns
        rename_map = {
            'For_Seq_No':   'For Sequence #',
            'Seq_No_Pad':   'Sequence #',
            'Tax_Comm_Image': 'Filename of Image for the UT Tax Comm.',
            inv_col_jib:    'Txn Invoice No',
            gross_col:      'Txn Gross Amt',
        }
        df_final.rename(columns=rename_map, inplace=True)

        # Title-case remaining snake_case headers
        stop = {'the', 'for', 'by'}
        def fmt_header(col):
            words = str(col).replace('_', ' ').split()
            return ' '.join(
                w.capitalize() if i == 0 or w.lower() not in stop else w.lower()
                for i, w in enumerate(words)
            )
        df_final.columns = [fmt_header(c) for c in df_final.columns]

        # ------------------------------------------------------------------
        # 8. Write Excel output
        # ------------------------------------------------------------------
        out_filename = f"{year} {month:02d} Sales Tax - NNOGC PY d1-4.xlsx"
        out_path     = os.path.join(output_dir, out_filename)
        logging.info(f"Writing → {out_path}")

        with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, sheet_name='Sales Tax Report', index=False)
            wb  = writer.book
            ws  = writer.sheets['Sales Tax Report']

            hdr_fmt = wb.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
            for i, col in enumerate(df_final.columns):
                ws.write(0, i, col, hdr_fmt)
                ws.set_column(i, i, min(len(str(col)) + 7, 50))

            ws.autofilter(0, 0, 0, len(df_final.columns) - 1)

        logging.info(f"✓ Month {month} complete → {out_path}")

    logging.info("All months processed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Resolve a sensible default output directory next to the EXE / script
    if getattr(sys, 'frozen', False):
        _exe_dir = os.path.dirname(sys.executable)
    else:
        _exe_dir = os.path.dirname(os.path.abspath(__file__))

    # Platform-aware defaults
    if platform.system() == "Linux" and os.path.exists("/home/lpoley"):
        _default_jib = "/home/lpoley/Documents/Accounting/SQL project/antigravity/jib.csv"
        _default_ir  = "/home/lpoley/Documents/Accounting/SQL project/antigravity/Invoicereferencecomibed.csv"
        _default_out = "/home/lpoley/Documents/Accounting/SQL project/antigravity/output"
    else:
        # On Windows use the folder where the EXE lives as the default base
        _default_jib = os.path.join(_exe_dir, "jib.xlsx")
        _default_ir  = os.path.join(_exe_dir, "Invoicereferencecomibed.xlsx")
        _default_out = os.path.join(_exe_dir, "output")

    print("=" * 50)
    print("  Sales Tax Refund Generator")
    print(f"  Log: {LOG_PATH}")
    print("=" * 50)

    try:
        month_in = input("Enter Month(s)  (e.g. '1-3' or '4'): ").strip()
        year_in  = input("Enter Year      (e.g. 2024): ").strip()

        val = input(f"JIB path        [{_default_jib}]: ").strip()
        jib_in = clean_path(val) if val else _default_jib

        val = input(f"Invoice Ref path [{_default_ir}]: ").strip()
        ir_in = clean_path(val) if val else _default_ir

        val = input(f"Output folder   [{_default_out}]: ").strip()
        out_in = clean_path(val) if val else _default_out

        process_sales_tax(
            parse_months(month_in),
            int(year_in),
            jib_in,
            ir_in,
            out_in,
        )

    except KeyboardInterrupt:
        logging.info("Aborted by user.")
    except Exception:
        # Always log the full traceback so it ends up in the log file
        logging.error("Unhandled exception:\n" + traceback.format_exc())
    finally:
        # ---------------------------------------------------------------
        # KEY FIX: pause before closing so the console window never just
        # disappears on Windows – whether the script succeeded or failed.
        # ---------------------------------------------------------------
        print()
        input("Press ENTER to close…")
