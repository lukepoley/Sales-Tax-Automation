import pandas as pd
import os
import platform
import subprocess
import glob
import re
from datetime import datetime

def clean_path(path):
    """Removes quotes and extra whitespace from user input paths."""
    if not path: return ""
    return path.strip().replace('"', '').replace("'", "")

def open_folder(path):
    """Cross-platform 'startfile' equivalent."""
    if platform.system() == "Windows":
        os.startfile(path)
    elif platform.system() == "Darwin":  # macOS
        subprocess.Popen(["open", path])
    else:  # Linux
        subprocess.Popen(["xdg-open", path])

def parse_months(month_input):
    """Parses inputs like '1-3' or '1, 2, 6' into a list of integers."""
    months = []
    try:
        month_input = str(month_input).strip()
        if '-' in month_input:
            start, end = map(int, month_input.split('-'))
            months = list(range(start, end + 1))
        elif ',' in month_input:
            months = [int(m.strip()) for m in month_input.split(',')]
        else:
            months = [int(month_input)]
    except ValueError:
        print(f"Invalid month input: {month_input}. Defaulting to month 1.")
        months = [1]
    return sorted([m for m in months if 1 <= m <= 12])

def load_data(path, keyword=None):
    """Loads CSV or Excel data smartly."""
    print(f"Loading {path}...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    
    if ext == '.csv':
        # Try reading with different encodings if default fails
        try:
            df = pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(path, encoding='latin1', low_memory=False)
    elif ext in ['.xlsx', '.xls', '.xlsm']:
        xl = pd.ExcelFile(path)
        target_sheet = xl.sheet_names[0]
        if keyword:
            for sheet in xl.sheet_names:
                if keyword.lower() in sheet.lower():
                    target_sheet = sheet
                    break
        
        # Read header logic similar to existing script to find the real header
        raw_df = pd.read_excel(path, sheet_name=target_sheet, header=None, nrows=50)
        start_row = 0
        keywords = {'owner', 'txn gross amt', 'vendor', 'invoice #', 'invoice no'}
        for r_idx, row in raw_df.iterrows():
            row_vals = [str(val).strip().lower() for val in row]
            if any(k in row_vals for k in keywords):
                start_row = r_idx
                break
        
        df = pd.read_excel(path, sheet_name=target_sheet, skiprows=start_row)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")
    
    # Normalize columns
    df.columns = [str(c).strip().lower().replace(' ', '_').replace('#', 'no').replace('.', '_') for c in df.columns]
    return df

def process_sales_tax(months, year, jib_path, ir_path, output_dir):
    
    # 1. Load Data
    try:
        df_jib = load_data(jib_path, keyword="JIB")
        df_ir = load_data(ir_path, keyword="Combined")
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 2. Pre-process JIB Data
    # Convert date column to datetime objects
    date_col = next((c for c in df_jib.columns if 'inv_date' in c), 'txn_inv_date')
    if date_col not in df_jib.columns:
         print(f"Warning: Could not find Transaction Invoice Date column. Available: {df_jib.columns}")
         # Attempt to find any date column
         date_col = 'txn_inv_date' 
    
    # Ensure date column exists before processing
    if date_col in df_jib.columns:
        df_jib[date_col] = pd.to_datetime(df_jib[date_col], errors='coerce')
    
    # Clean Numeric Columns
    gross_col = next((c for c in df_jib.columns if 'gross_amt' in c), 'txn_gross_amt')
    if gross_col in df_jib.columns:
        df_jib = df_jib.dropna(subset=[gross_col]) # Drop rows where gross amount is NaN
    
    # Function to clean currency strings
    def clean_currency(x):
        if isinstance(x, str):
            x = x.replace('(', '-').replace(')', '').replace(',', '').replace('$', '')
        return pd.to_numeric(x, errors='coerce')

    if gross_col in df_jib.columns:
        df_jib[gross_col] = df_jib[gross_col].apply(clean_currency)

    # Clean Invoice Number
    inv_col_jib = next((c for c in df_jib.columns if 'invoice_no' in c), 'txn_invoice_no')
    if inv_col_jib in df_jib.columns:
        df_jib[inv_col_jib] = df_jib[inv_col_jib].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    # 3. Pre-process Invoice Reference Data
    inv_col_ir = next((c for c in df_ir.columns if 'invoice_no' in c), 'invoice_no')
    if inv_col_ir in df_ir.columns:
        df_ir[inv_col_ir] = df_ir[inv_col_ir].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    # Aggregate IR data to handle duplicate invoice numbers (take first non-null image)
    # Assuming columns like related_file_001, etc.
    img_cols = [c for c in df_ir.columns if 'related_file' in c]
    if inv_col_ir in df_ir.columns:
        df_ir_grouped = df_ir.groupby(inv_col_ir)[img_cols].first().reset_index()
    else:
        print("Warning: Invoice Number column not found in Invoice Reference file.")
        df_ir_grouped = pd.DataFrame()


    for month in months:
        print(f"Processing Month: {month}, Year: {year}")
        
        # 4. Filter by Date (Draft 1-1)
        if date_col in df_jib.columns:
            mask = (df_jib[date_col].dt.month == month) & (df_jib[date_col].dt.year == year)
            df_month = df_jib[mask].copy()
        else:
            print("Skipping date filtering due to missing column.")
            df_month = df_jib.copy()
        
        if df_month.empty:
            print(f"No data found for Month {month}, skipping.")
            continue

        # 5. Sorting (Draft 1-2)
        # Sort Order: Vendor -> Invoice -> Property -> Billing Cat -> Gross Amt (Desc)
        possible_cols = {
            'vendor': ['name_1', 'vendor_name', 'vendor'],
            'invoice': [inv_col_jib],
            'property': ['property', 'prop'],
            'billing': ['billing_cat', 'bill_cat'],
            'gross': [gross_col]
        }
        
        # Helper to find actual column names
        actual_sort_cols = []
        ascending_flags = []
        
        for key in ['vendor', 'invoice', 'property', 'billing']:
            found = False
            for candidate in possible_cols[key]:
                if candidate in df_month.columns:
                    actual_sort_cols.append(candidate)
                    ascending_flags.append(True) # A-Z
                    found = True
                    break
            if not found and key != 'vendor': # Vendor might be complex
                print(f"Warning: Could not find sort column for {key}")

        # Gross Amount Descending
        if gross_col in df_month.columns:
            actual_sort_cols.append(gross_col)
            ascending_flags.append(False) # Largest to Smallest
        
        if actual_sort_cols:
            df_month = df_month.sort_values(by=actual_sort_cols, ascending=ascending_flags)

        # 6. Calculations (Draft 1-2 & 1-3)
        # "Gross Amount of 100% of Invoice"
        # We need invoice column and gross column
        if inv_col_jib in df_month.columns and gross_col in df_month.columns:
            df_month['Inv_Total'] = df_month.groupby(inv_col_jib)[gross_col].transform('sum')
            
            # Filter: Inv_Total >= 2000 OR Inv_Total <= -2000
            df_filtered = df_month[(df_month['Inv_Total'] >= 2000) | (df_month['Inv_Total'] <= -2000)].copy()
        else:
            print("Skipping aggregation/filtering due to missing columns.")
            df_filtered = df_month.copy()
        
        # Filter out GJ and PE (Draft 1-5-0)
        if inv_col_jib in df_filtered.columns:
            df_filtered = df_filtered[~df_filtered[inv_col_jib].str.upper().str.startswith(('GJ', 'PE'), na=False)]

        # Filter out specific vendors if Total < 3500 (Draft 1-5-1)
        excluded_vendors = [
            "J R CONSTRUCTION", "MONTEZUMA WELL SERVICE", "MARYBOY", 
            "NELSON'S WELDING & ROUSTABOUT", "3G CONSULTING"
        ]
        vendor_col = next((c for c in df_filtered.columns if c in ['name_1', 'vendor_name', 'vendor']), None)
        
        if vendor_col and 'Inv_Total' in df_filtered.columns:
            # Create mask: (Abs(Total) < 3500) AND (Vendor in Excluded)
            mask_vendor = (df_filtered['Inv_Total'].abs() < 3500) & \
                          (df_filtered[vendor_col].str.upper().apply(lambda x: any(v in str(x).upper() for v in excluded_vendors)))
            df_filtered = df_filtered[~mask_vendor]

        if df_filtered.empty:
            print(f"No transactions met the $2000 threshold for Month {month}.")
            continue

        # Re-sort by Inv_Total (Desc), then Vendor, etc. (Draft 1-3)
        # We want [Inv_Total, Vendor, Invoice, Prop, Bill, Gross]
        
        final_sort_order = ['Inv_Total'] + actual_sort_cols
        final_asc_order = [False] + ascending_flags
        
        # Add Inv_Total to sort if it exists
        if 'Inv_Total' in df_filtered.columns:
            df_filtered = df_filtered.sort_values(by=final_sort_order, ascending=final_asc_order)

        # Determine "Gross amount of first occurrence" logic (Visual mostly, strictly it's the top row)
        # We'll handle this by flagging the first row of each invoice
        if inv_col_jib in df_filtered.columns:
            df_filtered['is_first_row'] = ~df_filtered.duplicated(subset=[inv_col_jib], keep='first')
        else:
            df_filtered['is_first_row'] = True
        
        # 7. Merge Images (Draft 1-3)
        # Join df_filtered with df_ir_grouped
        if inv_col_jib in df_filtered.columns and inv_col_ir in df_ir_grouped.columns:
             df_merged = pd.merge(df_filtered, df_ir_grouped, left_on=inv_col_jib, right_on=inv_col_ir, how='left')
        else:
             df_merged = df_filtered.copy()
        
        # 8. Add Hyperlink Columns (Draft 1-4)
        # Logic for Quarter
        # Month 1-3 = Q1, 4-6 = Q2, 7-9 = Q3, 10-12 = Q4
        q_map = {1:1, 2:1, 3:1, 4:2, 5:2, 6:2, 7:3, 8:3, 9:3, 10:4, 11:4, 12:4}
        current_q = q_map.get(month, 1)
        next_q = current_q + 1 if current_q < 4 else 1
        next_q_year = year if current_q < 4 else year + 1
        
        # Placeholder for user name - user might need to change this, but we'll use 'brend' as per procedure default 1-5-0
        user_name = "brend"
        base_dropbox = f"C:\\Users\\{user_name}\\Dropbox\\Images MP-BC-AP R4Q2"
        base_f_drive = "F:\\Images MP-BC-AP R4Q2"
        
        path_current_q = f"{base_dropbox}\\{year} Q{current_q} Invoices\\"
        path_next_q = f"{base_dropbox}\\{next_q_year} Q{next_q} Invoices\\"
        
        f_path_current_q = f"{base_f_drive}\\{year} Q{current_q} Invoices\\"
        f_path_next_q = f"{base_f_drive}\\{next_q_year} Q{next_q} Invoices\\"
        
        def create_hyperlink_formula(row, img_col, path):
            if not row.get('is_first_row', False):
                return 0
            img_val = row.get(img_col)
            if pd.isna(img_val) or str(img_val).strip() == '':
                return 0
            # Excel Formula: =HYPERLINK("path" & cell_ref, "Link") or just path
            full_path = path + str(img_val)
            # Escape double quotes if any in filename (unlikely but safe)
            # full_path = full_path.replace('"', '""') 
            return f'=HYPERLINK("{full_path}", "{str(img_val)}")'

        # Create columns for up to 4 images
        for i in range(1, 5):
            col_name = f'related_file_{i:03d}'
            if col_name not in df_merged.columns:
                continue
            
            # Use 'apply' cautiously with axis=1 for large DFs, but should be fine here
            
            df_merged[f'Dropbox Link Image {i} Q{current_q}'] = df_merged.apply(
                lambda r: create_hyperlink_formula(r, col_name, path_current_q), axis=1)
            
            df_merged[f'Dropbox Link Image {i} Q{next_q}'] = df_merged.apply(
                lambda r: create_hyperlink_formula(r, col_name, path_next_q), axis=1)
                
            # F Drive Links
            df_merged[f'F Drive Link Image {i} Q{current_q}'] = df_merged.apply(
                lambda r: create_hyperlink_formula(r, col_name, f_path_current_q), axis=1)
            
            df_merged[f'F Drive Link Image {i} Q{next_q}'] = df_merged.apply(
                lambda r: create_hyperlink_formula(r, col_name, f_path_next_q), axis=1)

        # 9. Sequence Numbers (Draft 1-4)
        # Reset index to ensure clean iteration match
        df_merged = df_merged.reset_index(drop=True)
        
        # Generate Sequence ID
        if inv_col_jib in df_merged.columns:
            df_merged['For_Seq_No'] = (df_merged[inv_col_jib] != df_merged[inv_col_jib].shift()).cumsum()
        else:
            df_merged['For_Seq_No'] = range(1, len(df_merged) + 1)

        # Generate Padded Seq No
        df_merged['Seq_No_Pad'] = df_merged['For_Seq_No'].apply(lambda x: f"{x:03d}")
        
        # Generate Filename of Image for UT Tax Comm
        target_filename = f"S{year}{month:02d}-" + df_merged['Seq_No_Pad'] + ".pdf"
        df_merged['Tax_Comm_Image'] = df_merged['is_first_row'].map({True: 1, False: 0}) * target_filename
        df_merged.loc[~df_merged['is_first_row'], 'Tax_Comm_Image'] = 0 # Ensure 0 for non-first

        # 10. Final Column Selection & Formatting
        # Select columns to keep
        # We need: For Seq #, Seq #, [Original JIB Cols], [Links], [Tax Comm Filename], [Empty Tax Cols]
        
        out_cols = ['For_Seq_No', 'Seq_No_Pad']
        # Add original columns from month (except temporary ones)
        # Use actual_sort_cols and others, or just all from df_month
        # Let's keep all original columns from JIB
        original_cols = [c for c in df_jib.columns if c in df_merged.columns]
        
        # Add generated columns
        link_cols = [c for c in df_merged.columns if 'Link Image' in c]
        
        final_cols_ordered = out_cols + original_cols + link_cols + ['Tax_Comm_Image']
        
        # Add the empty tax columns requested in Draft 1-5-0
        empty_tax_cols = [
            "UT + SJ Combined Sales Tax", "Utah State Sales Tax", "San Juan County Sales Tax",
            "Other local Utah tax", "Other entity collecting tax", "Sum of UT Tx Excl Chrgd by N.N.",
            "NNOGC Entity Tx Pd Amt", "Poley Team Notes"
        ]
        for c in empty_tax_cols:
            df_merged[c] = ""
            final_cols_ordered.append(c)

        # Ensure we don't have duplicates or missing columns in the projection
        final_cols_ordered = []
        seen = set()
        for c in (out_cols + original_cols + link_cols + ['Tax_Comm_Image'] + empty_tax_cols):
            if c in df_merged.columns and c not in seen:
                final_cols_ordered.append(c)
                seen.add(c)
        
        # Create Final DF
        df_final = df_merged[final_cols_ordered].copy()
        
        # Rename Headers
        rename_map = {
            'For_Seq_No': 'For Sequence #',
            'Seq_No_Pad': 'Sequence #',
            'Tax_Comm_Image': 'Filename of Image for the UT Tax Comm.',
            inv_col_jib: 'Txn Invoice No',
            gross_col: 'Txn Gross Amt'
        }
        df_final = df_final.rename(columns=rename_map)

        # 11. Format Column Headers (User Request)
        # remove underscores, Capitalize Words except "the", "for", "by"
        def format_header(col_name):
            # Replace underscores with spaces
            s = str(col_name).replace('_', ' ')
            # Split into words
            words = s.split()
            # Capitalize logic
            stopwords = {'the', 'for', 'by', 'of', 'and', 'to', 'in', 'on', 'at'} # added a few common ones just in case, but user specified "the, for, by"
            # Strict user request: "except for 'the', 'for', and 'by'"
            stopwords = {'the', 'for', 'by'} 
            
            res_words = []
            for i, w in enumerate(words):
                # Always capitalize first and last word? User didn't specify, but standard Title Case does.
                # "All words there should be capitalized except..." implies internal words.
                if i == 0 or w.lower() not in stopwords:
                    # capitalize() lowers the rest, title() might be too aggressive with apostrophes?
                    # simple .capitalize() is usually safe for single words
                    res_words.append(w.capitalize())
                else:
                    res_words.append(w.lower())
            
            return " ".join(res_words)

        df_final.columns = [format_header(c) for c in df_final.columns]

        # Write to Excel
        out_filename = f"{year} {month:02d} Sales Tax - NNOGC PY d1-4.xlsx"
        out_path = os.path.join(output_dir, out_filename)
        
        print(f"Writing to {out_path}...")
        
        with pd.ExcelWriter(out_path, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, sheet_name='Sales Tax Report', index=False)
            workbook = writer.book
            worksheet = writer.sheets['Sales Tax Report']
            
            # Formats
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
            
            # Apply Header Format and Auto-width
            for i, col in enumerate(df_final.columns):
                worksheet.write(0, i, col, header_fmt)
                # Estimate width
                max_len = len(str(col)) + 2
                worksheet.set_column(i, i, min(max_len + 5, 50))
            
            worksheet.autofilter(0, 0, 0, len(df_final.columns) - 1)
        
        print(f"Finished Month {month}!")

if __name__ == "__main__":
    # Default paths for Windows users (User Request)
    # Using raw strings for Windows paths example
    default_jib = r"C:\Users\User\Documents\jib.xlsx"
    default_ir = r"C:\Users\User\Documents\Invoicereferencecomibed.xlsx"
    default_out = r"C:\Users\User\Documents\output"
    
    # If running on Linux (dev environment), fallback to local for convenience if they exist
    if platform.system() == "Linux" and os.path.exists("/home/lpoley"):
         default_jib = "/home/lpoley/Documents/Accounting/SQL project/antigravity/jib.csv"
         default_ir = "/home/lpoley/Documents/Accounting/SQL project/antigravity/Invoicereferencecomibed.csv"
         default_out = "/home/lpoley/Documents/Accounting/SQL project/antigravity/output"

    print("--- Sales Tax Refund Generator ---")
    
    try:
        month_in = input("Enter Month(s) (e.g., '1-3'): ")
        year_in = input("Enter Year (e.g., 2023): ")
        
        # Prompt with defaults
        val = input(f"Enter JIB Path: ")
        jib_in =  clean_path(val) if val.strip() else default_jib
        
        val = input(f"Enter Invoice Ref Path: ")
        ir_in = clean_path(val) if val.strip() else default_ir
        
        val = input(f"Enter Output Path: ")
        out_in = clean_path(val) if val.strip() else default_out
        
        process_sales_tax(
            parse_months(month_in), 
            int(year_in), 
            jib_in, 
            ir_in, 
            out_in
        )
    except KeyboardInterrupt:
        print("\nAborted.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
