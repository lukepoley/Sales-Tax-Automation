import pandas as pd
import sqlite3
import os
import platform
import subprocess

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
        if '-' in month_input:
            start, end = map(int, month_input.split('-'))
            months = list(range(start, end + 1))
        elif ',' in month_input:
            months = [int(m.strip()) for m in month_input.split(',')]
        else:
            months = [int(month_input.strip())]
    except ValueError:
        print(f"Invalid month input: {month_input}. Defaulting to month 1.")
        months = [1]
    return [m for m in months if 1 <= m <= 12]

def get_quarter_info(month, year, offset=0):
    """Returns (Quarter String, Year) based on month and a quarter offset."""
    current_q = (month - 1) // 3 + 1
    target_q = current_q + offset
    target_year = year
    while target_q > 4:
        target_q -= 4
        target_year += 1
    while target_q < 1:
        target_q += 4
        target_year -= 1
    return f"Q{target_q}", target_year

def load_excel_smart(path, sheet_keyword=None):
    """Finds the header row and loads the Excel sheet."""
    xl = pd.ExcelFile(path)
    target_sheet = xl.sheet_names[0]
    if sheet_keyword:
        for sheet in xl.sheet_names:
            if sheet_keyword.lower() in sheet.lower():
                target_sheet = sheet
                break
    raw_df = pd.read_excel(path, sheet_name=target_sheet, header=None, nrows=50)
    start_row = 0
    keywords = {'owner', 'txn gross amt', 'vendor', 'invoice #'}
    for r_idx, row in raw_df.iterrows():
        row_vals = [str(val).strip().lower() for val in row]
        if any(k in row_vals for k in keywords):
            start_row = r_idx
            break
    return pd.read_excel(path, sheet_name=target_sheet, skiprows=start_row)

def run_processor():
    month_input = input("Enter Month(s) (e.g., '1-3' or '1,2,6'): ")
    months_to_process = parse_months(month_input)
    year = int(input("Enter Year: "))
    jib_path = clean_path(input("NNOG JIB Path: "))
    ir_path = clean_path(input("Invoice Reference Path: "))
    user_out = clean_path(input("Output Folder Path: "))

    if not os.path.exists(user_out): 
        os.makedirs(user_out)

    print("\n--- LOADING SOURCE DATA ---")
    df_jib_all = load_excel_smart(jib_path, sheet_keyword="JIB")
    df_elk = load_excel_smart(ir_path, sheet_keyword="Combined")

    df_jib_all.columns = [str(c).strip().lower().replace(' ', '_').replace('#', 'no').replace('.', '_') for c in df_jib_all.columns]
    df_elk.columns = [str(c).strip().lower().replace(' ', '_').replace('#', 'no').replace('.', '_') for c in df_elk.columns]

    for month in months_to_process:
        print(f"Processing Month {month}...")
        conn = sqlite3.connect(':memory:')
        df_jib_all.to_sql('jib_master', conn, index=False)
        df_elk.to_sql('elk', conn, index=False)

        q_str, q_year = get_quarter_info(month, year, offset=0)
        nq_str, nq_year = get_quarter_info(month, year, offset=1)

        sql_logic = f"""
        CREATE TABLE stage1 AS 
        SELECT *, CAST(NULLIF(REPLACE(REPLACE(REPLACE(txn_gross_amt, '(', '-'), ')', ''), ',', ''), '') AS NUMERIC) as clean_gross
        FROM jib_master WHERE CAST(strftime('%m', txn_inv_date) AS INT) = {month};

        CREATE TABLE stage2 AS
        SELECT *, ROW_NUMBER() OVER(PARTITION BY txn_invoice_no, name_1, property, billing_cat, clean_gross ORDER BY txn_invoice_no) as row_id FROM stage1;

        CREATE TABLE stage3 AS
        SELECT *, SUM(CASE WHEN row_id = 1 THEN clean_gross ELSE 0 END) OVER(PARTITION BY txn_invoice_no) as inv_total FROM stage2;

        CREATE TABLE stage4 AS
        WITH Filtered AS (
            SELECT * FROM stage3 WHERE inv_total >= 2000 OR inv_total <= -2000
        ),
        Ranked AS (
            SELECT f.*, 
                   l.related_file_001, l.related_file_002, l.related_file_003, l.related_file_004,
                   DENSE_RANK() OVER(ORDER BY name_1, txn_invoice_no) as for_sequence_no
            FROM Filtered f
            LEFT JOIN (SELECT invoice_no, related_file_001, related_file_002, related_file_003, related_file_004 FROM elk GROUP BY invoice_no) l 
            ON f.txn_invoice_no = l.invoice_no
        )
        SELECT 
            for_sequence_no AS "For Sequence #",
            substr('000' || for_sequence_no, -3) AS "Sequence #",
            *,
            -- This flag now triggers only on the FIRST row of a new Sequence number
            CASE WHEN LAG(for_sequence_no) OVER(ORDER BY name_1, txn_invoice_no) IS NOT for_sequence_no THEN 'CHANGE' ELSE 'SAME' END as sequence_change_flag,
            'S' || {year} || printf('%02d', {month}) || '-' || substr('000' || for_sequence_no, -3) || '.pdf' as raw_fn
        FROM Ranked;

        CREATE TABLE final_report AS
        SELECT 
            "For Sequence #",
            "Sequence #",
            *,
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_001 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_001 || '")' ELSE '' END AS "Dropbox Link to Image 1 for {q_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_001 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_001 || '")' ELSE '' END AS "Dropbox Link to Image 1 for {nq_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_002 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_002 || '")' ELSE '' END AS "Dropbox Link to Image 2 for {q_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_002 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_002 || '")' ELSE '' END AS "Dropbox Link to Image 2 for {nq_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_003 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_003 || '")' ELSE '' END AS "Dropbox Link to Image 3 for {q_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_003 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_003 || '")' ELSE '' END AS "Dropbox Link to Image 3 for {nq_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_004 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_004 || '")' ELSE '' END AS "Dropbox Link to Image 4 for {q_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_004 IS NOT NULL THEN '=HYPERLINK("C:\\Users\\brend\\Dropbox\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_004 || '")' ELSE '' END AS "Dropbox Link to Image 4 for {nq_str} for Brenda",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_001 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_001 || '")' ELSE '' END AS "F drive to Image 1 for {q_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_001 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_001 || '")' ELSE '' END AS "F drive to Image 1 for {nq_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_002 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_002 || '")' ELSE '' END AS "F drive to Image 2 for {q_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_002 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_002 || '")' ELSE '' END AS "F drive to Image 2 for {nq_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_003 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_003 || '")' ELSE '' END AS "F drive to Image 3 for {q_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_003 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_003 || '")' ELSE '' END AS "F drive to Image 3 for {nq_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_004 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{q_year} {q_str} Invoices\\' || related_file_004 || '")' ELSE '' END AS "F drive to Image 4 for {q_str}",
            CASE WHEN sequence_change_flag = 'CHANGE' AND related_file_004 IS NOT NULL THEN '=HYPERLINK("F:\\Images MP-BC-AP R4Q2\\{nq_year} {nq_str} Invoices\\' || related_file_004 || '")' ELSE '' END AS "F drive to Image 4 for {nq_str}",
            CASE 
                WHEN "For Sequence #" IS NULL OR "For Sequence #" = '' THEN '0'
                WHEN sequence_change_flag = 'SAME' THEN '0'
                ELSE raw_fn 
            END AS "Filename of Image for the UT Tax Comm.",
            '' AS " UT + SJ Combined Sales Tax ",
            '' AS " Utah State Sales Tax ",
            '' AS " San Juan County Sales Tax ",
            '' AS " Other local Utah tax ",
            '' AS " Other entity collecting tax ",
            '' AS " Sum of UT Tx Excl Chrgd by N.N.",
            '' AS " NNOGC Entity Tx Pd Amt ",
            '' AS " Poley Team Notes "
        FROM stage4;
        """

        try:
            conn.executescript(sql_logic)
            df_final = pd.read_sql("SELECT * FROM final_report", conn)
            drop_cols = ['clean_gross', 'row_id', 'inv_total', 'invoice_change_flag', 
                         'related_file_001', 'related_file_002', 'related_file_003', 'related_file_004', 'raw_fn']
            df_final.drop(columns=drop_cols, inplace=True, errors='ignore')

            output_path = os.path.join(user_out, f"{year} {month:02d} Sales Tax - NNOGC PY d1-4.xlsx")
            
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                df_final.to_excel(writer, sheet_name='Sales Tax Report', index=False)
                workbook = writer.book
                worksheet = writer.sheets['Sales Tax Report']
                header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC', 'border': 1})
                
                # --- UPDATED COLUMN LOGIC (NO GROUPING) ---
                for i, col in enumerate(df_final.columns):
                    # Write header with formatting
                    worksheet.write(0, i, col, header_fmt)
                    
                    # Set standard column width based on content length
                    max_len = max(df_final[col].astype(str).map(len).max(), len(col)) + 2
                    worksheet.set_column(i, i, min(max_len, 50))
                
                # Apply filter to headers
                worksheet.autofilter(0, 0, 0, len(df_final.columns) - 1)
                
            print(f"   Saved: {os.path.basename(output_path)}")
        except Exception as e:
            print(f"   ERROR processing Month {month}: {e}")
        finally:
            conn.close()

    print(f"\nALL DONE! Files are in: {user_out}")
    open_folder(user_out)

if __name__ == "__main__":
    run_processor()
