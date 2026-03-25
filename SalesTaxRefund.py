import argparse
import os
import pandas as pd
import numpy as np

def load_config(txt_path):
    with open(txt_path, 'r') as f:
        lines = [l.strip(' "\'\n\r') for l in f.readlines() if l.strip()]
    if len(lines) < 3:
        raise ValueError("Config text file must have at least 3 lines: JIB path, Ref path, Output dir/path")
    return lines[0], lines[1], lines[2]

def run(jib_path, ref_path, out_path, year, months):
    print("Loading JIB Excel file...")
    xl = pd.ExcelFile(jib_path)
    sheet_name = xl.sheet_names[0]
    for name in xl.sheet_names:
        if 'JIB DETAIL' in name.upper() or 'JIB' in name.upper():
            if 'PIVOT' not in name.upper():
                sheet_name = name
                break
    df = pd.read_excel(jib_path, sheet_name=sheet_name)
    
    print("Loading Invoice Reference Excel file...")
    ref_df = pd.read_excel(ref_path)

    print("Processing JIB Data...")
    if 'Name 1' in df.columns:
        df.rename(columns={'Name 1': 'Vendor Name'}, inplace=True)

    if 'Nnogc Entity Tax Pd Amt' in df.columns:
        df.rename(columns={'Nnogc Entity Tax Pd Amt': 'NNOGC Entity Tax Pd Amt'}, inplace=True)

    # Convert Txn Inv Date
    if 'Txn Inv Date' in df.columns:
        df['Txn Inv Date parsed'] = pd.to_datetime(df['Txn Inv Date'], errors='coerce')
        df = df.sort_values('Txn Inv Date parsed').reset_index(drop=True)
        valid_idx = df[(df['Txn Inv Date parsed'].dt.year == int(year)) & (df['Txn Inv Date parsed'].dt.month.isin(months))].index
        if not valid_idx.empty:
            min_idx = max(0, valid_idx.min() - 1)
            max_idx = min(len(df) - 1, valid_idx.max() + 1)
            df = df.iloc[min_idx:max_idx+1].copy()
            df.drop(columns=['Txn Inv Date parsed'], inplace=True)
            df.reset_index(drop=True, inplace=True)
        else:
            print("Warning: No records found for the specified month(s) and year.")
    
    # Sort
    sort_cols = ['Vendor Name', 'Txn Invoice No', 'Property', 'Billing Cat', 'Txn Gross Amt']
    asc = [True, True, True, True, False]
    valid_sort = [c for c in sort_cols if c in df.columns]
    valid_asc = [asc[i] for i, c in enumerate(sort_cols) if c in df.columns]
    if valid_sort:
        df = df.sort_values(by=valid_sort, ascending=valid_asc)
    
    # Gross amounts
    if valid_sort:
        is_duplicate = df.duplicated(subset=valid_sort, keep='first')
        if 'Txn Gross Amt' in df.columns:
            df['Gross amount of first occurrence'] = np.where(is_duplicate, 0, df['Txn Gross Amt'])
            df['Gross Amount of 100% of Invoice'] = df.groupby('Txn Invoice No')['Gross amount of first occurrence'].transform('sum')
    
    # Sort again
    sort2 = ['Gross Amount of 100% of Invoice', 'Vendor Name', 'Txn Invoice No', 'Property', 'Billing Cat', 'Txn Gross Amt']
    asc2 = [False, True, True, True, True, False]
    v_sort2 = [c for c in sort2 if c in df.columns]
    v_asc2 = [asc2[i] for i, c in enumerate(sort2) if c in df.columns]
    if v_sort2:
        df = df.sort_values(by=v_sort2, ascending=v_asc2)
    
    # Filter
    if 'Gross Amount of 100% of Invoice' in df.columns:
        df = df[df['Gross Amount of 100% of Invoice'].abs() >= 2000]

    # Merge images
    inv_col = 'Invoice #' if 'Invoice #' in ref_df.columns else ref_df.columns[0]
    inv_idx = ref_df.columns.get_loc(inv_col)
    
    img_cols = ref_df.columns[inv_idx+1 : inv_idx+5]
    img_map = ref_df.drop_duplicates(subset=[inv_col]).set_index(inv_col)[img_cols].to_dict('index')
    
    img1, img2, img3, img4 = [], [], [], []
    for inv in df['Txn Invoice No']:
        imgs = img_map.get(inv, {})
        vals = [imgs.get(c, 0) for c in img_cols]
        vals = [v if pd.notna(v) and str(v).strip() != '' else 0 for v in vals]
        vals += [0] * (4 - len(vals))
        img1.append(vals[0])
        img2.append(vals[1])
        img3.append(vals[2])
        img4.append(vals[3])
    
    df['Image 1 from Elk Look-up'] = img1
    df['Image 2 from Elk Look-up'] = img2
    df['Image 3 from Elk Look-up'] = img3
    df['Image 4 from Elk Look-up'] = img4

    # Remove GJ or PE
    mask = df['Txn Invoice No'].astype(str).str.upper().str.startswith('GJ') | df['Txn Invoice No'].astype(str).str.upper().str.startswith('PE')
    df = df[~mask].reset_index(drop=True)

    # Sequence No
    is_first = (df['Txn Invoice No'] != df['Txn Invoice No'].shift(1))
    seq_no = is_first.cumsum()
    df.insert(0, 'For Sequence #', seq_no)
    df.insert(1, 'Sequence #', seq_no.apply(lambda x: f"{x:03d}"))

    y_str = str(year)[-2:] if len(str(year))==4 else str(year)
    m_str = f"{int(months[0]):02d}"
    df['Filename of Image for the UT Tax Comm.'] = np.where(is_first, f"S20{y_str}{m_str}-" + df['Sequence #'] + ".pdf", 0)

    # Quarter math
    cur_q_num = (int(months[0]) - 1) // 3 + 1
    next_q_num = (cur_q_num % 4) + 1
    cur_q = f"Q{cur_q_num}"
    next_q = f"Q{next_q_num}"
    cur_year = str(year)
    next_year = str(year) if next_q_num > 1 else str(int(year) + 1)
    
    db_links = []
    f_links = []
    
    for i in range(1, 5):
        lookup_col = f'Image {i} from Elk Look-up'
        db_cur = f'www.dropbox.com Link to Image {i} for {cur_q}'
        db_next = f'www.dropbox.com Link to Image {i} for {next_q}'
        f_cur = f'F drive Link to Image {i} for {cur_q}'
        f_next = f'F drive Link to Image {i} for {next_q}'
        
        db_links.extend([db_cur, db_next])
        f_links.extend([f_cur, f_next])
        
        def make_link(val, pfx, q_str, yr_str, first):
            if not first or str(val) == '0' or not str(val).strip() or str(val).strip() == 'nan':
                return 0
            if pfx == 'www.dropbox.com':
                path = f"{pfx}\\Images MP-BC-AP R4Q2\\{yr_str} {q_str} Invoices\\{val}"
            else:
                path = f"{pfx}\\Images MP-BC-AP R4Q2\\{yr_str} {q_str} Invoices\\{val}"
            return f'=HYPERLINK("{path}", "{path}")'
            
        df[db_cur] = [make_link(img, "www.dropbox.com", cur_q, cur_year, first) for img, first in zip(df[lookup_col], is_first)]
        df[db_next] = [make_link(img, "www.dropbox.com", next_q, next_year, first) for img, first in zip(df[lookup_col], is_first)]
        
        df[f_cur] = [make_link(img, "F:", cur_q, cur_year, first) for img, first in zip(df[lookup_col], is_first)]
        df[f_next] = [make_link(img, "F:", next_q, next_year, first) for img, first in zip(df[lookup_col], is_first)]
        
    add_cols = [
        'UT + SJ Combined Sales Tax',
        'Utah State Sales Tax',
        'San Juan County Sales Tax',
        'Other local Utah tax',
        'Other entity collecting tax',
        'Sum of UT Tx Excl Chrgd by N.N.',
        'NNOGC Entity Tax Pd Amt',
        'Poley Team Notes'
    ]
    for c in add_cols:
        if c not in df.columns:
            df[c] = ''

    all_cols = list(df.columns)
    link_set = set(db_links + f_links)
    base_cols = [c for c in all_cols if c not in link_set]
    
    try:
        idx_tc = base_cols.index('Filename of Image for the UT Tax Comm.')
        final_cols = base_cols[:idx_tc] + db_links + f_links + base_cols[idx_tc:]
    except ValueError:
        final_cols = base_cols + db_links + f_links 
        
    df = df[final_cols]
    
    print(f"Writing Excel to {out_path if out_path else 'Current Directory'}...")
    
    if not out_path or os.path.isdir(out_path):
        out_filename = f"{year} {m_str} Sales Tax - NNOGC PY d1-4-4.xlsx"
        if out_path:
            out_path = os.path.join(out_path, out_filename)
        else:
            out_path = out_filename
            
    writer = pd.ExcelWriter(out_path, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_formulas': True}})
    
    # Remove timezone info for excel export if needed
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].dt.tz_localize(None)

    df.to_excel(writer, index=False, sheet_name='sales tax by invoice')
    
    workbook = writer.book
    worksheet = writer.sheets['sales tax by invoice']
    
    date_format = workbook.add_format({'num_format': 'm/d/yyyy h:mm AM/PM'})
    header_format = workbook.add_format({'bold': True, 'border': 1})
    
    for col_num, value in enumerate(df.columns.values):
        worksheet.write(0, col_num, value, header_format)
        
    def group_cols(start_name, end_name=None):
        if start_name in df.columns:
            start_idx = df.columns.get_loc(start_name)
            if end_name and end_name in df.columns:
                end_idx = df.columns.get_loc(end_name)
                worksheet.set_column(start_idx, end_idx, None, None, {'hidden': True, 'level': 1})
            else:
                worksheet.set_column(start_idx, start_idx, None, None, {'hidden': True, 'level': 1})

    group_cols('Owner', 'Billing Cat Type') 
    group_cols('Txn Net Amt', 'Interest')
    group_cols('Property Hid', 'Vendor Code')
    group_cols('Addr 1')
    group_cols('Billing Date', 'Color Code')
    
    if 'Color Code' in df.columns and 'Gross amount of first occurrence' in df.columns:
        start_idx = df.columns.get_loc('Color Code') + 1
        end_idx = df.columns.get_loc('Gross amount of first occurrence') - 1
        if start_idx <= end_idx:
            worksheet.set_column(start_idx, end_idx, None, None, {'hidden': True, 'level': 1})
            
    if 'Txn Inv Date' in df.columns:
        idx = df.columns.get_loc('Txn Inv Date')
        worksheet.set_column(idx, idx, 20, date_format)
    if 'Txn Acct Date' in df.columns:
        idx = df.columns.get_loc('Txn Acct Date')
        worksheet.set_column(idx, idx, 20, date_format)

    writer.close()
    print(f"Successfully created {out_path}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process Sales Tax")
    parser.add_argument('--auto', help='Path to text file with paths', type=str)
    
    args = parser.parse_args()
    
    if args.auto:
        jib_path, ref_path, out_path = load_config(args.auto)
        year = input("Enter Year (YYYY): ").strip()
        months_str = input("Enter Month(s) separated by comma (e.g. 1, 2, 3): ").strip()
        months = [int(m.strip()) for m in months_str.split(',')]
    else:
        year = input("Enter Year (YYYY): ").strip()
        months_str = input("Enter Month(s) separated by comma (e.g. 1, 2, 3): ").strip()
        months = [int(m.strip()) for m in months_str.split(',')]
        jib_path = input("Enter JIB Excel file path: ").strip(' "\'')
        ref_path = input("Enter Invoice Reference Excel file path: ").strip(' "\'')
        out_path = input("Enter output directory path: ").strip(' "\'')
        
    run(jib_path, ref_path, out_path, year, months)
