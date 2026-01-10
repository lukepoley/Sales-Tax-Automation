This program automates the "NNOG Sales Tax Refund Creation" as outlined in `Sales only creation proc d5 AP TB.docx`

---
# ONE-TIME SETUP
#### 1. RUN THE SETUP FILE:
   - Right-click `setup.bat` and select "Run as Administrator".
   - This installs Python and the necessary data libraries.
   - Once finished, RESTART your computer.
---
# Running the Script
#### 1. PREPARE YOUR FILES:
   - Ensure  "NNOG JIB" Excel file and "Invoice Reference" Excel files are in dropbox and connected to your PC. Otherwise, you can go to Dropbox.com and download them.
#### 2. RUN THE TOOL:
   - Double-click `run.bat`.
   - A black window will open and ask you for:[^1]
     * The Month (1-12) and Year (YYYY).
	     * Ex. Type `3` and `2023` for March 2023 
     * The path to your JIB Excel file. [^2]
	     * Ex. `C:\Users\User\Dropbox\Audit 2023\2023 Core\NNOG JIB_01-2023 _ 06-2023 by inv date AP.xlsx
     * The path to your Invoice Reference Excel file.
	     * Ex. `C:\Users\User\Dropbox\Audit 2023\2023 Core\2023 Invoice Reference Combined for SQL.xlsx
     * The folder where you want the output Excel file to be saved.
	     * Ex. `C:\Users\User\Downloads\output
   - The screen will say '--- LOADING SOURCE DATA ---', this will take a while (10+ mins) to finish.
#### 3. GET YOUR RESULTS:
   When the window says `SUCCESS`, your final .xlsx file 
   will be waiting in your chosen output folder.

[^1]:    TIP: To "Paste" a file path, find the file in Windows, Right-Click it, select "Copy as path", then Right-Click inside the black window to paste it.
[^2]:    If there are spaces in the file name you must have quotes around the file path.
