# Practical Guide — Running the Employee ETL Pipeline

This guide walks students through setting up VS Code, installing Python/Pandas/PySpark,
and running both ETL pipelines (`etl_pipeline.py` and `pyspark_etl_pipeline.py`) end to end.

---

## Step 1: Install Python

Open Terminal in VS Code (`Ctrl + \``) and check if Python is installed.

```bash
python --version
```

or

```bash
python3 --version
```

**Example Output**

```
Python 3.12.4
```

---

## Step 2: Open Your Project

Suppose your folder structure is:

```
ETL_Project/
│
├── input/
│     └── employees.csv
│
├── output/
│
├── etl_pipeline.py
│
├── pyspark_etl_pipeline.py
│
└── requirements.txt
```

Open this folder in VS Code:

```
File
   ↓
Open Folder
```

Select `ETL_Project`.

---

## Step 3: Create a Virtual Environment (Recommended)

Open Terminal:

```bash
python -m venv venv
```

**Activate it**

Windows:

```bash
venv\Scripts\activate
```

Mac/Linux:

```bash
source venv/bin/activate
```

After activation you should see `(venv)` before the terminal path.

---

## Step 4: Install Pandas

```bash
pip install pandas
```

Check installation:

```bash
pip show pandas
```

---

## Step 5: Run the Pandas ETL Pipeline

```bash
python etl_pipeline.py
```

If everything is correct:

```
Reading CSV...

Cleaning data...

Calculating Salary...

Writing output...

Department Summary

IT        250000
HR        90000
Finance   150000

ETL Completed Successfully
```

**Output Folder**

```
output/
      employee_report.csv
```

Open it in Excel.

---

## Step 6: Install PySpark

```bash
pip install pyspark
```

Check:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Example:

```
4.0.0
```

---

-------------------------------------------------------------------
------------------------------------------------------------------

For this , setup is Required 



# Hadoop Setup Guide (Windows) — `winutils.exe` for PySpark

PySpark on Windows needs `winutils.exe` (a small Hadoop helper binary) even if you're
not running a real Hadoop cluster. This guide sets that up so `pyspark_etl_pipeline.py`
runs without `HADOOP_HOME` errors.

---

## Step 1: Download Git (if not installed)

Download Git from the official website:

[Git for Windows](https://git-scm.com/downloads)

Install it using the default options.

Verify:

```bash
git --version
```

---

## Step 2: Download the `winutils` Repository

Open PowerShell or the VS Code Terminal and run:

```bash
git clone https://github.com/cdarlint/winutils.git
```

Or download the ZIP directly from the
[cdarlint/winutils GitHub repository](https://github.com/cdarlint/winutils):

```
Code
   ↓
Download ZIP
```

Extract the ZIP.

---

## Step 3: Find `winutils.exe`

Inside the extracted folder, you'll see directories such as:

```
winutils/
│
├── hadoop-2.7.7
├── hadoop-3.0.0
├── hadoop-3.2.0
├── hadoop-3.3.6
```

Open the folder that matches your Hadoop version (for most recent Spark installations,
a Hadoop 3.x version is appropriate), then go to:

```
hadoop-3.x.x
    └── bin
          └── winutils.exe
```

---

## Step 4: Create the Hadoop Folder

Create these folders manually:

```
C:\
 └── hadoop
      └── bin
```

Copy `winutils.exe` into:

```
C:\hadoop\bin\
```

---

## Step 5: Set the `HADOOP_HOME` Environment Variable

1. Press the **Windows key**.
2. Search for:

   ```
   Edit the system environment variables
   ```

3. Click **Environment Variables**.

**Under System Variables**

Click **New**.

Enter:

```
Variable name:
HADOOP_HOME
```

```
Variable value:
C:\hadoop
```

Click **OK**.

> There is no password to set. Environment variables only require a name and a value.

---

## Step 6: Add Hadoop to the PATH

Still in Environment Variables:

1. Select the **Path** variable.
2. Click **Edit**.
3. Click **New**.
4. Add:

   ```
   C:\hadoop\bin
   ```

Click **OK** on all dialogs.

---

## Step 7: Restart VS Code

Close VS Code completely and open it again so the new environment variables load.

---

## Step 8: Verify the Setup

Run these commands in a **new** terminal:

```bash
echo $env:HADOOP_HOME
```

Expected output:

```
C:\hadoop
```

Then run:

```bash
where winutils
```

Expected output:

```
C:\hadoop\bin\winutils.exe
```

---

## A Note on Passwords

There is no password required anywhere in this setup:

- GitHub cloning does not require a password for public repositories.
- Setting `HADOOP_HOME` is just creating an environment variable with a name and a path.
- `winutils.exe` does not require a login or password.

---

## Reference

Repository used: <https://github.com/cdarlint/winutils.git>

Once this is done, `python pyspark_etl_pipeline.py` should run cleanly on Windows
without `HADOOP_HOME not set` or `winutils.exe not found` errors.


---------------------------------------------------------------------
---------------------------------------------------------------------

It not work :- 

pip uninstall pyspark
pip install pyspark==3.5.6
pip install "pandas<3.0"


## Step 7: Run the PySpark ETL Pipeline

```bash
python pyspark_etl_pipeline.py
```

Spark will start.

**Example Output**

```
Spark Session Created

Reading CSV

Transforming Data

Writing Parquet

Department Summary

+---------+------------+
|Department|TotalSalary|
+---------+------------+
|IT       |250000      |
|HR       |90000       |
|Finance  |150000      |
+---------+------------+

Pipeline Completed
```

---

## Step 8: Output Folder

```
output/

employee_report.csv

employee_report_spark/

      part-00000.csv

      _SUCCESS

      parquet/
```

---

## Step 9: Select the Python Interpreter

In VS Code:

```
Ctrl + Shift + P

↓

Python: Select Interpreter

↓

Choose your venv
```

---

## Step 10: Install the Required Extension

Install the **Python extension by Microsoft**. It provides:

- Run button
- IntelliSense
- Debugging
- Code completion
- Error highlighting

---

## Running from the VS Code Run Button

Open `etl_pipeline.py`. Click:

```
▶ Run Python File
```

or press `Ctrl + F5`.

Do the same for `pyspark_etl_pipeline.py`.

---

## Common Errors

**1. `ModuleNotFoundError: No module named 'pandas'`**

Install it:

```bash
pip install pandas
```

**2. `ModuleNotFoundError: No module named 'pyspark'`**

Install it:

```bash
pip install pyspark
```

**3. `'python' is not recognized`**

Python is not added to your system PATH. Reinstall Python and select
"Add Python to PATH", or use the Python launcher:

```bash
py --version
py etl_pipeline.py
```

**4. `FileNotFoundError`**

Ensure your input file exists, for example:

```
input/
    employees.csv
```

and that your script references the correct relative path.

---

## Classroom Demonstration Flow

1. Show the `employees.csv` input file.
2. Run `python etl_pipeline.py` and inspect `output/employee_report.csv`.
3. Explain the Extract → Transform → Load steps in Pandas.
4. Run `python pyspark_etl_pipeline.py`.
5. Compare the Spark output (CSV + Parquet) with the Pandas output.
6. Highlight how the code structure is nearly identical, but PySpark scales to
   distributed processing.
