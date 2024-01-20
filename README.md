---

# ETL Pipeline for Employment Data

## Project Overview
This project is an ETL (Extract, Transform, Load) pipeline designed to process and store employment data.
It extracts data from specified sources, transforms it through various cleaning and processing steps, and loads
it into a SQLite database for further analysis and storage.

## Features
- **Data Extraction**: Downloads data from online sources.
- **Data Cleaning**: Processes and cleans the data for consistency and usability.
- **Data Loading**: Stores the cleaned data in a SQLite database.

## Prerequisites
- Python 3.x
- Pandas library
- SQLite3

## Installation
1. Clone the repository:
   ```bash
   git clone git@github.com:DrAdamDev/Data-Engineering-Essentials.git
   ```
2. Install required Python packages:
   ```bash
   pip install pandas
   pip install requests
   ```

## File Structure
- `data_extraction.py`: Handles the extraction of data from online sources.
- `data_cleaning.py`: Contains functions for cleaning and processing the data.
- `database_operations.py`: Manages the loading of data into a SQLite database.
- `main.py`: The main script to run the ETL process.

## Usage
Run the main script to execute the ETL process:
```bash
python main.py
```

## Viewing the Database
To view the SQLite database, upload the .db file provide at https://sqliteviewer.app/ or use an SQLite database viewer like DB Browser for SQLite,
or access it via the SQLite command-line tool:
```bash
sqlite3 /path/to/your/databasefile.db
```

## License
[MIT License](LICENSE)

---
