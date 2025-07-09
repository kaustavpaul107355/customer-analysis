# Customer Purchase Behavior Analytics App

This Streamlit app provides interactive analytics and visualizations for customer purchase behavior data stored in a Databricks SQL warehouse.

## Features
- Multi-tab interface: Data Overview and Analytics
- Category-based filtering (multi-select)
- Customer and geographic analysis
- Robust error handling and user feedback
- Modern, responsive UI

## Setup

### 1. Prerequisites
- Python 3.8+
- Access to a Databricks SQL warehouse
- Databricks Service Principal or user token

### 2. Installation
Clone the repository and install dependencies:
```bash
pip install -r requirements.txt
```

### 3. Configuration
Set up your `app.yaml` with the required environment variables:
```yaml
command: ["streamlit", "run", "app.py"]
env:
  - name: "DATABRICKS_WAREHOUSE_ID"
    valueFrom: "sql-warehouse"
  - name: STREAMLIT_BROWSER_GATHER_USAGE_STATS
    value: "false"
```

You can also set `DATABRICKS_WAREHOUSE_ID` in your shell environment.

### 4. Running the App
```bash
streamlit run app.py
```

## Usage
- **Data Overview Tab:**
  - View table structure, column info, and sample records.
- **Analytics Tab:**
  - Filter by one or more categories.
  - Explore customer and geographic insights.
  - Visualize top categories and value segments.

## Troubleshooting
- **DATABRICKS_WAREHOUSE_ID not set:** Ensure the environment variable is set in `app.yaml` or your shell.
- **No data returned:** Check your Databricks table name, permissions, and query.
- **Token errors:** Make sure your authentication token is valid and passed correctly.
- **Dependency issues:** Run `pip install -r requirements.txt` to ensure all packages are installed.

## Development
- The `.vscode/settings.json` is only needed if you use VSCode with Databricks/Jupyter integration. It is not required for running the app.
- You can safely delete the `.vscode` folder if you do not use VSCode or do not need these settings.

## License
MIT 