# WQ-Brain

WQ-Brain is a Python project for interacting with the WorldQuant Brain platform API, retrieving data fields, generating alpha factors, and running backtests.

## Features

- **User Authentication**: Log in to the WorldQuant Brain platform using your credentials.
- **Data Field Retrieval**: Fetch data fields in bulk based on various filters (instrumentType, region, delay, universe, etc.).
- **Alpha Generation**: Automatically generate alpha expressions and payloads from data fields.
- **Simulation & Backtesting**: Submit simulation jobs and poll for results automatically.

## Directory Structure

```
WQ-Brain/
├── brain/
│   ├── AlphaSimulator.py
│   ├── brain1.py
│   ├── brain2.py
│   ├── brain3.py
│   ├── brain4.py
│   └── brain_credentials.txt
├── Alpha_Factory/
│   ├── Alpha Machine Factory.ipynb
│   └── machine_lib.py
└── README.md
```

## Requirements

- Python 3.8+
- requests
- pandas

Install dependencies with:
```bash
pip install requests pandas
```

## Getting Started

1. **Prepare Credentials**  
   Create a `brain_credentials.txt` file in the `brain/` directory with one of the following formats:
   ```json
   ["your_username", "your_password"]
   ```
   or
   ```json
   {"username": "your_username", "password": "your_password"}
   ```

2. **Run the Main Script**  
   For example, to run the main workflow:
   ```bash
   python ./brain/brain1.py
   ```

3. **Workflow Overview**
   - Authenticate with the platform
   - Retrieve data fields
   - Generate and filter alphas
   - Submit simulations and collect results

## Notes

- Keep your credentials secure and do not share them.
