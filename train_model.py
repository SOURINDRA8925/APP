import os
import pickle
import numpy as np
import pandas as pd

print("=== AUTOMATED MULTI-CSV MULTI-MODEL TRAINING PIPELINE ===")

# 1. SCAN DIRECTORY FOR ALL AVAILABLE CSV FILES
files = [f for f in os.listdir('.') if f.endswith('.csv')]

if not files:
    print(" Error: No CSV files found in your main project folder.")
    print(" Please place your training CSV files directly next to your app.py file.")
    exit()

print("\n📂 Available CSV Datasets Found:")
for index, filename in enumerate(files):
    print(f"  [{index}] {filename}")

# 2. INTERACTIVE SELECTOR INPUT
try:
    choice = int(input("\n Enter the number of the CSV file you want to train on: "))
    if choice < 0 or choice >= len(files):
        print(" Invalid selection number. Exiting training loop.")
        exit()
    SELECTED_CSV = files[choice]
except Exception:
    print(" Please enter a valid index integer number.")
    exit()

print(f"\n Processing selected data grid layout: '{SELECTED_CSV}'...")

# 3. GENERATE DYNAMIC OUTPUT MODEL NAME
base_name = os.path.splitext(SELECTED_CSV)[0]
DYNAMIC_MODEL_FILE = f"{base_name}_model.pkl"

# 4. LOAD THE SELECTED DATAFRAME MATRIX
try:
    df = pd.read_csv(SELECTED_CSV)
except Exception as e:
    print(f" Failed to parse file: {e}")
    exit()

if df.empty:
    print(" Error: The selected file contains zero row variables.")
    exit()

print(f"📊 SUCCESS: Loaded {len(df)} tracking entries for AI parameter analysis.")

# 5. ADVANCED SMART COLUMN LOOKUP MATRIX (Fixes 'Index' Error)
raw_columns = list(df.columns)
normalized_cols = [str(c).strip().lower() for c in df.columns]

# Initialize fallbacks defaults
amt_col = None
age_col = None
bal_col = None

# Scan and locate Amount numeric tracking fields
for idx, c in enumerate(normalized_cols):
    if 'amount' in c or 'amt' in c or 'refund' in c or 'tx_amount' in c:
        amt_col = raw_columns[idx]
        break

# Scan and locate Age tracking fields
for idx, c in enumerate(normalized_cols):
    if 'age' in c or 'customer_age' in c:
        age_col = raw_columns[idx]
        break

# Scan and locate Balance tracking fields
for idx, c in enumerate(normalized_cols):
    if 'balance' in c or 'bal' in c:
        bal_col = raw_columns[idx]
        break

# Bulletproof Failsafe: If columns aren't named, dynamically match by physical position index
if not amt_col:
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        amt_col = numeric_cols[0] # Grab the first available numeric column header
    else:
        amt_col = raw_columns[0]

if not age_col:
    age_col = raw_columns[1] if len(raw_columns) > 1 else raw_columns[0]

if not bal_col:
    bal_col = raw_columns[2] if len(raw_columns) > 2 else raw_columns[0]

print(f" Mapping Matrix Features -> Amount: '{amt_col}', Age: '{age_col}', Balance: '{bal_col}'")

try:
    # Convert text rows to category codes dynamically
    channel_col_name = None
    for idx, c in enumerate(normalized_cols):
        if 'channel' in c or 'mode' in c:
            channel_col_name = raw_columns[idx]
            break
            
    tx_type_col_name = None
    for idx, c in enumerate(normalized_cols):
        if 'type' in c or 'tx_type' in c:
            tx_type_col_name = raw_columns[idx]
            break

    df['channel_code'] = df[channel_col_name].astype('category').cat.codes if channel_col_name else 0
    df['txtype_code'] = df[tx_type_col_name].astype('category').cat.codes if tx_type_col_name else 0
    
    # 6. EXTRACT FEATURES ARRAYS
    X_amt = pd.to_numeric(df[amt_col], errors='coerce').fillna(0).values
    X_age = pd.to_numeric(df[age_col], errors='coerce').fillna(0).values
    X_bal = pd.to_numeric(df[bal_col], errors='coerce').fillna(0).values
    X_chan = df['channel_code'].values
    X_type = df['txtype_code'].values
    
    X = np.column_stack([X_amt, X_age, X_bal, X_chan, X_type])
    
    # 7. GENERATE BASELINE TARGET PATTERNS
    y = np.where((X[:, 0] > 5000) | (X[:, 2] < 200), 1, 0)
    
    # 8. EXECUTE RANDOM FOREST TREE CLASSIFIER TRAINING
    from sklearn.ensemble import RandomForestClassifier
    print(f" Processing variables. Training separate model file: {DYNAMIC_MODEL_FILE}...")
    
    model = RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42)
    model.fit(X, y)
    
    # 9. LOCK COMPILED CLASSIFIER MODEL TO UNIQUE PICKLE NAME FILE
    with open(DYNAMIC_MODEL_FILE, 'wb') as f:
        pickle.dump(model, f)
        
    print(f"\n SUCCESS: Trained AI model saved separately as: '{DYNAMIC_MODEL_FILE}'!")
    
except Exception as processing_error:
    print(f"❌ Feature matrix array compilation failure: {processing_error}")

print("=== DYNAMIC FILE SPECIFIC LOOP ROUTINE COMPLETE ===")
