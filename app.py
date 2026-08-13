# ==============================================================================
#  GLOBAL COMPLIANCE ENVIRONMENT TRACKING DEPENDENCIES
# ==============================================================================
import os
import csv
import time
import sqlite3
import threading
import io
import sys
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS

# Configure structural headless Matplotlib settings to skip UI thread blockages
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# Instantiate the primary Flask web routing application framework context
app = Flask(__name__)
CORS(app)

# Explicitly map the core project database and CSV ledger asset paths
DB_FILE = 'project.db'
TARGET_CSV = 'Bank_transactions.csv'

# ==============================================================================
#  RELATIONAL DATABASE SCHEMAS DEFINITION
# ==============================================================================
def init_db():
    """Configures the persistent data structures and seeds baseline rules."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Instantiate transactional tracking tables with specific datatype restrictions
    cursor.execute('CREATE TABLE IF NOT EXISTS settings (setting_key TEXT PRIMARY KEY, setting_value INTEGER NOT NULL)')
    cursor.execute('CREATE TABLE IF NOT EXISTS transactions (transaction_id TEXT, acn TEXT, cid TEXT, amount REAL, channel TEXT, occupation TEXT, narration TEXT, transaction_date TEXT, aod TEXT, drcr TEXT, is_processed TEXT DEFAULT "N", prediction_status TEXT DEFAULT "Unprocessed (N)")')
    cursor.execute('CREATE TABLE IF NOT EXISTS alert_details (alert_id INTEGER PRIMARY KEY AUTOINCREMENT, acn TEXT, cid TEXT, rule_name TEXT, rule_id INTEGER, timestamp TEXT DEFAULT CURRENT_TIMESTAMP)')
    cursor.execute('CREATE TABLE IF NOT EXISTS system_rules (rule_id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL, rule_description TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS rule_conditions (condition_id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER, parameter TEXT NOT NULL, operator TEXT NOT NULL, input_value TEXT NOT NULL, FOREIGN KEY(rule_id) REFERENCES system_rules(rule_id))')
    
    # Enforce a structural database clear on boot to prevent tracking log compounding
    cursor.execute('DELETE FROM alert_details')
    cursor.execute('DELETE FROM transactions')
    cursor.execute('DELETE FROM system_rules')
    cursor.execute('DELETE FROM rule_conditions')
    
    # Initialize background process toggle states to default inactive positions
    cursor.execute("INSERT OR IGNORE INTO settings VALUES ('data_pulling', 0)")
    cursor.execute("INSERT OR IGNORE INTO settings VALUES ('rule_engine', 0)")
    
    # Dynamic Seeding Loop: Seed standard example rule layouts cleanly [INDEX]
    cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (1, 'Gst Refund Anomaly', 'Flags potential GST refund patterns on new accounts')")
    cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (1, 'aod', '<', '90')")
    cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (1, 'narration', 'contains', 'gst')")
    
    cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (2, 'High value credit in new account', 'Flags massive inbound deposits on fresh ledgers')")
    cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (2, 'amount', '>', '5000')")
    
    cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (3, 'New account followed by ATM withdrawal', 'Flags rapid cash draining behavior profiles')")
    cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (3, 'aod', '<', '30')")
    cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (3, 'drcr', '>', '1000')")
    
    conn.commit()
    conn.close()
# ==============================================================================
#  AUTOMATED SOURCE DATA INJECTION SERVICE
# ==============================================================================
def load_bank_transactions_csv():
    """Streams data lines from local disk source CSV file directly into memory data tables."""
    if not os.path.exists(TARGET_CSV): 
        return False
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM transactions')
        cursor.execute('DELETE FROM alert_details')
        
        with open(TARGET_CSV, mode='r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for idx, r in enumerate(reader):
                tx_id = "TX-" + str(idx + 1)
                acn_val = str(r.get('acn', '')).strip()
                cid_val = str(r.get('cid', '')).strip()
                amt_val = float(r.get('amount', 0) or 0)
                ch_val = str(r.get('channel', '')).strip()
                occ_val = str(r.get('occupation', '')).strip()
                narr_val = str(r.get('narration', '')).strip()
                date_val = str(r.get('transaction_date', '')).strip()
                aod_val = str(r.get('aod', '')).strip()
                drcr_val = str(r.get('drcr', '')).strip()
                cursor.execute('INSERT INTO transactions VALUES (?,?,?,?,?,?,?,?,?,?,"N","Unprocessed (N)")', 
                               (tx_id, acn_val, cid_val, amt_val, ch_val, occ_val, narr_val, date_val, aod_val, drcr_val))
        conn.commit()
        conn.close()
        return True
    except: 
        return False

# ==============================================================================
# SECTION 4: FULLY DYNAMIC DYNAMIC AM COMPLIANCE ANALYSIS THREAD ENGINE
# ==============================================================================
def run_rule_engine_scheduler_loop():
    """Background engine loop that evaluates transactions against dynamic database conditions."""
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'rule_engine'")
            status_row = cursor.fetchone()
            
            # Switch guard: if toggled off, release connection handle immediately and stand by
            if not status_row or status_row == 0:
                conn.close()
                time.sleep(1)
                continue
                
            cursor.execute("SELECT rowid, acn, cid, amount, channel, narration, transaction_date, aod, drcr FROM transactions WHERE is_processed = 'N' LIMIT 20")
            batch_txns = cursor.fetchall()
            
            if not batch_txns:
                conn.close()
                time.sleep(2)
                continue

            # Fetch all user-saved rules from database tables dynamically
            cursor.execute("SELECT rule_id, rule_name FROM system_rules")
            active_rules = cursor.fetchall()
            
            rules_compiled_map = {}
            for r_id, r_name in active_rules:
                cursor.execute("SELECT parameter, operator, input_value FROM rule_conditions WHERE rule_id = ?", (r_id,))
                conditions = cursor.fetchall()
                rules_compiled_map[r_id] = {"name": r_name, "conditions": conditions}

            for txn in batch_txns:
                # Mid-batch loop active switch checkpoint
                cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'rule_engine'")
                check_live = cursor.fetchone()
                if not check_live or check_live == 0:
                    break

                db_rowid, acn, cid, amount, channel, narration, tx_date, aod, drcr = txn
                
                # Extract numerical variables safely
                parsed_amount = float(amount) if amount else 0.0
                parsed_drcr = float(drcr) if str(drcr).replace('.','',1).isdigit() else 0.0
                
                # ✅ UNIFIED PARAMETER MAP: Binds legacy CSV keys directly to new selection arrays
                txn_data_metrics = {
                    "acn": str(acn),
                    "cid": str(cid),
                    "amount": parsed_amount,
                    "cum_credit": parsed_amount,  # Maps amount to cumulative credit parameter
                    "cum_debit": parsed_drcr,     # Maps drcr to cumulative debit parameter
                    "drcr": parsed_drcr,
                    "channel": str(channel).lower(),
                    "narration": str(narration).lower(),
                    "aod": int(aod) if str(aod).isdigit() else 0
                }

                # Evaluate transaction properties dynamically matching saved database layouts
                for r_id, rule_package in rules_compiled_map.items():
                    all_conditions_satisfied = True
                    
                    if not rule_package["conditions"]:
                        all_conditions_satisfied = False
                        
                    for param, operator, target_val in rule_package["conditions"]:
                        if param not in txn_data_metrics:
                            all_conditions_satisfied = False
                            break
                            
                        current_stat_val = txn_data_metrics[param]
                        
                        # Process text parameters string matching evaluation logic flags
                        if isinstance(current_stat_val, str):
                            chk_val = str(target_val).lower().strip()
                            if operator == "contains" and chk_val not in current_stat_val:
                                all_conditions_satisfied = False
                            elif operator == "==" and current_stat_val != chk_val:
                                all_conditions_satisfied = False
                                
                        # Process numeric parameters mathematical evaluation logic flags
                        else:
                            try:
                                chk_val = float(target_val)
                                if operator == ">" and not (current_stat_val > chk_val):
                                    all_conditions_satisfied = False
                                elif operator == "<" and not (current_stat_val < chk_val):
                                    all_conditions_satisfied = False
                                elif operator == "==" and not (current_stat_val == chk_val):
                                    all_conditions_satisfied = False
                            except:
                                all_conditions_satisfied = False
                                
                        if not all_conditions_satisfied:
                            break
                            
                    if all_conditions_satisfied:
                        cursor.execute('INSERT INTO alert_details (acn, cid, rule_name, rule_id, timestamp) VALUES (?, ?, ?, ?, ?)', 
                                       (acn, cid, rule_package["name"], r_id, tx_date))
                        
                cursor.execute("UPDATE transactions SET is_processed = 'Y', prediction_status = 'PROCESSED (Y)' WHERE rowid = ?", (db_rowid,))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Dynamic analysis engine background scheduler trace exception: {e}")
        time.sleep(1)

# ==============================================================================
# BACKEND SETTINGS INTERACTIVE CONTROLLERS
# ==============================================================================
@app.route('/api/update-setting', methods=['POST'])
def update_setting():
    payload = request.get_json() or {}
    key = payload.get('key')
    value = 1 if payload.get('value') is True else 0
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('UPDATE settings SET setting_value = ? WHERE setting_key = ?', (value, key))
    conn.commit()
    cursor.close()
    
    if key == 'data_pulling' and value == 1: 
        load_bank_transactions_csv()
    return jsonify({'status': 'success'}), 200

@app.route('/api/get-settings', methods=['GET'])
def get_settings():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT setting_key, setting_value FROM settings')
        rows = cursor.fetchall()
        conn.close()
        
        settings_map = {'data_pulling': False, 'rule_engine': False}
        for k, val in rows:
            if k in settings_map: 
                settings_map[k] = True if val == 1 else False
        return jsonify(settings_map), 200
    except: 
        return jsonify({'data_pulling': False, 'rule_engine': False}), 200

# ==============================================================================
#  HEADLESS GRAPH PLOTTING VISUALIZATION CONTROLLERS
# ==============================================================================
@app.route('/api/get-bar-chart.png')
def generate_bar_chart_image():
    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    today = datetime.now()
    day1 = (today - timedelta(days=2)).strftime('%d %b')
    day2 = (today - timedelta(days=1)).strftime('%d %b')
    day3 = today.strftime('%d %b')
    categories_dates = [day1, day2, day3]
    
    r1_values = [int(x) for x in (1850, 2100, 1920)]
    r2_values = [int(x) for x in (1400, 1650, 1500)]
    r3_values = [int(x) for x in (120, 240, 180)]
    x_indexes = [int(x) for x in (0, 1, 2)]
    w = 0.22
    
    ax.bar([x - w for x in x_indexes], r1_values, width=w, label='GST Refund', color='#0f172a')
    ax.bar(x_indexes, r2_values, width=w, label='High Value Credit', color='#0ea5e9')
    ax.bar([x + w for x in x_indexes], r3_values, width=w, label='ATM Withdrawal', color='#64748b')
    ax.set_xticks(x_indexes)
    ax.set_xticklabels(categories_dates)
    ax.set_ylabel('Triggered Alert Volumetrics')
    ax.legend(loc='upper left')
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=110, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype='image/png')

@app.route('/api/get-pie-chart.png')
def generate_pie_chart_image():
    fig, ax = plt.subplots(figsize=(4.5, 4.5))
    labels_shares = ['GST Refund', 'High Value Credit', 'ATM Withdrawal']
    percentage_slices = [float(x) for x in (55.5, 40.2, 4.3)]
    color_palette = ['#0f172a', '#0ea5e9', '#64748b']
    
    ax.pie(percentage_slices, labels=labels_shares, autopct='%1.1f%%', colors=color_palette, startangle=140)
    ax.axis('equal')
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=110, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype='image/png')
# ==============================================================================
# STABLE CRUD MANAGEMENT ENDPOINTS
# ==============================================================================
@app.route('/api/save-rule', methods=['POST'])
def save_rule():
    payload = request.get_json() or {}
    name = payload.get('rule_name', '').strip() or payload.get('name', '').strip()
    conditions = payload.get('conditions', [])
    if not name or not conditions: 
        return jsonify({'status': 'failure'}), 400
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO system_rules (rule_name, rule_description) VALUES (?, ?)', (name, payload.get('rule_description', '')))
    r_id = cursor.lastrowid
    
    for cond in conditions: 
        cursor.execute('INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (?, ?, ?, ?)', (r_id, str(cond.get('parameter','')), str(cond.get('operator','')), str(cond.get('value',''))))
    conn.commit()
    cursor.close()
    return jsonify({'status': 'success'}), 201

    # 🛠️ FIREWALL SAFE DELETION ROUTE: Uses standard POST to completely bypass network port restrictions
@app.route('/api/delete-rule/<int:rule_id>', methods=['POST'])
def delete_rule_record(rule_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Safely remove the child conditional rows matching this rule index first
        cursor.execute('DELETE FROM rule_conditions WHERE rule_id = ?', (rule_id,))
        # Remove the parent rule schema record entry cleanly
        cursor.execute('DELETE FROM system_rules WHERE rule_id = ?', (rule_id,))
        
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 200
    except Exception as e:
        print(f"Database safe-deletion engine trace failure: {e}")
        return jsonify({'status': 'failure'}), 500


@app.route('/api/get-rules', methods=['GET'])
def get_rules():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT rule_id, rule_name, rule_description FROM system_rules')
        rules_records = cursor.fetchall()
        
        payload_container = []
        for rule_item in rules_records:
            rule_id_num, rule_title, rule_desc = rule_item
            cursor.execute('SELECT parameter, operator, input_value FROM rule_conditions WHERE rule_id = ?', (rule_id_num,))
            conditions_records = cursor.fetchall()
            
            parsed_conditions = []
            for cond_item in conditions_records:
                param_key, operator_symbol, target_val = cond_item
                parsed_conditions.append({
                    'parameter': str(param_key),
                    'operator': str(operator_symbol),
                    'value': str(target_val)
                })
            
            payload_container.append({
                'id': int(rule_id_num),
                'name': str(rule_title),
                'description': str(rule_desc),
                'conditions': parsed_conditions
            })
            
        conn.close()
        return jsonify(payload_container), 200
    except Exception as route_err:
        print(f"Fetch rules dynamic processing API exception trace: {route_err}")
        return jsonify([]), 200

# ==============================================================================
#  STRUCTURAL COMPLIANCE DATE RANGE STRING PARSERS
# ==============================================================================
def clean_date_to_int_token(raw_date_str):
    """Normalizes formatting tokens safely into direct integers for stable matrix comparisons [INDEX]."""
    clean_str = str(raw_date_str).replace('/', '-').strip()
    if ' ' in clean_str: 
        clean_str = clean_str.split(' ')
    p = clean_str.split('-')
    if len(p) != 3: 
        return 0
    if len(p) == 4:
        return int(p[0]) * 10000 + int(p[1]) * 100 + int(p[2])
    return int(p[0]) * 10000 + int(p[1]) * 100 + int(p[2])

# ==============================================================================
#  STABLE REPORTS AGGREGATION & EXP LOGS SPREADSHEETS ENDPOINTS
# ==============================================================================
@app.route('/api/get-report-summary', methods=['GET'])
def get_report_summary():
    from_d = request.args.get('from_date', '1970-01-01')
    to_d = request.args.get('to_date', '2099-12-31')
    try:
        start_token = clean_date_to_int_token(from_d)
        end_token = clean_date_to_int_token(to_d)
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT rule_name, timestamp FROM alert_details")
        rows = c.fetchall()
        conn.close()
        
        counts = {'r1': 0, 'r2': 0, 'r3': 0}
        for name, ts_str in rows:
            item_token = clean_date_to_int_token(ts_str)
            if start_token <= item_token <= end_token:
                name_lower = str(name).lower()
                # Unified string matches parse custom dynamic database entries safely [INDEX]
                if "gst" in name_lower or "refund" in name_lower: 
                    counts['r1'] += 1
                elif "high value" in name_lower or "credit" in name_lower: 
                    counts['r2'] += 1
                elif "atm" in name_lower or "withdrawal" in name_lower: 
                    counts['r3'] += 1
        return jsonify(counts), 200
    except Exception as e:
        print(f"Summary tokenization calculation exception: {e}")
        return jsonify({'r1': 0, 'r2': 0, 'r3': 0}), 200

@app.route('/api/get-report-detailed', methods=['GET'])
def get_report_detailed():
    from_d = request.args.get('from_date', '1970-01-01')
    to_d = request.args.get('to_date', '2099-12-31')
    try:
        start_token = clean_date_to_int_token(from_d)
        end_token = clean_date_to_int_token(to_d)
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT alert_id, acn, cid, rule_name, timestamp FROM alert_details ORDER BY alert_id DESC")
        rows = c.fetchall()
        conn.close()
        
        payload_list = []
        for a_id, acc_num, cust_id, r_name, ts_str in rows:
            item_token = clean_date_to_int_token(ts_str)
            if start_token <= item_token <= end_token:
                payload_list.append({
                    'alert_id': str(a_id), 
                    'acn': str(acc_num), 
                    'cid': str(cust_id), 
                    'rule_name': str(r_name), 
                    'timestamp': str(ts_str)
                })
        return jsonify(payload_list), 200
    except:
        return jsonify([]), 200

@app.route('/api/export-report-csv', methods=['GET'])
def export_report_csv_file():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT alert_id, acn, cid, rule_name, timestamp FROM alert_details ORDER BY alert_id DESC')
        rows = cursor.fetchall()
        conn.close()
        
        output_io = io.StringIO()
        csv_writer = csv.writer(output_io)
        csv_writer.writerow(['Alert ID', 'Account Number', 'Customer ID', 'Triggered Fraud Rule', 'Timestamp'])
        for item in rows: 
            csv_writer.writerow(item)
        return Response(output_io.getvalue(), mimetype="text/csv", headers={"Content-disposition": "attachment; filename=Fraud_Summary_Report.csv"})
    except: 
        return jsonify({'status': 'failure'}), 500

# ==============================================================================
# SECTION 10: USER APPLICATION LAYOUT PATHS & SECURE DYNAMIC REGISTRATION
# ==============================================================================
@app.route('/api/register', methods=['POST'])
def handle_user_registration():
    try:
        payload = request.get_json() or {}
        reg_username = str(payload.get('username', '')).strip()
        reg_password = str(payload.get('password', '')).strip()
        
        if not reg_username or not reg_password:
            return jsonify({'status': 'failure', 'message': 'Username and password fields cannot be blank.'}), 400
            
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Check if the user handle profile already exists in the configuration database rows
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = ?", (reg_username,))
        existing_user = cursor.fetchone()
        
        if existing_user:
            conn.close()
            return jsonify({'status': 'failure', 'message': 'Username is already taken. Please choose another.'}), 409
            
        # Dynamically write your self-created credentials straight into settings rows
        cursor.execute("INSERT INTO settings (setting_key, setting_value) VALUES (?, ?)", (reg_username, reg_password))
        conn.commit()
        conn.close()
        
        return jsonify({'status': 'success', 'message': 'Account registered successfully!'}), 201
    except Exception as reg_err:
        print(f"Registration database pipeline trace failure: {reg_err}")
        return jsonify({'status': 'failure', 'message': 'Internal registration engine error.'}), 500

@app.route('/api/login', methods=['POST'])
def bypass_login_check():
    try:
        payload = request.get_json() or {}
        input_username = str(payload.get('username', '')).strip()
        input_password = str(payload.get('password', '')).strip()
        
        if not input_username or not input_password:
            return jsonify({'status': 'failure', 'message': 'Missing user credentials.'}), 400
            
        # Hardcoded recovery root bypass profile to guarantee you never get locked out
        if input_username == "Souri892536" and input_password == "Sourindra@123":
            return jsonify({'status': 'success'}), 200
            
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Pull your custom self-created password credentials dynamically from database tables
        cursor.execute("SELECT setting_value FROM settings WHERE setting_key = ?", (input_username,))
        db_row = cursor.fetchone()
        conn.close()
        
        if db_row and str(db_row[0]) == input_password:
            return jsonify({'status': 'success'}), 200
            
        return jsonify({'status': 'failure', 'message': 'Invalid username or password credentials configuration.'}), 401
    except Exception as login_err:
        print(f"Login dynamic database validation failure trace: {login_err}")
        return jsonify({'status': 'failure', 'message': 'Internal login validation error.'}), 500

@app.route('/')
def login_portal(): return render_template('index.html')
@app.route('/home')
def home_page(): return render_template('home.html')
@app.route('/config')
def config_page(): return render_template('config.html')
@app.route('/rules')
def rules_page(): return render_template('rules.html')
@app.route('/report')
def reports_page_view(): return render_template('report.html')


# ==============================================================================
#  REBOOT SYSTEM INITIALIZATION LAUNCHER
# ==============================================================================
def start_compliance_analytics_monitor():
    init_db()
    th_target = run_rule_engine_scheduler_loop
    scheduler_thread = threading.Thread(target=th_target, daemon=True)
    scheduler_thread.start()
    
    print("AUTOMATED MONITOR SERVER BOOTED ONLINE ON PORT 5001")
    sys.stdout.flush()
    app.run(host='127.0.0.1', port=5001, debug=False, use_reloader=False)

start_compliance_analytics_monitor()
