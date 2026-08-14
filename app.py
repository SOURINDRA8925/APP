
# SECTION 1: GLOBAL PLATFORM SYSTEM LIBRARIES & SETUP

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

# Force Matplotlib graphics to render headlessly to prevent GUI thread conflicts
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

app = Flask(__name__)
CORS(app)

DB_FILE = 'project.db'
TARGET_CSV = 'Bank_transactions.csv'


# SECTION 2: RELATIONAL DATABASE CONFIGURATION

def init_db():
    """Initializes standard SQL storage tables without wiping data on boot."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('CREATE TABLE IF NOT EXISTS settings (setting_key TEXT PRIMARY KEY, setting_value INTEGER NOT NULL)')
    cursor.execute('CREATE TABLE IF NOT EXISTS transactions (transaction_id TEXT PRIMARY KEY, acn TEXT, cid TEXT, amount REAL, channel TEXT, occupation TEXT, narration TEXT, transaction_date TEXT, aod TEXT, drcr TEXT, is_processed TEXT DEFAULT "N", prediction_status TEXT DEFAULT "Unprocessed (N)")')
    cursor.execute('CREATE TABLE IF NOT EXISTS alert_details (alert_id INTEGER PRIMARY KEY AUTOINCREMENT, acn TEXT, cid TEXT, rule_name TEXT, rule_id INTEGER, timestamp TEXT DEFAULT CURRENT_TIMESTAMP)')
    cursor.execute('CREATE TABLE IF NOT EXISTS system_rules (rule_id INTEGER PRIMARY KEY AUTOINCREMENT, rule_name TEXT NOT NULL, rule_description TEXT)')
    cursor.execute('CREATE TABLE IF NOT EXISTS rule_conditions (condition_id INTEGER PRIMARY KEY AUTOINCREMENT, rule_id INTEGER, parameter TEXT NOT NULL, operator TEXT NOT NULL, input_value TEXT NOT NULL, FOREIGN KEY(rule_id) REFERENCES system_rules(rule_id))')
    
    # Initialize basic background switch keys to off positions safely
    cursor.execute("INSERT OR IGNORE INTO settings VALUES ('data_pulling', 0)")
    cursor.execute("INSERT OR IGNORE INTO settings VALUES ('rule_engine', 0)")
    
    # Seed baseline rules solely if the system rules table is empty
    cursor.execute("SELECT COUNT(*) FROM system_rules")
    if cursor.fetchone() == 0:
        cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (1, 'Gst Refund Anomaly', 'Flags potential GST refund patterns on new accounts')")
        cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (1, 'aod', '<', '90')")
        cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (1, 'narration', 'contains', 'gst')")
        
        cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (2, 'High value credit in new account', 'Flags massive inbound deposits on fresh ledgers')")
        cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (2, 'amount', '>', '5000')")
        
        cursor.execute("INSERT INTO system_rules (rule_id, rule_name, rule_description) VALUES (3, 'New account followed by ATM withdrawal', 'Flags rapid cash draining behavior profiles')")
        cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (3, 'aod', '<', '30')")
        cursor.execute("INSERT INTO rule_conditions (rule_id, parameter, operator, input_value) VALUES (3, 'amount', '>', '1000')")
        
    conn.commit()
    conn.close()


# SECTION 3: AUTOMATED DATA INJECTION SERVICE WITH FLOATING dec RECTIFICATION

def load_bank_transactions_csv():
    """Streams transaction data rows directly from the CSV file into DB with float cleanup."""
    if not os.path.exists(TARGET_CSV): 
        return False
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        
        # Clear out previous table caches to prevent data duplication issues
        cursor.execute('DELETE FROM transactions')
        cursor.execute('DELETE FROM alert_details')
        
        with open(TARGET_CSV, mode='r', encoding='utf-8-sig', errors='ignore') as f:
            reader = csv.DictReader(f)
            for idx, r in enumerate(reader):
                tx_id = "TX-" + str(idx + 1)
                acn_val = str(r.get('acn', '')).strip()
                cid_val = str(r.get('cid', '')).strip()
                
                # ✅ FIX: Strict programmatic extraction purges text padding strings cleanly
                raw_amt = str(r.get('amount', '0')).replace('$', '').replace(',', '').strip()
                try:
                    amt_val = float(raw_amt) if raw_amt else 0.0
                except:
                    amt_val = 0.0
                    
                ch_val = str(r.get('channel', '')).strip()
                occ_val = str(r.get('occupation', '')).strip()
                narr_val = str(r.get('narration', '')).strip()
                date_val = str(r.get('transaction_date', '')).strip()
                aod_val = str(r.get('aod', '')).strip()
                drcr_val = str(r.get('drcr', '')).strip()
                
                cursor.execute('INSERT INTO transactions (transaction_id, acn, cid, amount, channel, occupation, narration, transaction_date, aod, drcr) VALUES (?,?,?,?,?,?,?,?,?,?)', 
                               (tx_id, acn_val, cid_val, amt_val, ch_val, occ_val, narr_val, date_val, aod_val, drcr_val))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"CSV data parsing channel pipeline failure trace: {e}")
        return False


# SECTION 4: FIREWALL-SAFE REAL-TIME RULES PROCESSING ENGINE

def run_rule_engine_scheduler_loop():
    """Background engine loop that evaluates transactions with strict float casting and logs live timestamps."""
    while True:
        try:
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT setting_value FROM settings WHERE setting_key = 'rule_engine'")
            status_row = cursor.fetchone()
            
            if not status_row or status_row == 0:
                conn.close()
                time.sleep(1)
                continue
                
            cursor.execute("SELECT transaction_id, acn, cid, amount, channel, narration, transaction_date, aod, drcr FROM transactions WHERE is_processed = 'N' LIMIT 20")
            batch_txns = cursor.fetchall()
            
            if not batch_txns:
                conn.close()
                time.sleep(2)
                continue

            cursor.execute("SELECT rule_id, rule_name FROM system_rules")
            active_rules = cursor.fetchall()
            
            rules_compiled_map = {}
            for r_id, r_name in active_rules:
                cursor.execute("SELECT parameter, operator, input_value FROM rule_conditions WHERE rule_id = ?", (r_id,))
                conditions = cursor.fetchall()
                rules_compiled_map[r_id] = {"name": r_name, "conditions": conditions}

            for txn in batch_txns:
                tx_id, acn, cid, amount, channel, narration, tx_date, aod, drcr = txn
                
                #  Strict programmatic extraction purges text padding strings cleanly
                clean_amount_str = str(amount).replace('$', '').replace(',', '').strip()
                try:
                    parsed_amount = float(clean_amount_str) if clean_amount_str else 0.0
                except:
                    parsed_amount = 0.0
                
                txn_data_metrics = {
                    "acn": str(acn),
                    "cid": str(cid),
                    "amount": parsed_amount,
                    "cum_credit": parsed_amount,
                    "cum_debit": parsed_amount,
                    "drcr": str(drcr).strip().lower(),
                    "channel": str(channel).lower(),
                    "narration": str(narration).lower(),
                    "aod": int(aod) if str(aod).isdigit() else 0
                }

                for r_id, rule_package in rules_compiled_map.items():
                    all_conditions_satisfied = True
                    if not rule_package["conditions"]:
                        all_conditions_satisfied = False
                        
                    for param, operator, target_val in rule_package["conditions"]:
                        if param not in txn_data_metrics:
                            all_conditions_satisfied = False
                            break
                            
                        current_stat_val = txn_data_metrics[param]
                        
                        if isinstance(current_stat_val, str):
                            chk_val = str(target_val).lower().strip()
                            if operator == "contains" and chk_val not in current_stat_val:
                                all_conditions_satisfied = False
                            elif operator == "==" and current_stat_val != chk_val:
                                all_conditions_satisfied = False
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
                        # ✅ FIX: Automatically grabs your machine's exact system date and time right now
                        current_live_time = datetime.now().strftime('%d %b %Y %H:%M:%S')
                        cursor.execute('INSERT INTO alert_details (acn, cid, rule_name, rule_id, timestamp) VALUES (?, ?, ?, ?, ?)', 
                                       (acn, cid, rule_package["name"], r_id, current_live_time))
                        
                cursor.execute("UPDATE transactions SET is_processed = 'Y', prediction_status = 'PROCESSED (Y)' WHERE transaction_id = ?", (tx_id,))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Dynamic analysis engine background worker failure trace: {e}")
        time.sleep(1)


# SECTION 5: APP ROUTING CONTROLLER INPUTS

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
            if k in settings_map: settings_map[k] = True if val == 1 else False
        return jsonify(settings_map), 200
    except:
        return jsonify({'data_pulling': False, 'rule_engine': False}), 200


# SECTION 6: LIVE RECURSIVE PLOTTING CHARTS GENERATOR FROM DATABASE ROWS

@app.route('/api/get-bar-chart.png')
def generate_bar_chart_image():
    """Generates the timeline history bar chart dynamically from database alert details."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT rule_id, timestamp FROM alert_details")
        alert_rows = cursor.fetchall()
        conn.close()
    except Exception as e:
        print(f"Chart database fetch error: {e}")
        alert_rows = []

    today = datetime.now()
    day1_str = (today - timedelta(days=2)).strftime('%d %b')
    day2_str = (today - timedelta(days=1)).strftime('%d %b')
    day3_str = today.strftime('%d %b')
    categories_dates = [day1_str, day2_str, day3_str]

    r1_values = [0, 0, 0]
    r2_values = [0, 0, 0]
    r3_values = [0, 0, 0]

    for item in alert_rows:
        rule_id, ts_string = item
        ts_lower = str(ts_string).lower()
        
        day_idx = 2  
        if day1_str.lower() in ts_lower:
            day_idx = 0
        elif day2_str.lower() in ts_lower:
            day_idx = 1
        elif day3_str.lower() in ts_lower:
            day_idx = 2

        if rule_id == 1:
            r1_values[day_idx] += 1
        elif rule_id == 2:
            r2_values[day_idx] += 1
        elif rule_id == 3:
            r3_values[day_idx] += 1

    fig, ax = plt.subplots(figsize=(6.5, 4.2))
    x_indexes = [0, 1, 2]
    w = 0.22
    
    ax.bar([x - w for x in x_indexes], r1_values, width=w, label='GST Refund', color='#0ea5e9')
    ax.bar(x_indexes, r2_values, width=w, label='High Value Credit', color='#0f172a')
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
    """Generates the volumetric proportions pie chart dynamically from database alert details."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("SELECT rule_id FROM alert_details")
        alert_rows = cursor.fetchall()
        conn.close()
        
        r1 = sum(1 for r in alert_rows if r[0] == 1)
        r2 = sum(1 for r in alert_rows if r[0] == 2)
        r3 = sum(1 for r in alert_rows if r[0] == 3)
        total_sum = r1 + r2 + r3
    except Exception as e:
        print(f"Pie chart error: {e}")
        r1 = r2 = r3 = total_sum = 0

    fig, ax = plt.subplots(figsize=(4.5, 4.5))
    
    if total_sum == 0:
        ax.pie([1], labels=['No Active Database Alerts Found'], colors=['#cbd5e1'], startangle=140)
    else:
        ax.pie([r1, r2, r3], labels=['GST Refund', 'High Value Credit', 'ATM Withdrawal'], autopct='%1.1f%%', colors=['#0ea5e9', '#0f172a', '#64748b'], startangle=140)
        
    ax.axis('equal')
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=110, bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype='image/png')



# SECTION 7: INTERACTIVE SAVE, GET & Dynamic REST CONFIGURATORS

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


@app.route('/api/get-rules', methods=['GET'])
def get_rules():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('SELECT rule_id, rule_name, rule_description FROM system_rules')
        rules_records = cursor.fetchall()
        
        payload_container = []
        for rule_item in rules_records:
            r_id, r_title, r_desc = rule_item
            cursor.execute('SELECT parameter, operator, input_value FROM rule_conditions WHERE rule_id = ?', (r_id,))
            conditions_records = cursor.fetchall()
            
            conds = []
            for c in conditions_records:
                conds.append({
                    'parameter': str(c[0]), 
                    'operator': str(c[1]), 
                    'value': str(c[2])
                })
            payload_container.append({
                'id': int(r_id), 
                'name': str(r_title), 
                'description': str(r_desc), 
                'conditions': conds
            })
        conn.close()
        return jsonify(payload_container), 200
    except Exception as e: 
        print(f"Get rules error: {e}")
        return jsonify([]), 200



# SECTION 8: STRUCTURAL COMPLIANCE DATE RANGE STRING PARSERS

def clean_date_to_int_token(raw_date_str):
    """Converts both frontend YYYY-MM-DD inputs and backend text month strings safely into comparable integers."""
    try:
        clean_str = str(raw_date_str).strip()
        if not clean_str:
            return 0
            
        # Context 1: Handle frontend calendar input strings (e.g., "2026-08-14")
        if '-' in clean_str and ' ' not in clean_str:
            parts = clean_str.split('-')
            if len(parts) == 3:
                return int(parts[0]) * 10000 + int(parts[1]) * 100 + int(parts[2])
                
        # Context 2: Handle backend live system timestamp strings (e.g., "14 Aug 2026 12:45:00")
        if ' ' in clean_str:
            date_chunk = clean_str.split(' ')
            day_val = int(date_chunk[0])
            year_val = int(date_chunk[2])
            
            months_map = {
                "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
                "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
            }
            month_str = str(date_chunk[1]).lower()[:3]
            month_val = months_map.get(month_str, 1)
            
            return year_val * 10000 + month_val * 100 + day_val
            
    except Exception as token_err:
        print(f"Date conversion normalization exception trace: {token_err}")
        
    return 0


# SECTION 9: REPORT AGGREGATORS & EXPORT SPREADSHEETS

@app.route('/api/get-report-summary', methods=['GET'])
def get_report_summary():
    from_d = request.args.get('from_date', '1970-01-01')
    to_d = request.args.get('to_date', '2099-12-31')
    try:
        start_token = clean_date_to_int_token(from_d)
        end_token = clean_date_to_int_token(to_d)
        
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT rule_id, timestamp FROM alert_details")
        rows = c.fetchall()
        conn.close()
        
        counts = {'r1': 0, 'r2': 0, 'r3': 0}
        for r_id, ts_str in rows:
            item_token = clean_date_to_int_token(ts_str)
            if start_token <= item_token <= end_token:
                if r_id == 1: 
                    counts['r1'] += 1
                elif r_id == 2: 
                    counts['r2'] += 1
                elif r_id == 3: 
                    counts['r3'] += 1
        return jsonify(counts), 200
    except Exception as e:
        print(f"Summary query pipeline exception: {e}")
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
    except Exception as e:
        print(f"Detailed logs query pipeline exception: {e}")
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

@app.route('/api/delete-rule/<int:rule_id>', methods=['POST'])
def delete_rule_record(rule_id):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM rule_conditions WHERE rule_id = ?', (rule_id,))
        cursor.execute('DELETE FROM system_rules WHERE rule_id = ?', (rule_id,))
        conn.commit()
        conn.close()
        return jsonify({'status': 'success'}), 200
    except: 
        return jsonify({'status': 'failure'}), 500


# SECTION 10: USER LOGIN VERIFICATION PATHS

@app.route('/api/login', methods=['POST'])
def bypass_login_check(): return jsonify({'status': 'success'}), 200
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

def start_compliance_analytics_monitor():
    init_db()
    threading.Thread(target=run_rule_engine_scheduler_loop, daemon=True).start()
    print("SERVER STARTED FRESH ON PORT 5001")
    sys.stdout.flush()
    app.run(host='127.0.0.1', port=5001, debug=False, use_reloader=False)

if __name__ == '__main__':
    start_compliance_analytics_monitor()
