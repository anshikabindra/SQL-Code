import os

# db_config = {
#    "user": os.environ["DB_USER"],
#    "password": os.environ["DB_PASS"],
#    "database": os.environ["DB_NAME"],
#    "unix_socket": f"/cloudsql/{os.environ['INSTANCE_CONNECTION_NAME']}"
# }

# Added jsonify to the Flask import line to support chatbot responses
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
import mysql.connector
from mysql.connector import Error
import csv
import io
from werkzeug.security import generate_password_hash, check_password_hash

from Category_Mapping import category_mapping_bp
from bank_transaction import bank_bp
from fd_transactions import fd_bp
from Equity import equity_bp
from mf_transactions import mf_bp
from pf_transactions import pf_bp
from gold import gold_bp
from real_estate import real_estate_bp
from cash import cash_bp
from private_equity import private_equity_bp

app = Flask(__name__)
app.secret_key = 'supersecretkey'

# db_config = {
#     'user': 'root',
#     'password': 'Anshika',
#     'host': '127.0.0.1',
#     'port': '3306',
#     'database': 'portfolioManagement'
# }

# --- NEW AIVEN CLOUD DB CONFIG --- #
db_config = {
    'user': 'avnadmin',
    'password': 'AVNS_SRtc5d4cDCrezjU_70x',
    'host': 'portfolio-db-bindraanshika-32d.i.aivencloud.com',
    'port': '26174',
    'database': 'defaultdb',
    'ssl_disabled': False  # Aiven requires SSL connection
}


# ---------------- DATABASE INITIALIZATION ---------------- #
def init_db():
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # 1. Users Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password VARCHAR(255) NOT NULL
            )
        """)

        # 2. Bank Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bank_transaction (
                id INT AUTO_INCREMENT PRIMARY KEY,
                DT DATE,
                Narration TEXT,
                CHq_Ref_No VARCHAR(100),
                Value_Dt DATE,
                Withdrawal_Amt DECIMAL(15, 2),
                Deposit_Amt DECIMAL(15, 2),
                Closing_Balance DECIMAL(15, 2),
                bank_name VARCHAR(100),
                user_id INT
            )
        """)

        # 3. FD Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fd_transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Portfolio_name VARCHAR(100),
                FD_number VARCHAR(100),
                Principal_amt DECIMAL(15, 2),
                Start_date DATE,
                Rate DECIMAL(5, 2),
                Opening_Balance DECIMAL(15, 2),
                Current_value DECIMAL(15, 2),
                Maturity_date DATE,
                user_id INT
            )
        """)

        # 4. Equity Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS equity_transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Company_name VARCHAR(255),
                ISIN_number VARCHAR(100),
                Transaction_date DATE,
                Transaction_rate DECIMAL(15, 2),
                Quantity INT,
                Transaction_type VARCHAR(50),
                user_id INT
            )
        """)

        # 5. Mutual Fund Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Mutual_Fund_transactions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Company_name VARCHAR(255),
                ISIN_number VARCHAR(100),
                Transaction_date DATE,
                Transaction_rate DECIMAL(15, 2),
                Quantity INT,
                Transaction_type VARCHAR(50),
                user_id INT
            )
        """)

        # 6. PF Transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pf (
                id INT AUTO_INCREMENT PRIMARY KEY,
                DT DATE,
                Narration TEXT,
                CHq_Ref_No VARCHAR(100),
                Value_Dt DATE,
                Withdrawal_Amt DECIMAL(15, 2),
                Deposit_Amt DECIMAL(15, 2),
                Closing_Balance DECIMAL(15, 2),
                user_id INT
            )
        """)

        # 7. Gold Investments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS gold_investments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                investment_name VARCHAR(255),
                value_per_gram DECIMAL(15, 2),
                quantity DECIMAL(15, 2),
                investment_date DATE,
                user_id INT
            )
        """)

        # 8. Real Estate
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS real_estate (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Investment_name VARCHAR(255),
                Invested_value DECIMAL(15, 2),
                Date_of_investment DATE,
                Current_value DECIMAL(15, 2),
                user_id INT
            )
        """)

        # 9. Cash Investments
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cash_investments (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Investment_name VARCHAR(255),
                Invested_value_per_share DECIMAL(15, 2),
                No_of_units INT,
                Date_of_investment DATE,
                Current_value_per_share DECIMAL(15, 2),
                user_id INT
            )
        """)

        # 10. Private Equity
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS private_equity (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Investment_name VARCHAR(255),
                Invested_value_per_share DECIMAL(15, 2),
                No_of_shares_issued INT,
                Date_of_investment DATE,
                Current_value_per_share DECIMAL(15, 2),
                user_id INT
            )
        """)

        # 11. Category Mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Category_Mapping (
                id INT AUTO_INCREMENT PRIMARY KEY,
                Category VARCHAR(100),
                Description TEXT,
                Sub_Category VARCHAR(100),
                user_id INT
            )
        """)

        conn.commit()
        cursor.close()
        print("Database tables initialized successfully!")
    except Error as e:
        print(f"Error initializing database: {e}")
    finally:
        if conn: conn.close()


# ---------------- AUTH ---------------- #

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email'].lower()
        password = request.form['password']
        hashed_password = generate_password_hash(password)

        conn = cursor = None
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()

            cursor.execute(
                "INSERT INTO users (email, password) VALUES (%s, %s)",
                (email, hashed_password)
            )
            conn.commit()

            # This triggers the green "Successfully Registered" message on the login page
            flash('Successfully Registered! Please login.', 'success')
            return redirect(url_for('login'))

        except mysql.connector.IntegrityError:
            # Added 'error' category for red styling
            flash('Email already exists.', 'error')
        except Error as e:
            # Added 'error' category for red styling
            flash(f"Database error: {e}", 'error')

        finally:
            if cursor: cursor.close()
            if conn: conn.close()

        return redirect(url_for('register'))

    return render_template('register.html')


@app.route('/', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':

        email = request.form['email'].lower()
        password = request.form['password']

        conn = cursor = None
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT * FROM users WHERE email=%s", (email,))
            user = cursor.fetchone()

            if user:
                stored_password = user['password']
                valid = False

                if stored_password and stored_password.startswith(("scrypt:", "pbkdf2:")):
                    valid = check_password_hash(stored_password, password)

                elif stored_password == password:
                    valid = True

                    # Upgrade to hashed password
                    new_hash = generate_password_hash(password)
                    cursor.execute(
                        "UPDATE users SET password=%s WHERE id=%s",
                        (new_hash, user['id'])
                    )
                    conn.commit()

                if valid:
                    session['user'] = email
                    session['user_id'] = user['id']
                    return redirect(url_for('dashboard'))

            flash('Invalid email or password')

        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    return render_template('login.html')


@app.route('/dashboard')
def dashboard():
    if 'user' not in session:
        return redirect(url_for('login'))

    return render_template('dashboard.html', transactions=None, title=None)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# ---------------- UNIFIED CONTEXT PROCESSOR ---------------- #

@app.context_processor
def inject_active_assets():
    active_assets = []
    gold_investments = []
    real_estate = []
    cash = []
    private_equity = []

    if 'user_id' in session:
        conn = cursor = None
        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor(dictionary=True)
            user_id = session['user_id']

            # Check Gold
            cursor.execute("SELECT * FROM gold_investments WHERE user_id = %s", (user_id,))
            gold_investments = cursor.fetchall()
            if gold_investments: active_assets.append('Gold')

            # Check Real Estate
            cursor.execute("SELECT * FROM real_estate WHERE user_id = %s", (user_id,))
            real_estate = cursor.fetchall()
            if real_estate: active_assets.append('Real Estate')

            # Check Cash
            cursor.execute("SELECT * FROM cash_investments WHERE user_id = %s", (user_id,))
            cash = cursor.fetchall()
            if cash: active_assets.append('Cash')

            # Check Private Equity
            cursor.execute("SELECT * FROM private_equity WHERE user_id = %s", (user_id,))
            private_equity = cursor.fetchall()
            if private_equity: active_assets.append('Private Equity')

        except Exception:
            pass
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    return dict(
        active_assets=active_assets,
        gold_investments=gold_investments,
        gold_exists=len(gold_investments) > 0,
        real_estate=real_estate,
        real_estate_exists=len(real_estate) > 0,
        cash=cash,
        cash_exists=len(cash) > 0,
        private_equity=private_equity,
        private_equity_exists=len(private_equity) > 0
    )


# ---------------- CSV UPLOAD ---------------- #

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    if 'user' not in session:
        return redirect(url_for('login'))

    file = request.files.get('csv_file')
    table_name_raw = request.form.get('table_name', '').strip()

    table_map = {
        'bank transactions': 'bank_transaction',
        'fd transactions': 'fd_transactions',
        'equity transactions': 'equity_transactions',
        'mutual fund transactions': 'Mutual_Fund_transactions',
        'pf transactions': 'pf',
        'category mapping': 'Category_Mapping',
        'gold investments': 'gold_investments',
        'real estate': 'real_estate',
        'private equity': 'private_equity',
        'cash investments': 'cash_investments'
    }

    table_name = table_map.get(table_name_raw.lower())

    if not file or not table_name:
        flash('Error uploading CSV file', 'error')
        return redirect(request.referrer)

    conn = cursor = None

    try:
        stream = io.StringIO(file.stream.read().decode("utf-8-sig"))
        reader = csv.DictReader(stream)
        csv_headers = [h.strip().lower() for h in reader.fieldnames] if reader.fieldnames else []

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Get Database columns
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        columns_info = cursor.fetchall()
        db_columns = [col['Field'].lower() for col in columns_info]

        # Columns that MUST be in the CSV (excluding auto-managed fields)
        required_db_columns = [col for col in db_columns if col not in ['id', 'user_id']]

        # VALIDATION: Ensure CSV headers match the required DB columns exactly
        is_match = True
        if len(csv_headers) != len(required_db_columns):
            is_match = False
        else:
            for header in csv_headers:
                if header not in required_db_columns:
                    is_match = False
                    break

        if not is_match:
            flash("File doesn't match columns", "error")
            return redirect(request.referrer)

        # Process insertion if columns match
        insert_columns = [h for h in reader.fieldnames if h.strip().lower() in db_columns]
        if 'user_id' in db_columns:
            insert_columns.append('user_id')

        placeholders = ", ".join(["%s"] * len(insert_columns))
        column_names = ", ".join(insert_columns)
        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        for row in reader:
            values = [
                session.get('user_id') if col == 'user_id' else row.get(col)
                for col in insert_columns
            ]
            cursor.execute(query, values)

        conn.commit()
        flash('File uploaded successfully', 'success')

    except Exception as e:
        print(f"CSV Upload Error: {e}")
        flash('Error uploading CSV file', 'error')

    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    return redirect(request.referrer)


import re


# ---------------- NATURAL LANGUAGE CHATBOT TERMINAL ---------------- #

@app.route('/chatbot', methods=['POST'])
def chatbot():
    if 'user_id' not in session:
        return jsonify({"reply": "Please log in to your account first so I can access your secure ledger."})

    data = request.get_json() or {}
    user_message = data.get('message', '').strip()
    user_message_lower = user_message.lower()
    user_id = session['user_id']

    # Blueprint table structure mapping reference
    table_map = {
        'bank': 'bank_transaction',
        'fd': 'fd_transactions',
        'equity': 'equity_transactions',
        'mutual fund': 'Mutual_Fund_transactions',
        'mf': 'Mutual_Fund_transactions',
        'pf': 'pf',
        'gold': 'gold_investments',
        'real estate': 'real_estate',
        'cash': 'cash_investments',
        'private equity': 'private_equity'
    }

    # Find which asset class the user is talking about
    target_asset = None
    for key in table_map.keys():
        if key in user_message_lower:
            target_asset = key
            break

    # --- 1. NATURAL LANGUAGE DETECTOR: DELETE ---
    # Matches: "delete ATM Run from bank", "remove rent from pf", "delete this transaction from bank: Gym"
    if any(w in user_message_lower for w in ['delete', 'remove', 'drop', 'clear']):
        if not target_asset:
            return jsonify({
                               "reply": "I can help you delete that! Which asset or category (Bank, Gold, Equity, etc.) should I remove it from?"})

        table_name = table_map[target_asset]

        # Determine identifying column based on asset types
        ident_column = 'investment_name' if target_asset in ['gold', 'real estate', 'cash',
                                                             'private_equity'] else 'Narration'
        if target_asset in ['equity', 'mutual fund', 'mf']:
            ident_column = 'Company_name'
        elif target_asset == 'fd':
            ident_column = 'Portfolio_name'

        # Extract the target value by cleaning out the common phrasing keywords
        clean_text = re.sub(r'\b(delete|remove|drop|clear|from|this|transaction|asset|in|the|ledger)\b', '',
                            user_message_lower).replace(target_asset, '').strip()
        # Strip trailing punctuation if any
        clean_text = clean_text.strip("':\",.?!")

        if not clean_text:
            return jsonify({
                               "reply": f"Which specific transaction title or description would you like to delete from your {target_asset.upper()} records?"})

        try:
            conn = sql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
            cursor = conn.cursor()

            # Search securely for transactions using a flexible LIKE match under your account profile
            query = f"DELETE FROM {table_name} WHERE LOWER({ident_column}) LIKE %s AND user_id = %s"
            cursor.execute(query, (f"%{clean_text}%", user_id))
            conn.commit()
            affected = cursor.rowcount
            cursor.close()
            conn.close()

            if affected > 0:
                return jsonify({
                                   "reply": f"✅ Successfully deleted entries matching '<strong>{clean_text}</strong>' from your {target_asset.upper()} ledger rows."})
            else:
                return jsonify({
                                   "reply": f"❌ Couldn't find any records matching '{clean_text}' inside your active {target_asset.upper()} list."})
        except Exception as e:
            return jsonify({"reply": f"❌ Error executing transaction drop: {str(e)}"})

    # --- 2. NATURAL LANGUAGE DETECTOR: MODIFY/CHANGE ---
    # Matches: "change salary to bonus in bank", "modify bank by changing rent to utilities", "change tech stocks to apple in equity"
    elif any(w in user_message_lower for w in ['change', 'modify', 'update', 'correct']):
        if not target_asset:
            return jsonify({
                               "reply": "I see you want to modify a transaction. Which asset class or category tab is it located under?"})

        table_name = table_map[target_asset]

        ident_column = 'investment_name' if target_asset in ['gold', 'real estate', 'cash',
                                                             'private_equity'] else 'Narration'
        if target_asset in ['equity', 'mutual fund', 'mf']:
            ident_column = 'Company_name'
        elif target_asset == 'fd':
            ident_column = 'Portfolio_name'

        # Look for natural transitions like "from X to Y" or "X to Y"
        match_from_to = re.search(r'(?:changing|change|from)?\s*(.*?)\s+to\s+(.*)', user_message, re.IGNORECASE)

        if match_from_to:
            old_val = match_from_to.group(1).strip()
            new_val = match_from_to.group(2).strip()

            # Clean up trailing words like "in bank", "from gold" from the extracted new value string
            new_val = re.sub(r'\b(in|from|under|inside|asset|the|ledger)\b.*', '', new_val, flags=re.IGNORECASE).strip()
            old_val = re.sub(r'\b(this|transaction|the)\b', '', old_val, flags=re.IGNORECASE).strip()

            # Strip punctuation handles
            old_val = old_val.strip("':\",.?!")
            new_val = new_val.strip("':\",.?!")

            try:
                conn = sql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
                cursor = conn.cursor()

                query = f"UPDATE {table_name} SET {ident_column} = %s WHERE LOWER({ident_column}) LIKE %s AND user_id = %s"
                cursor.execute(query, (new_val, f"%{old_val.lower()}%", user_id))
                conn.commit()
                affected = cursor.rowcount
                cursor.close()
                conn.close()

                if affected > 0:
                    return jsonify({
                                       "reply": f"✅ Successfully updated your {target_asset.upper()} record title from '<strong>{old_val}</strong>' to '<strong>{new_val}</strong>'."})
                else:
                    return jsonify({
                                       "reply": f"❌ Couldn't locate a transaction matching '{old_val}' under your profile parameters."})
            except Exception as e:
                return jsonify({"reply": f"❌ Modification pipeline error: {str(e)}"})

        return jsonify({
                           "reply": "💡 Try phrasing modifications naturally like this: *'Change Salary to Bonus in Bank'* or *'Modify Equity by changing Google to Alphabet'*."})

    # --- 3. NATURAL LANGUAGE DETECTOR: TOTAL CALCULATION ---
    # Matches: "what is the total bank deposit_amt?", "total gold quantity"
    elif 'total' in user_message_lower:
        if not target_asset:
            return jsonify({"reply": "Which specific ledger or asset category would you like me to sum up totals for?"})

        table_name = table_map[target_asset]

        allowed_fields = [
            'withdrawal_amt', 'deposit_amt', 'closing_balance', 'principal_amt',
            'rate', 'opening_balance', 'current_value', 'transaction_rate',
            'quantity', 'value_per_gram', 'invested_value', 'invested_value_per_share',
            'no_of_units', 'current_value_per_share', 'no_of_shares_issued'
        ]

        field_casing_map = {
            'withdrawal_amt': 'Withdrawal_Amt', 'deposit_amt': 'Deposit_Amt', 'closing_balance': 'Closing_Balance',
            'principal_amt': 'Principal_amt', 'rate': 'Rate', 'opening_balance': 'Opening_Balance',
            'current_value': 'Current_value', 'transaction_rate': 'Transaction_rate', 'quantity': 'Quantity',
            'value_per_gram': 'value_per_gram', 'invested_value': 'Invested_value',
            'invested_value_per_share': 'Invested_value_per_share', 'no_of_units': 'No_of_units',
            'current_value_per_share': 'Current_value_per_share', 'no_of_shares_issued': 'No_of_shares_issued'
        }

        detected_field = None
        for f in allowed_fields:
            if f in user_message_lower:
                detected_field = f
                break

        if detected_field:
            actual_field = field_casing_map[detected_field]
            try:
                conn = sql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
                cursor = conn.cursor()
                query = f"SELECT SUM({actual_field}) FROM {table_name} WHERE user_id = %s"
                cursor.execute(query, (user_id,))
                result = cursor.fetchone()
                total_sum = result[0] if result[0] is not None else 0.00
                cursor.close()
                conn.close()

                return jsonify({
                                   "reply": f"📊 The calculated total for <strong>{detected_field.upper()}</strong> in your <strong>{target_asset.upper()}</strong> data matches: <strong>₹{total_sum:,.2f}</strong>"})
            except Exception as e:
                return jsonify({"reply": f"❌ Calculation parsing fault: {str(e)}"})
        else:
            return jsonify({
                               "reply": f"Which column property would you like computed? Try asking: *'Total bank deposit_amt'* or *'What is my total gold quantity?'*"})

    # --- 4. NATURAL LANGUAGE DETECTOR: ADDING ---
    # Matches: "add a bank transaction for groceries worth -1500", "add gold entry for coin worth 6400 with quantity 5"
    elif user_message_lower.startswith('add '):
        # Fallback to display example pattern if text doesn't contain numerical inputs
        numbers = re.findall(r'[-+]?\d*\.\d+|\d+', user_message)
        if not target_asset or not numbers:
            return jsonify({"reply": "💡 **To add an item, phrase it with numbers like this:**<br>"
                                     "• *'Add a bank transaction for Freelance Design worth 5000'*<br>"
                                     "• *'Add gold position for 24K Coin at rate 6200 and quantity 10'*"})

        table_name = table_map[target_asset]
        clean_narr = re.sub(
            r'\b(add|bank|gold|equity|mf|pf|transaction|entry|position|for|worth|at|rate|quantity|with|value)\b', '',
            user_message_lower).strip()
        clean_narr = re.sub(r'[-+]?\d*\.\d+|\d+', '', clean_narr).strip().strip("|,.-_")

        try:
            conn = sql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
            cursor = conn.cursor()

            if table_name in ['bank_transaction', 'pf']:
                amt = float(numbers[0])
                dep = amt if amt >= 0 else 0
                wit = abs(amt) if amt < 0 else 0
                cursor.execute(
                    f"INSERT INTO {table_name} (DT, Narration, Deposit_Amt, Withdrawal_Amt, Value_Dt, user_id) VALUES (CURDATE(), %s, %s, %s, CURDATE(), %s)",
                    (clean_narr.capitalize() or "Manual Entry", dep, wit, user_id)
                )
            elif table_name == 'gold_investments' and len(numbers) >= 2:
                rate = float(numbers[0])
                qty = float(numbers[1])
                cursor.execute(
                    "INSERT INTO gold_investments (investment_name, value_per_gram, quantity, investment_date, user_id) VALUES (%s, %s, %s, CURDATE(), %s)",
                    (clean_narr.capitalize() or "Gold Asset", rate, qty, user_id)
                )
            elif table_name in ['equity_transactions', 'Mutual_Fund_transactions'] and len(numbers) >= 2:
                rate = float(numbers[0])
                qty = int(numbers[1])
                tx_type = "Buy" if "sell" not in user_message_lower else "Sell"
                cursor.execute(
                    f"INSERT INTO {table_name} (Company_name, Transaction_date, Transaction_rate, Quantity, Transaction_type, user_id) VALUES (%s, CURDATE(), %s, %s, %s, %s)",
                    (clean_narr.upper() or "SECURITY", rate, qty, tx_type, user_id)
                )
            else:
                return jsonify({
                                   "reply": "Could you provide both the financial rate/amount and the specific quantities required for that operation?"})

            conn.commit()
            cursor.close()
            conn.close()
            return jsonify(
                {"reply": f"✅ Successfully added your custom entry to <strong>{target_asset.upper()}</strong>!"})

        except Exception as e:
            return jsonify({"reply": f"❌ Record generation layout fault: {str(e)}"})

    # Catch-all greeting / helper pattern response
    return jsonify({
                       "reply": "Hello! I am your helpful PortfolioHub assistant. How can I help you manage your financial tracking, asset visibility, or statement sheets today?"})


# ---------------- BLUEPRINTS ---------------- #

app.register_blueprint(bank_bp, url_prefix="/bank")
app.register_blueprint(fd_bp, url_prefix="/fd")
app.register_blueprint(equity_bp, url_prefix="/equity")
app.register_blueprint(mf_bp, url_prefix="/mf")
app.register_blueprint(pf_bp, url_prefix="/pf")
app.register_blueprint(category_mapping_bp, url_prefix="/category_mapping")
app.register_blueprint(gold_bp, url_prefix="/gold")
app.register_blueprint(real_estate_bp, url_prefix="/real_estate")
app.register_blueprint(cash_bp, url_prefix="/cash")
app.register_blueprint(private_equity_bp, url_prefix="/private_equity")

# ---------------- RUN ---------------- #

if __name__ == '__main__':
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
