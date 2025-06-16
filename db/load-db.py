import sqlite3
import json
import os
from datetime import datetime

class JSONToSQLite:
    def __init__(self, db="transactions.db"):
        self.db = db
        self.create_database()
    
    def create_database(self):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        # Creating all tables
        tables = {
            'received_transactions': '''
                CREATE TABLE IF NOT EXISTS received_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    sender_name VARCHAR(255),
                    sender_phone VARCHAR(20),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    financial_transaction_id VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'payment_transactions': '''
                CREATE TABLE IF NOT EXISTS payment_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    recipient_name VARCHAR(255),
                    recipient_id VARCHAR(50),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    txid VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'transfer_transactions': '''
                CREATE TABLE IF NOT EXISTS transfer_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    recipient_name VARCHAR(255),
                    recipient_phone VARCHAR(20),
                    sender_account VARCHAR(50),
                    timestamp DATETIME,
                    fee DECIMAL(10,2),
                    new_balance DECIMAL(15,2),
                    raw_message TEXT
                )
            ''',
            'bank_transfer_transactions': '''
                CREATE TABLE IF NOT EXISTS bank_transfer_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    recipient_name VARCHAR(255),
                    recipient_phone VARCHAR(20),
                    sender_account VARCHAR(50),
                    bank VARCHAR(100),
                    timestamp DATETIME,
                    financial_transaction_id VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'bank_deposit_transactions': '''
                CREATE TABLE IF NOT EXISTS bank_deposit_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    deposit_reference TEXT,
                    raw_message TEXT
                )
            ''',
            'airtime_transactions': '''
                CREATE TABLE IF NOT EXISTS airtime_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    txid VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'bundle_transactions': '''
                CREATE TABLE IF NOT EXISTS bundle_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    service VARCHAR(100),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    txid VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'cash_power_transactions': '''
                CREATE TABLE IF NOT EXISTS cash_power_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    token VARCHAR(100),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    txid VARCHAR(50),
                    raw_message TEXT
                )
            ''',
            'external_transactions': '''
                CREATE TABLE IF NOT EXISTS external_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    merchant VARCHAR(255),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    financial_transaction_id VARCHAR(50),
                    external_transaction_id VARCHAR(100),
                    raw_message TEXT
                )
            ''',
            'withdrawal_transactions': '''
                CREATE TABLE IF NOT EXISTS withdrawal_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    amount DECIMAL(15,2),
                    currency VARCHAR(10),
                    customer_name VARCHAR(255),
                    customer_phone VARCHAR(20),
                    agent_name VARCHAR(255),
                    agent_phone VARCHAR(20),
                    account VARCHAR(50),
                    timestamp DATETIME,
                    new_balance DECIMAL(15,2),
                    fee DECIMAL(10,2),
                    financial_transaction_id VARCHAR(50),
                    raw_message TEXT
                )
            '''
        }
        
        for table_name, create_sql in tables.items():
            cursor.execute(create_sql)
        
        conn.commit()
        conn.close()
    
    def parse_timestamp(self, timestamp_str):
        if not timestamp_str:
            return None
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%d-%m-%Y %H:%M'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue
        
        return None
    
    def load_received_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO received_transactions 
            (amount, currency, sender_name, sender_phone, timestamp, 
             new_balance, financial_transaction_id, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('sender_name'),
                record.get('sender_phone'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('financial_transaction_id'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_payment_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO payment_transactions 
            (amount, currency, recipient_name, recipient_id, timestamp, 
             new_balance, txid, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('recipient_name'),
                record.get('recipient_id'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('txid'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_transfer_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO transfer_transactions 
            (amount, currency, recipient_name, recipient_phone, sender_account, 
             timestamp, fee, new_balance, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('recipient_name'),
                record.get('recipient_phone'),
                record.get('sender_account'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('fee'),
                record.get('new_balance'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_bank_transfer_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO bank_transfer_transactions 
            (amount, currency, recipient_name, recipient_phone, sender_account, 
             bank, timestamp, new_balance, message_from_sender, message_to_receiver, 
             financial_transaction_id, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('recipient_name'),
                record.get('recipient_phone'),
                record.get('sender_account'),
                record.get('bank'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('message_from_sender'),
                record.get('message_to_receiver'),
                record.get('financial_transaction_id'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_bank_deposit_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO bank_deposit_transactions 
            (amount, currency, timestamp, new_balance, deposit_reference, raw_message)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('deposit_reference'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_airtime_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO airtime_transactions 
            (amount, currency, timestamp, new_balance, txid, raw_message)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('txid'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_bundle_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO bundle_transactions 
            (amount, currency, service, timestamp, new_balance, txid, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('service'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('txid'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_cash_power_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO cash_power_transactions 
            (amount, currency, token, timestamp, new_balance, txid, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('token'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('txid'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_external_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO external_transactions 
            (amount, currency, merchant, timestamp, new_balance, 
            financial_transaction_id, external_transaction_id, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('merchant'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('financial_transaction_id'),
                record.get('external_transaction_id'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_withdrawal_transactions(self, data):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        
        insert_sql = '''
            INSERT INTO withdrawal_transactions 
            (amount, currency, customer_name, customer_phone, agent_name, 
             agent_phone, account, timestamp, new_balance, fee, 
             financial_transaction_id, raw_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        for record in data:
            values = (
                record.get('amount'),
                record.get('currency'),
                record.get('customer_name'),
                record.get('customer_phone'),
                record.get('agent_name'),
                record.get('agent_phone'),
                record.get('account'),
                self.parse_timestamp(record.get('timestamp')),
                record.get('new_balance'),
                record.get('fee'),
                record.get('financial_transaction_id'),
                record.get('raw_message')
            )
            cursor.execute(insert_sql, values)
        
        conn.commit()
        conn.close()
    
    def load_json_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
        
    
    def load_all_data(self, json_directory="parsed_json"):
        files = {
            'received_parsed.json': self.load_received_transactions,
            'payment_parsed.json': self.load_payment_transactions,
            'transfer_parsed.json': self.load_transfer_transactions,
            'bank_transfer_parsed.json': self.load_bank_transfer_transactions,
            'bank_deposit_parsed.json': self.load_bank_deposit_transactions,
            'airtime_parsed.json': self.load_airtime_transactions,
            'bundle_parsed.json': self.load_bundle_transactions,
            'cash_power_parsed.json': self.load_cash_power_transactions,
            'external_transaction_parsed.json': self.load_external_transactions,
            'withdrawal_parsed.json': self.load_withdrawal_transactions
        }
        
        total_loaded = 0
        
        for filename, loader_method in files.items():
            file_path = os.path.join(json_directory, filename)
            if os.path.exists(file_path):
                data = self.load_json_file(file_path)
                if data:
                    loader_method(data)
                    total_loaded += len(data)
        return total_loaded

if __name__ == "__main__":
    loader = JSONToSQLite("transactions.db")
    
    # Load all JSON data
    total_records = loader.load_all_data("../separate_files/parsed_json")