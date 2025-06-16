import re
import json
import os

class SMSParser:
    def __init__(self):
        self.patterns = {
            'received': [
                r'you have received (\d+(?:,\d+)*) (\w+) from (.+?) \(\*+(\d+)\) on your mobile money account at ([\d-]+ [\d:]+)\. message from sender:. your new balance:(\d+(?:,\d+)*) (\w+)\. financial transaction id: (\d+)',
            ],
            'payment': [
                r'txid: (\d+)\. your payment of ([\d,]+) (\w+) to (.+?) (\w+) has been completed at ([\d-]+ [\d:]+)\. your new balance: ([\d,]+) (\w+)\. fee was 0 (\w+)',
            ],
            'transfer': [
                r'\*165\*s\*(\d+(?:,\d+)*) (\w+) transferred to (.+?) \((\d+)\) from (\d+) at ([\d-]+ [\d:]+) \. fee was: (\d+) (\w+)\. new balance: (\d+(?:,\d+)*) (\w+)',
            ],
            'bank_transfer': [
                r'you have transferred (\d+(?:,\d+)*) (\w+) to (.+?) \((\d+)\) from your mobile money account (\w+) (.+?) at ([\d-]+ [\d:]+)\. your new balance: (.*?)\. message from sender: (.*?)\. message to receiver: (.*?)\. financial transaction id: (\d+)',
            ],
            'bank_deposit': [
                r'\*113\*r\*a bank deposit of (\d+(?:,\d+)*) (\w+) has been added to your mobile money account at ([\d-]+ [\d:]+)\. your new balance :(\d+(?:,\d+)*) (\w+)\. (.*?)\.',
            ],
            'airtime': [
                r'\*162\*txid:(\d+)\*s\*your payment of (\d+(?:,\d+)*) (\w+) to airtime with token .* has been completed at ([\d-]+ [\d:]+)\. fee was 0 (\w+)\. your new balance: (\d+(?:,\d+)*) (\w+)',
            ],
            'bundle': [
                r'\*162\*txid:(\d+)\*s\*your payment of (\d+(?:,\d+)*) (\w+) to (bundles and packs|bundle) with token .* has been completed at ([\d-]+ [\d:]+)\. fee was 0 (\w+)\. your new balance: (\d+(?:,\d+)*) (\w+)',
            ],
            'cash_power': [
                r'\*162\*txid:(\d+)\*s\*your payment of (\d+(?:,\d+)*) (\w+) to mtn cash power with token (.+?) has been completed at ([\d-]+ [\d:]+)\. fee was 0 (\w+)\. your new balance: (\d+(?:,\d+)*) (\w+)',
            ],
            'external_transaction': [
                r'\*164\*s\*y\'ello,a transaction of (\d+(?:,\d+)*) (\w+) by (.+?) on your momo account was successfully completed at ([\d-]+ [\d:]+)\. message from debit receiver: (.*?)\. your new balance:(\d+(?:,\d+)*) (\w+)\. fee was 0 (\w+)\. financial transaction id: (\d+)\. external transaction id: (.+?)\.',
            ],
            'withdrawal': [
                r'you (.+?) \(\*+(\d+)\) have via agent: (.+?) \((\d+)\), withdrawn (\d+(?:,\d+)*) (\w+) from your mobile money account: (\d+) at ([\d-]+ [\d:]+) .* your new balance: (\d+(?:,\d+)*) (\w+)\. fee paid: (\d+) (\w+)\. message from agent: financial transaction id: (\d+)',
            ]
        }
    
    def clean_amount(self, amount_str):
        return float(amount_str.replace(',', ''))
    
    # Parsing methods for each transaction category
    def parse_received(self, match):
        return {
            'transaction_type': 'received',
            'amount': self.clean_amount(match.group(1)),
            'currency': match.group(2).upper(),
            'sender_name': match.group(3).strip(),
            'sender_phone': match.group(4),
            'timestamp': match.group(5),
            'new_balance': self.clean_amount(match.group(6)),
            'financial_transaction_id': match.group(8)
        }
    
    def parse_payment(self, match):
        return {
            'transaction_type': 'payment',
            'txid': match.group(1),
            'amount': self.clean_amount(match.group(2)),
            'currency': match.group(3).upper(),
            'recipient_name': match.group(4).strip(),
            'recipient_id': match.group(5),
            'timestamp': match.group(6),
            'new_balance': self.clean_amount(match.group(7)),
        }
    
    def parse_transfer(self, match):
        return {
            'transaction_type': 'transfer',
            'amount': self.clean_amount(match.group(1)),
            'currency': match.group(2).upper(),
            'recipient_name': match.group(3).strip(),
            'recipient_phone': match.group(4),
            'sender_account': match.group(5),
            'timestamp': match.group(6),
            'fee': float(match.group(7)),
            'new_balance': self.clean_amount(match.group(9))
        }
    
    def parse_bank_transfer(self, match):
        return {
            'transaction_type': 'bank_transfer',
            'amount': self.clean_amount(match.group(1)),
            'currency': match.group(2).upper(),
            'recipient_name': match.group(3).strip(),
            'recipient_phone': match.group(4),
            'sender_account': match.group(5),
            'bank': match.group(6).strip(),
            'timestamp': match.group(7),
            'financial_transaction_id': match.group(11)
        }
    
    def parse_bank_deposit(self, match):
        return {
            'transaction_type': 'bank_deposit',
            'amount': self.clean_amount(match.group(1)),
            'currency': match.group(2).upper(),
            'timestamp': match.group(3),
            'new_balance': self.clean_amount(match.group(4)),
            'deposit_reference': match.group(6).strip()
        }
    
    def parse_airtime(self, match):
        return {
            'transaction_type': 'airtime',
            'txid': match.group(1),
            'amount': self.clean_amount(match.group(2)),
            'currency': match.group(3).upper(),
            'timestamp': match.group(4),
            'new_balance': self.clean_amount(match.group(6))
        }
    
    def parse_bundle(self, match):
        return {
            'transaction_type': 'bundle',
            'txid': match.group(1),
            'amount': self.clean_amount(match.group(2)),
            'currency': match.group(3).upper(),
            'service': match.group(4),
            'timestamp': match.group(5),
            'new_balance': self.clean_amount(match.group(7))
        }
    
    def parse_cash_power(self, match):
        return {
            'transaction_type': 'cash_power',
            'txid': match.group(1),
            'amount': self.clean_amount(match.group(2)),
            'currency': match.group(3).upper(),
            'token': match.group(4).strip(),
            'timestamp': match.group(5),
            'new_balance': self.clean_amount(match.group(7))
        }
    
    def parse_external_transaction(self, match):
        return {
            'transaction_type': 'external_transaction',
            'amount': self.clean_amount(match.group(1)),
            'currency': match.group(2).upper(),
            'merchant': match.group(3).strip(),
            'timestamp': match.group(4),
            'new_balance': self.clean_amount(match.group(6)),
            'financial_transaction_id': match.group(9),
            'external_transaction_id': match.group(10).strip()
        }
    
    def parse_withdrawal(self, match):
        return {
            'transaction_type': 'withdrawal',
            'customer_name': match.group(1).strip(),
            'customer_phone': match.group(2),
            'agent_name': match.group(3).strip(),
            'agent_phone': match.group(4),
            'amount': self.clean_amount(match.group(5)),
            'currency': match.group(6).upper(),
            'account': match.group(7),
            'timestamp': match.group(8),
            'new_balance': self.clean_amount(match.group(9)),
            'fee': float(match.group(11)),
            'financial_transaction_id': match.group(13)
        }
    
    
    
    def parse_file(self, file_path, transaction_type):
        parsed_data = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
                
            for pattern in self.patterns.get(transaction_type, []):
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                        parser_method = getattr(self, f'parse_{transaction_type}')
                        data = parser_method(match)
                        data['raw_message'] = line
                        data['line_number'] = line_num
                        parsed_data.append(data)
                        break
            
        return parsed_data
    
    def parse_all_files(self, input_directory, output_directory):
        #Parse all transaction files and save to JSON
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)
        
        files = {
            'received_messages.txt': 'received',
            'payment_messages.txt': 'payment',
            'transfer_messages.txt': 'transfer',
            'bank_transfer_messages.txt': 'bank_transfer',
            'deposit_messages.txt': 'bank_deposit',
            'airtime_messages.txt': 'airtime',
            'bundle_messages.txt': 'bundle',
            'cash_power_messages.txt': 'cash_power',
            'external_transaction_messages.txt': 'external_transaction',
            'withdrawal_messages.txt': 'withdrawal'
        }
        
        for filename, transaction_type in files.items():
            file_path = os.path.join(input_directory, filename)
            if os.path.exists(file_path):
                parsed_data = self.parse_file(file_path, transaction_type)
                
                output_file = os.path.join(output_directory, f'{transaction_type}_parsed.json')
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(parsed_data, f, indent=2)

if __name__ == "__main__":
    parser = SMSParser()
    
    input_dir = "."  # Current directory
    output_dir = "parsed_json"  # Directory to save JSON files
    
    summary = parser.parse_all_files(input_dir, output_dir)