from bs4 import BeautifulSoup

file_handler = open('modified_sms_v2.xml')
data = BeautifulSoup(file_handler, 'lxml-xml')

# Get all messages
all_sms = data.find_all('sms')

#Categorizing messages
received_messages = [] #received mobile money
payment_messages = [] #payment using mobile money
transfer_messages = [] #mobile money transfers
deposit_messages = [] #bank deposit
airtime_messages = [] #airtime
cash_power_messages = [] #cash power
external_transaction_messages = [] #third party transactions
withdrawal_messages = [] #witdrawals
bundle_messages = [] #voice and data bundles
bank_transfer_messages = [] #bank transfers
uncategorized_messages = [] #messages without categories

for sms in all_sms:
    body = sms['body'].lower()
    if 'received' in body:
        received_messages.append(body)
    elif 'transferred' in body and 'financial transaction id' in body:
        bank_transfer_messages.append(body)
    elif 'transferred' in body and 'financial transaction id' not in body:
        transfer_messages.append(body)
    elif 'bundle' in body:
        bundle_messages.append(body)
    elif 'cash power' in body:
        cash_power_messages.append(body)
    elif 'airtime' in body and 'bundle' not in body:
        airtime_messages.append(body)
    elif 'external transaction' in body or 'external transaction id' in body and 'bundle' not in body and 'payment' not in body:
        external_transaction_messages.append(body)
    elif 'bank deposit' in body:
        deposit_messages.append(body)
    elif 'payment' in body and 'external transaction' not in body:
        payment_messages.append(body)
    elif 'withdrawn' in body:
        withdrawal_messages.append(body)
    else:
        uncategorized_messages.append(body)
    
        
# Writing results to separate text files   
with open('../separate_files/received_messages.txt', 'w', encoding='utf-8') as f:
    for msg in received_messages:
        f.write(f"{msg}\n")

with open('../separate_files/payment_messages.txt', 'w', encoding='utf-8') as f:
    for msg in payment_messages:
        f.write(f"{msg}\n")

with open('../separate_files/transfer_messages.txt', 'w', encoding='utf-8') as f:
    for msg in transfer_messages:
        f.write(f"{msg}\n")

with open('../separate_files/deposit_messages.txt', 'w', encoding='utf-8') as f:
    for msg in deposit_messages:
        f.write(f"{msg}\n")

with open('../separate_files/airtime_messages.txt', 'w', encoding='utf-8') as f:
    for msg in airtime_messages:
        f.write(f"{msg}\n")

with open('../separate_files/cash_power_messages.txt', 'w', encoding='utf-8') as f:
    for msg in cash_power_messages:
        f.write(f"{msg}\n")

with open('../separate_files/external_transaction_messages.txt', 'w', encoding='utf-8') as f:
    for msg in external_transaction_messages:
        f.write(f"{msg}\n")

with open('../separate_files/withdrawal_messages.txt', 'w', encoding='utf-8') as f:
    for msg in withdrawal_messages:
        f.write(f"{msg}\n")

with open('../separate_files/bundle_messages.txt', 'w', encoding='utf-8') as f:
    for msg in bundle_messages:
        f.write(f"{msg}\n")
        
with open('../separate_files/bank_transfer_messages.txt', 'w', encoding='utf-8') as f:
    for msg in bank_transfer_messages:
        f.write(f"{msg}\n")
        
with open('../separate_files/uncategorized_messages.txt', 'w', encoding='utf-8') as f:
    for msg in uncategorized_messages:
        f.write(f"{msg}\n")