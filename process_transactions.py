import os, json
import sqlite3

def drop_table():
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute("DROP TABLE IF EXISTS DEPOSITED_TXN")
	conn.commit()
	conn.close()

def create_table():
	conn = sqlite3.connect('transactions.db')
	conn.execute('''CREATE TABLE IF NOT EXISTS DEPOSITED_TXN
		(TXID TEXT PRIMARY KEY NOT NULL,
		ADDRESS TEXT NOT NULL,
		AMOUNT REAL NOT NULL,
		CONFIRMATIONS INT NOT NULL,
		BLOCKHASH TEXT,
		BLOCKINDEX INT,
		BLOCKTIME INT,
		VOUT INT,
		TX_TIME INT NOT NULL,
		TIME_RECEIVED INT NOT NULL,
        BIP_125_REPLACEABLE TEXT);''')
	#print("Table created successfully")
	conn.commit()
	conn.close()

def open_json_insert_to_db():
	#link describing the fields of the transactions https://bitcoincore.org/en/doc/0.16.0/rpc/wallet/listsinceblock/
	path_to_json = 'json_transaction_data/'
	json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
	unique_tx_list = [] 
	for json_file in json_files:
		file_name_with_path = path_to_json + json_file
		with open(file_name_with_path) as file:
			data = json.load(file)
			transactions = data['transactions']
			count = 0
			for transaction in transactions:
				if transaction['category'] == 'receive' and transaction['txid'] not in unique_tx_list: #transaction['confirmations'] >= 6 and
					unique_tx_list.append(transaction['txid'])
					count = count + 1
					#print(count) #count to display number of rows (records) inserted from json files
					insert_to_db(transaction['txid'],transaction['address'],transaction['amount'],transaction['confirmations'],transaction['blockhash'],transaction['blockindex'],transaction['blocktime'],transaction['vout'],transaction['time'],transaction['timereceived'],transaction['bip125-replaceable'])
				#elif transaction['category'] == 'receive' and transaction['txid'] in unique_tx_list:
					#here we will get the repeating transactions (in total there are 13 transactions that are repeating and all the feilds for them are the same {i.e. both the copies have same information})

def insert_to_db(txid, address, amount, confirmations, blockhash, blockindex, blocktime, vout, tx_time, timereceived, bip125):
	#helper function for open_json_insert_to_db() which opens the connection to the database and inserts a record to it
	conn = sqlite3.connect('transactions.db')
	conn.execute('INSERT INTO DEPOSITED_TXN (TXID, ADDRESS, AMOUNT, CONFIRMATIONS, BLOCKHASH, BLOCKINDEX, BLOCKTIME, VOUT, TX_TIME, TIME_RECEIVED, BIP_125_REPLACEABLE) \
		VALUES (?,?,?,?,?,?,?,?,?,?,?)', (txid, address, amount, confirmations, blockhash, blockindex, blocktime, vout, tx_time, timereceived, bip125));
	conn.commit()
	conn.close()	

def select_all_valid_deposits():
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute('SELECT COUNT(AMOUNT), SUM(AMOUNT) FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND AMOUNT > 0') #added AMOUNT > 0
	for row in cursor:
		result = {'count': row[0], 'total_amount': row[1]}
	conn.close()
	return result

def select_txs_for_address(address):
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute('SELECT COUNT(AMOUNT), SUM(AMOUNT) FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND ADDRESS = ? AND AMOUNT > 0',(address,)); #added AMOUNT > 0
	for row in cursor:
		result = {'count': row[0], 'total_amount': row[1]}
	conn.close()
	return result

def get_min():
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute('SELECT *, MIN(AMOUNT) FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND AMOUNT > 0') #added AMOUNT > 0
	#another query that gets the same result
	#conn.execute('SELECT * from DEPOSITED_TXN where AMOUNT = (SELECT min(AMOUNT) from DEPOSITED_TXN where CONFIRMATIONS >= 6)') 
	for row in cursor:
		min = row[2]
	conn.close()
	return min
	
def get_max():
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute('SELECT *, MAX(AMOUNT) FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6')
	#conn.execute('SELECT * from DEPOSITED_TXN where AMOUNT = (SELECT max(AMOUNT) from DEPOSITED_TXN where CONFIRMATIONS >= 6)')
	for row in cursor:
		max = row[2]
	conn.close()
	return max

def select_txs_except_address(addresses_to_be_omitted):
	"""
	This function takes list of addresses and creates a sql query by joining them and returns a dictionary containing the sum and count values. Currently it is not being used to calculate the
	sum and count for the addresses without reference. We need to pass it a list of customer addresses.
	"""
	query_string_place_holders = ''
	for x in range(len(addresses_to_be_omitted)):
		if x == (len(addresses_to_be_omitted)-1):
			query_string_place_holders = query_string_place_holders + "\'" + addresses_to_be_omitted[x]+ "\'" 
		else:
			query_string_place_holders = query_string_place_holders + "\'" + addresses_to_be_omitted[x]+ "\'" +", "
	query_string = 'SELECT * FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND  TXID NOT IN (' + query_string_place_holders + ')'
	#print(query_string)
	#SELECT * FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND  TXID NOT IN ('mvd6qFeVkqH6MNAS2Y2cLifbdaX5XUkbZJ', 'mmFFG4jqAtw9MoCC88hw5FNfreQWuEHADp', 'mzzg8fvHXydKs8j9D2a8t7KpSXpGgAnk4n', '2N1SP7r92ZZJvYKG2oNtzPwYnzw62up7mTo', 'mutrAf4usv3HKNdpLwVD4ow2oLArL6Rez8', 'miTHhiX3iFhVnAEecLjybxvV5g8mKYTtnM', 'mvcyJMiAcSXKAEsQxbW9TYZ369rsMG6rVV')
	
	"""
	for x in range(len(addresses_to_be_omitted)):
		if x == (len(addresses_to_be_omitted)-1):
			query_string_place_holders = query_string_place_holders + " TXID != \'"+ addresses_to_be_omitted[x] + "\'"
		else:
			query_string_place_holders = query_string_place_holders + "TXID != \'"+addresses_to_be_omitted[x]+"\'"+" OR "
	query_string = 'SELECT COUNT(AMOUNT), SUM(AMOUNT) FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND  (' + query_string_place_holders + ')'
	#print(query_string)
	#SELECT * FROM DEPOSITED_TXN WHERE CONFIRMATIONS >= 6 AND  (TXID != 'mvd6qFeVkqH6MNAS2Y2cLifbdaX5XUkbZJ' OR TXID != 'mmFFG4jqAtw9MoCC88hw5FNfreQWuEHADp' OR TXID != 'mzzg8fvHXydKs8j9D2a8t7KpSXpGgAnk4n' OR TXID != '2N1SP7r92ZZJvYKG2oNtzPwYnzw62up7mTo' OR TXID != 'mutrAf4usv3HKNdpLwVD4ow2oLArL6Rez8' OR TXID != 'miTHhiX3iFhVnAEecLjybxvV5g8mKYTtnM' OR  TXID != 'mvcyJMiAcSXKAEsQxbW9TYZ369rsMG6rVV')
	"""
	conn = sqlite3.connect('transactions.db')
	cursor = conn.execute(query_string);
	for row in cursor:
		result = {'count': row[0], 'total_amount': row[1]}
	conn.close()
	return result

if __name__ == "__main__":
	drop_table() #drop table if it already exists
	create_table() #create table if it does not exist
	open_json_insert_to_db() #open all the json files from folder json_transaction_data and inserts the ones with unique txid and category receive 
	result = select_all_valid_deposits()
	count_all_valid = result['count']
	amount_all_valid =  result['total_amount']
	
	customer_list = {
		'Wesley Crusher': 'mvd6qFeVkqH6MNAS2Y2cLifbdaX5XUkbZJ', 
		'Leonard McCoy': 'mmFFG4jqAtw9MoCC88hw5FNfreQWuEHADp', 
		'Jonathan Archer': 'mzzg8fvHXydKs8j9D2a8t7KpSXpGgAnk4n', 
		'Jadzia Dax': '2N1SP7r92ZZJvYKG2oNtzPwYnzw62up7mTo', 
		'Montgomery Scott': 'mutrAf4usv3HKNdpLwVD4ow2oLArL6Rez8', 
		'James T. Kirk': 'miTHhiX3iFhVnAEecLjybxvV5g8mKYTtnM', 
		'Spock': 'mvcyJMiAcSXKAEsQxbW9TYZ369rsMG6rVV'
	}
	count_customers = 0
	amount_customers = 0
	for key, value in customer_list.items():
		result = select_txs_for_address(value)
		amount_total = result['total_amount']
		count_customers = count_customers + result['count']
		amount_customers = amount_customers + result['total_amount']
		print("    Deposited for "+key+": count="+str(result['count'])+" sum="+f'{amount_total:.8f}')
	"""
	addresses_to_be_omitted = []
	for value in customer_list.values():
		addresses_to_be_omitted.append(value)
	result_1 = select_txs_except_address(addresses_to_be_omitted)
	amount_value = result_1['total_amount']
	print("Test    Deposited without reference: count="+str(result_1['count'])+" sum="+f'{amount_value:.8f}')
	"""
	count_of_customers_not_referenced = count_all_valid - count_customers
	amount_for_customers_not_referenced = amount_all_valid - amount_customers
	print("    Deposited without reference: count="+str(count_of_customers_not_referenced)+" sum="+f'{amount_for_customers_not_referenced:.8f}')
	
	#get min deposit 
	min = get_min()
	print("    Smallest valid deposit: "+ f'{min:.8f}')

	#get max deposit
	max = get_max()
	print("    Largest valid deposit: "+ f'{max:.8f}')

#total transactions in the jsonn files are 312 (175+137)
#out of which 13 are identical (with repeating txid)
#there are 269 records with category receive 
#from that 167 records have connfirmation >= 6
#145 of those records belong to the known customers
#22 remaining records are valid but without reference
#there are 28 transactions for category send and one of immature and generate category
"""
Repeating TXID's: 
111dc83db39d452daf199b1aa3829c39d79e802a9d7ba416a7560b2a4ceee3f0
fa96000f88693427485181510f57119a1704015b9f96b9c19efffb277d202548
58c33ad7c98754cce27b0ad60cc8bb612d8a37946d5a1439806c8ee4c0d295fd
b1c7e3b67d128088c829c31a323c883a05bd9fa8b9a5a7bfd56d67c8579f6473
6feb5e58452e07b074497f0082659b0463759418479e166a74b92b98eeed1a15
8aa80d8d09ec01163984e214295c2177563aaba4a595267b8a2c0215be8b4d7d
d2344f32357fcde1464c7dcd643a0e38f58283e4eaaa630831777d9ebcce8817
5862934ea32180ea6d8ccc2de7a937568f94277a74c2c37be6596041806d1984
1ab5c27a4896b8fb241271e2d7bba0306bb2da18bd763eecc8cbb6476449b56c
c7af9e3d47ea1e526227ae34d297ca57d95de89397fdf20342fe5d39d93b1041
ecebebf6ea1a46bf7df9ba3d38ffebcdd8f5b284b8b94b523ca131f751219554
f674a728f69e3f27054fd4cf1fcbb953275b214bf9a48936017a7a85fa6e2663
c828a14c948aadb71f4fd25e898bf4c147c6bfa4c26cf950d6026c536c855c9a
"""