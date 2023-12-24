import happybase

#create connection
connection = happybase.Connection(host='localhost', port=9090 ,autoconnect=False)

#open connection to perform operations
def open_connection():
    connection.open()

#close the opened connection
def close_connection():
    connection.close()

#get the pointer to a table
def get_table():
    # print(connection.tables())
    table_name = 'card_transactions'
    table = connection.table(table_name)
    return table


#batch insert data in events table 
def batch_insert_data(filename):
    open_connection()
    file = open(filename, "r")
    table = get_table()
    cols = ['card_id','member_id','amount','postcode','pos_id','transaction_dt','status']
    print("starting batch insert of events")

    with table.batch(batch_size=1000) as b:
        for line in file.readlines()[1:]:
            temp = line.strip().split(",")
            # taking card_id, amount, transaction_dt as rowkey
            row_key = f"{temp[0]}_{temp[2]}_{temp[5]}".encode()  # Encode row key to bytes
            for j in range(len(cols)) :
                if j not in [0,2,5]:
                    b.put(row_key, {b'cf1:' + cols[j].encode(): temp[j].encode()})  

    file.close()
    print("File written to Hbase")
    close_connection()


if __name__ == '__main__':
    batch_insert_data('card_transactions.csv')