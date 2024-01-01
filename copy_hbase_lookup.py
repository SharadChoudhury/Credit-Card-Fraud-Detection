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
    table_name = 'hbase_lookup_table'
    table = connection.table(table_name)
    return table


#batch insert data in events table 
def batch_insert_data(filename):
    open_connection()
    file = open(filename, "r")
    table = get_table()
    cols = ['card_id','last_postcode','last_transaction_dt','credit_score','ucl']
    print("starting batch insert of events")

    with table.batch(batch_size=1000) as b:
        for line in file:
            temp = line.strip().split(",")
            # taking card_id as rowkey
            row_key = temp[0].encode()  # Encode row key to bytes
            for j in range(len(cols)) :
                if j != 0:
                    b.put(row_key, {b'cf1:' + cols[j].encode(): temp[j].encode()})  
    file.close()
    print("File written to Hbase")
    close_connection()


if __name__ == '__main__':
    batch_insert_data('genuine_trans.csv')

# create 'hbase_lookup_table', 'cf1'