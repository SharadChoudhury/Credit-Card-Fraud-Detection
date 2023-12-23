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


def getlastrow(table):
    # Start a scanner with descending order to get the last row key
    scanner = table.scan(reverse=True, limit=1)
    # Get the last row key (if any) else default is 0.
    last_row_key = next(iter(scanner), [0])
    # Close the scanner
    scanner.close()
    # Extract the row key from the result
    last_row_key = last_row_key[0]
    return int(last_row_key)

    

    
#batch insert data in events table 
def batch_insert_data(filename):
    open_connection()

    file = open(filename, "r")
    table = get_table()
    lastrow = getlastrow(table)   # getting the last row id from the Hbase table
    i = lastrow 
    #cols = ['card_id','member_id','amount','postcode','pos_id','transaction_dt','status']
    cols = []

    print("starting batch insert of events")

    with table.batch(batch_size=1000) as b:
        for line in file:
            if i != lastrow : # if line is not header
                temp = line.strip().split(",")
                row_key = str(i).encode()  # Encode row key to bytes

                for j in range(len(cols)):
                    b.put(row_key, {b'cf1:' + cols[j].encode(): temp[j].encode()})

            else:       # if row is header, fetch the column names and store in cols
                cols = line.strip().split(",")       

            i+=1

    file.close()
    print("File written to Hbase")

    
    close_connection()





if __name__ == '__main__':
    batch_insert_data('card_transactions.csv')