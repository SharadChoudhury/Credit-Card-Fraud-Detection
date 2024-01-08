import dao
import verify_rules
from datetime import datetime 

hbase_instance = dao.HBaseDao.get_instance()
rules_instance = verify_rules.verifyRules.get_instance()

print(hbase_instance)
print(rules_instance)

file = 'test.csv'

with open(file) as f:
    for line in f:
        line = line.strip()
        card_id = line.split(',')[0]
        member_id = line.split(',')[1]
        amount = float(line.split(',')[2])
        postcode = line.split(',')[3]
        pos_id = line.split(',')[4]
        tran_dt = line.split(',')[5]
        status = rules_instance.rule_check(card_id, amount, postcode, tran_dt)
        tran_dt_hbase = datetime.strptime(tran_dt, '%Y-%m-%d %H:%M:%S').strftime('%d-%m-%Y %H:%M:%S')
        print(line, status, end='\n\n')
        print(f"{card_id}_{amount}_{tran_dt_hbase}", end='\n\n')

        hbase_instance.write_data(f"{card_id}_{amount}_{tran_dt_hbase}".encode(),
        {b'cf1:member_id': member_id.encode(), 
            b'cf1:postcode': postcode.encode(),
            b'cf1:pos_id': pos_id.encode(), 
            b'cf1:status': status.encode()},
        'card_transactions'
        )