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
        print(line)
        card_id = line.split(',')[0]
        amount = float(line.split(',')[2])
        postcode = line.split(',')[3]

        tran_dt = line.split(',')[5]
        input_datetime = datetime.strptime(tran_dt, '%Y-%m-%d %H:%M:%S')

        print('status :' ,rules_instance.rule_check(card_id, amount, postcode, input_datetime))