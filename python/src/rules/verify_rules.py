from ..db import dao
from ..db import geo_map
from datetime import datetime

hbase_dao_instance = dao.HBaseDao.get_instance()
geo_map_instance = geo_map.GEO_Map.get_instance()

def check_ucl(curr_card_id, curr_amt):
    lookup_data = hbase_dao_instance.get_data(curr_card_id, 'hbase_lookup_table')
    column_family = 'cf1'
    column_qualifier = 'ucl'
    if curr_amt < lookup_data.get(f'{column_family}:{column_qualifier}'):
        return True
    else:
        return False
    

def check_creditScore(curr_card_id):
    lookup_data = hbase_dao_instance.get_data(curr_card_id, 'hbase_lookup_table')
    column_family = 'cf1'
    column_qualifier = 'credit_score'
    if lookup_data.get(f'{column_family}:{column_qualifier}') > 200:
        return True
    else:
        return False
    

def checkDistance(curr_card_id, postcode, transaction_dt):
    lookup_data = hbase_dao_instance.get_data(curr_card_id, 'hbase_lookup_table')
    curr_lat = geo_map_instance.get_lat(postcode)
    curr_long = geo_map_instance.get_long(postcode)
    prev_lat = geo_map_instance.get_lat(lookup_data.get('cf1:last_postcode'))
    prev_long = geo_map_instance.get_long(lookup_data.get('cf1:last_postcode'))
    distance = geo_map_instance.distance(curr_lat,curr_long, prev_lat, prev_long)
    curr_timestamp = datetime.strptime(transaction_dt, '%Y-%m-%d %H:%M:%S')
    prev_timestamp = datetime.strptime(lookup_data.get('cf1:last_transaction_dt'), '%Y-%m-%d %H:%M:%S')
    hour_diff = (curr_timestamp - prev_timestamp).total_seconds() / 3600

    # assuming 900km/h as the max speed of passenger air travel
    if distance/hour_diff <= 900:
        return True
    else:
        return False
    

def rule_check(card_id, amount, postcode, transaction_dt):
    if check_ucl(card_id, amount) and check_creditScore(card_id) and checkDistance(card_id, postcode, transaction_dt):
        # update lookup table with current postcode and transaction_dt
        hbase_dao_instance.write_data(card_id, {'cf1:last_postcode': postcode, 'cf1:last_transaction_dt': transaction_dt}, 'hbase_lookup_table')
        return 'GENUINE'
    else:
        return 'FRAUD'

