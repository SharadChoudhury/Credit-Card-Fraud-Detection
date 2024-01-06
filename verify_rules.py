import dao, geo_map
from datetime import datetime
import dao
import geo_map

class verifyRules():
    """
    rules verification class for fraud detection
    """
    __instance = None
    dao_obj = dao.HBaseDao.get_instance()
    geo_obj = geo_map.GEO_Map.get_instance()

    @staticmethod
    def get_instance():
        """ Static access method. """
        if verifyRules.__instance == None:
            verifyRules()
        return verifyRules.__instance


    def __init__(self):
        if verifyRules.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            verifyRules.__instance = self
            
    def decode_items(self, hbase_data):
        decoded_data = {}
        for key, value in hbase_data.items():
            decoded_key = key.decode('utf-8')  # Decode key from bytes to string
            decoded_value = value.decode('utf-8')  # Decode value from bytes to string
            decoded_data[decoded_key] = decoded_value
        return decoded_data

    def check_ucl(self, curr_card_id, curr_amt):
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)
        column_family = 'cf1'
        column_qualifier = 'ucl'
        if curr_amt < float(decoded_data.get(f'{column_family}:{column_qualifier}')):
            return True
        else:
            return False
        

    def check_creditScore(self, curr_card_id):
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)
        column_family = 'cf1'
        column_qualifier = 'credit_score'
        if int(decoded_data.get(f'{column_family}:{column_qualifier}')) > 200:
            return True
        else:
            return False
        

    def checkDistance(self, curr_card_id, postcode, transaction_dt):
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)
        curr_lat = self.geo_obj.get_lat(postcode)
        curr_long = self.geo_obj.get_long(postcode)
        prev_lat = self.geo_obj.get_lat(decoded_data.get('cf1:last_postcode'))
        prev_long = self.geo_obj.get_long(decoded_data.get('cf1:last_postcode'))
        distance = self.geo_obj.distance(curr_lat,curr_long, prev_lat, prev_long)
        curr_timestamp = datetime.strptime(transaction_dt, '%Y-%m-%d %H:%M:%S')
        #curr_timestamp = transaction_dt
        prev_timestamp = datetime.strptime(decoded_data.get('cf1:last_transaction_dt'), '%Y-%m-%d %H:%M:%S')
        sec_diff = (curr_timestamp - prev_timestamp).total_seconds() 

        # assuming 900km/h (or 250m/s) as the max speed of passenger air travel
        if (distance*1000)/sec_diff <= 250:
            return True
        else:
            return False
        

    def rule_check(self, card_id, amount, postcode, transaction_dt):
        if self.check_ucl(card_id, amount) and self.check_creditScore(card_id) and self.checkDistance(card_id, postcode, transaction_dt):
            # update lookup table with current postcode and transaction_dt
            self.dao_obj.write_data(card_id.encode(), {b'cf1:last_postcode': postcode.encode(), b'cf1:last_transaction_dt': str(transaction_dt).encode()}, 'hbase_lookup_table')
            return 'GENUINE'
        else:
            return 'FRAUD'

