from datetime import datetime
from db import dao, geo_map


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

    # decoding items from HBase row   
    def decode_items(self, hbase_data):
        decoded_data = {}
        for key, value in hbase_data.items():
            decoded_key = key.decode('utf-8')  # Decode key from bytes to string
            decoded_value = value.decode('utf-8')  # Decode value from bytes to string
            decoded_data[decoded_key] = decoded_value
        return decoded_data


    # Returns True if amount < UCL as per the card_id from the lookup table
    def check_ucl(self, curr_card_id, curr_amt):
        # get data from Hbase lookup table as per this card_id
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)
        # compare amount with ucl from lookup
        column_family = 'cf1'
        column_qualifier = 'ucl'
        if curr_amt < float(decoded_data.get(f'{column_family}:{column_qualifier}')):
            return True
        else:
            return False
        
    # Return True if credit score of the card_id from lookup table > 200 
    def check_creditScore(self, curr_card_id):
        # get data from Hbase lookup table as per this card_id
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)
        # check if credit score > 200
        column_family = 'cf1'
        column_qualifier = 'credit_score'
        if int(decoded_data.get(f'{column_family}:{column_qualifier}')) > 200:
            return True
        else:
            return False
        

    # Return True if speed of travel between the last postcode and current postcode of the location is within possible limits
    def checkDistance(self, curr_card_id, postcode, transaction_dt):
        lookup_data = self.dao_obj.get_data(curr_card_id, 'hbase_lookup_table')
        decoded_data = self.decode_items(lookup_data)

        # fetch the latitude and longitude of current transaction postcode
        curr_lat = self.geo_obj.get_lat(postcode)
        curr_long = self.geo_obj.get_long(postcode)

        # fetch the latitude and longitude of postcode of the last transaction for this card_id
        prev_lat = self.geo_obj.get_lat(decoded_data.get('cf1:last_postcode'))
        prev_long = self.geo_obj.get_long(decoded_data.get('cf1:last_postcode'))

        # calculate the distance between current and last postcodes
        distance = self.geo_obj.distance(curr_lat,curr_long, prev_lat, prev_long)

        # find the time difference current and last transaction
        curr_timestamp = datetime.strptime(transaction_dt, '%Y-%m-%d %H:%M:%S')
        prev_timestamp = datetime.strptime(decoded_data.get('cf1:last_transaction_dt'), '%Y-%m-%d %H:%M:%S')
        sec_diff = (curr_timestamp - prev_timestamp).total_seconds() 

        # assuming 900km/h (or 250m/s) as the max speed of passenger air travel. 
        if sec_diff != 0 and (distance*1000)/sec_diff <= 250:
            return True
        else:
            return False
        

    # Classifying as GENUINE Or FRAUD and updating HBase tables
    def rule_check(self, card_id, member_id, amount, postcode, pos_id, transaction_dt):
        if self.check_ucl(card_id, amount) and self.check_creditScore(card_id) and self.checkDistance(card_id, postcode, transaction_dt):
            # update lookup table with current postcode and transaction_dt as transaction is Genuine
            self.dao_obj.write_data(card_id.encode(), 
            {
                b'cf1:last_postcode': postcode.encode(), 
                b'cf1:last_transaction_dt': str(transaction_dt).encode()
            }, 
                'hbase_lookup_table')
            # write the record to card_transactions table
            self.dao_obj.write_data(f"{card_id}_{amount}_{transaction_dt}".encode(),
            {   
                b'cf1:member_id': member_id.encode(), 
                b'cf1:postcode': postcode.encode(),
                b'cf1:pos_id': pos_id.encode(), 
                b'cf1:status': 'GENUINE'.encode()
            },
            'card_transactions')
            return 'GENUINE'
        else:
            # write the record to card_transactions table
            self.dao_obj.write_data(f"{card_id}_{amount}_{transaction_dt}".encode(),
            {   
                b'cf1:member_id': member_id.encode(), 
                b'cf1:postcode': postcode.encode(),
                b'cf1:pos_id': pos_id.encode(), 
                b'cf1:status': 'FRAUD'.encode()
            },
            'card_transactions')
            return 'FRAUD'

