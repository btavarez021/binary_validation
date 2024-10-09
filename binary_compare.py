from  creds import sf_creds, mysql_creds
import snowflake.connector
import logging 
from  PIL import Image
import io
import hashlib
import mysql.connector
import pandas as pd
import openpyxl
from datetime import datetime

logging.basicConfig(filename="./logs/binary_validations_{:%Y-%m-%d_%H}.log".format(datetime.now()),
                    format='%(asctime)s %(message)s',
                    filemode='w', level=logging.INFO)

# Connect to snowflake
def snowflake_connection():

    sf_conn = snowflake.connector.connect(
        user= sf_creds['sf_user'],
        password=sf_creds['sf_password'],
        account=sf_creds['sf_account'],
        warehouse=sf_creds['sf_warehouse'],
        database=sf_creds['sf_db'],
        schema=sf_creds['sf_schema']
    )

    return sf_conn

# Connect to MYSQL
def mysql_conn():
    mysql_conn = mysql.connector.connect(
    host=mysql_creds['host'],
    user=mysql_creds['user'],
    password=mysql_creds['password']
    )

    return mysql_conn


def get_binary_data(sf_conn, mysql_conn): 
   
    logging.info("Using the following queries to get data...")

    mysql_query = "select created_at, guest_id, guest_picture, cruise_contract_signature, waiver_signature, hippa_signature, payment_signature from embarkd.FORM_ANSWERS_HISTORY fah where  REGISTER_ID IN (12, 13,14,15,25);"
    logging.info(mysql_query)
    sf_query = "select created_at, guest_id, guest_picture, cruise_contract_signature, waiver_signature, hippa_signature, payment_signature from LZ_D_STRIIM.DEMO_EMBARKD.GRA_FORM_ANSWERS_HISTORY where REGISTER_ID IN (12, 13,14,15,25);"
    logging.info(sf_query)
    try:

        # Get MYSQL and Snowflake cursors and xecute MYSQL and Snowflake queries to get data.

        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(mysql_query)
        mysql_results = mysql_cursor.fetchall()

        sf_cursor = sf_conn.cursor()
        sf_cursor.execute(sf_query)
        sf_results= sf_cursor.fetchall()

        sf_data = {}
        mysql_data = {}
        messages = set()

        # Iterate over Snowflake results and get column names. 
        for row in sf_results:
            sf_created_at = row[0]
            sf_guest_id = row[1]
            sf_guest_picture = row[2]
            sf_cruise_contract_signature = row[3]
            sf_waiver_signature = row[4]
            sf_hippa_signature = row[5]
            sf_payment_signature = row[6]
            # print("SF DATA: ", sf_waiver_signature)

            # if isinstance(sf_payment_signature, (bytes, bytearray)):
            #     sf_payment_signature += b'\x01'

            # Append Snowflake guest_id with guest_picture binary into a dictionary.
            # Example {"1": "x89PNG\r\n\x1a"}
            sf_data[sf_guest_id] = {"guest_picture": sf_guest_picture, 'cruise_contract_signature':sf_cruise_contract_signature, 'waiver_signature':sf_waiver_signature, 
                                          'hippa_signature': sf_hippa_signature, 'payment_signature': sf_payment_signature}

            
                
                
            # Turn Binary into Image. Save file with file name convention DBNAME_GUESTID.png.
            # Filter query above to give you only the guest_id's that you need so that you are not saving a ton of files. 
            # To avoid opening up so many images,  I will add a conditional statement below.
            if len(row) <= 50:
                for guest_id, guest_data in sf_data.items():
                    for key, value in guest_data.items():
                        if isinstance(value, (bytearray, bytes)):
                            try:
                                # print(f"Processsing {key} for guest {guest_id}. Byte array length: {len(value)}.")
                                with Image.open(io.BytesIO(value)) as img:
                                    if img.getbbox() is None:
                                        messages.add(f"The image for {key} for guest {guest_id} is blank. No Bounding box.")
                                    else:
                                        # image.show()
                                        img.save(f'sf_{guest_id}_{key}.png')
                                        messages.add(f"{key} for guest {guest_id} is a valid image and displayed successfully!")
                            except Exception as e:
                                print(f"Error opening {key} for guest {guest_id} as an image: {e}")
                        elif isinstance(value, str):
                            print(f"{key} for guest {guest_id} is a string not byte/bytearray")
                        elif isinstance(value, int):
                            print(f"{key} for guest {guest_id} is a integer not byte/bytearray")
                        else:
                            print(f"{key} for guest {guest_id} is unhandled type {type(value).__name__}, value: {value}")

            else:
                print(f"Snowflake: More than {len(row)} rows. Please add a where clause to the Snowflake query to filter by guest_id.")

        # Iterate over MYSQL results and get column names. 
        for row in mysql_results:
            mysql_created_at = row[0]
            mysql_guest_id = row[1]
            mysql_guest_picture = row[2]
            mysql_cruise_contract_signature = row[3]
            mysql_waiver_signature = row[4]
            mysql_hippa_signature = row[5]
            mysql_payment_signature = row[6]
            # print("MYSQL DATA ", mysql_waiver_signature)

            # Append MYSQL guest_id with guest_picture binary into a dictionary.
            # Example {"1": "x89PNG\r\n\x1a"}
            mysql_data[mysql_guest_id] = {"guest_picture": mysql_guest_picture, 'cruise_contract_signature':mysql_cruise_contract_signature, 'waiver_signature':mysql_waiver_signature, 
                                          'hippa_signature': mysql_hippa_signature, 'payment_signature': mysql_payment_signature}
        
            # Turn Binary into Image. Save file with file name convention DBNAME_GUESTID.png.
            # Filter query above to give you only the guest_id's that you need so that you are not saving a ton of files. 
            # To avoid opening up so many images,  I will add a conditional statement below.
            if len(row) <= 50:
                for guest_id, guest_data in mysql_data.items():
                    for key, value in guest_data.items():
                        if isinstance(value, (bytearray, bytes)):
                            try:
                                # print(f"Processsing {key} for guest {guest_id}. Byte array length: {len(value)}.")
                                with Image.open(io.BytesIO(value)) as img :
                                    if img.getbbox() is None:
                                         messages.add(f"The image for {key} for guest {guest_id} is blank. No Bounding box.")
                                    else:
                                        # image.show()
                                        img.save(f'mysql_{guest_id}_{key}.png')
                                        messages.add("{key} for guest {guest_id} is a valid image and displayed successfully!")
                            except Exception as e:
                                print(f"Error opening {key} for guest {guest_id} as an image: {e}")
                        elif isinstance(value, str):
                            print(f"{key} for guest {guest_id} is a string not byte/bytearray")
                        elif isinstance(value, int):
                            print(f"{key} for guest {guest_id} is a integer not byte/bytearray")
                        else:
                            print(f"{key} for guest {guest_id} is unhandled type {type(value).__name__}, value: {value}")
            else:
                print(f"MYSQL: More than {len(row)} rows. Please add a where clause to the MYSQL query to filter by guest_id.")
        
    except Exception as e:
        print(str(e))
    finally:
        mysql_cursor.close()
        sf_cursor.close()
        mysql_conn.close()
        sf_conn.close()
    
    for message in messages:
        print(message)
    
    # print("SF DATA: ", sf_data['waiver_signature'])
    # print("MYSQL DATA ", mysql_data['waiver_signature'])
    
    return sf_data, mysql_data


def hash_binary_data(binary_data):
    if binary_data is None:
        return None
    return hashlib.md5(binary_data).hexdigest()

def compare_binary(sf_data, mysql_data, columns):

    mismatches = []

    validation_rows = []
    
    logging.info(f"Checking the following {columns}")

    # Compare SF into MYSQL. Where SF GuestID is in MYSQL.
    for guest_id, sf_binary_data in sf_data.items():
        if guest_id in mysql_data:
            mysql_binary_data = mysql_data[guest_id]
            for column in columns:
                if column in sf_binary_data and column in mysql_binary_data:
                    # Hash guest_picture from both tables
                    hash2= hash_binary_data(mysql_binary_data[column])
                    validation_rows.append({"source": "MySQL", "database":"embark", "schema":"", "table":"FORM_ANSWERS_HISTORY", "column":column, "md5_hash": hash2})
                    hash1 = hash_binary_data(sf_binary_data[column])
                    validation_rows.append({"source": "Snowflake", "database":"LZ_D_STRIIM", "schema":"DEMO_EMBARKD", "table":"GRA_FORM_ANSWERS_HISTORY","column":column, "md5_hash": hash1})
                    # Compare the hashes
                    if hash1 != hash2:
                        print(f"Guest ID {guest_id}: {column} are different.")
                        logging.info(f"Guest ID {guest_id}: {column} are different.")
                        mismatches.append({'guest_id':guest_id, 'column':column})
                    else:
                        logging.error(f"Guest ID {guest_id}: {column} are the same.")
                        print(f"Guest ID {guest_id}: {column} are the same.")
                else:
                    logging.error(f"Column {column} is not found under {guest_id}")
                    print(f"Column {column} is not found under {guest_id}")
    # Identify guests in MYSQL not in SF
    for guest_id in mysql_data:
        if guest_id not in sf_data:
            logging.warn(f"Guest ID {guest_id} exists in mysql but not in snowflake.")
            print(f"Guest ID {guest_id} exists in mysql but not in snowflake.")


    df = pd.DataFrame(validation_rows)
    df.to_csv('binary_validations.csv', index=False)

    return mismatches

sf_conn = snowflake_connection()
mysql_conn = mysql_conn()
sf_data, mysql_data = get_binary_data(sf_conn, mysql_conn)
columns_to_compare = ['guest_picture', 'cruise_contract_signature', 'waiver_signature', 'hippa_signature', 'payment_signature']
mismatched_guests = compare_binary(sf_data, mysql_data, columns_to_compare)

# Output the result and save it to a CSV locally.
if mismatched_guests:
   df = pd.DataFrame(mismatched_guests, columns=['GUEST_ID', 'cruise_contract_signature', 'WAIVER_SIGNATURE', 'HIPPA_SIGNATURE', 'PAYMENT_SIGNATURE'])
   df.to_csv('mismatched_guest_picture.csv', index=False)
   print(f"Mismatches found for guest IDs: {mismatched_guests}")
else:
   print("All images match.")