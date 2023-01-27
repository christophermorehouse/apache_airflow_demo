from airflow.models import Variable
import argparse
import pandas as pd
import requests
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--user")
parser.add_argument("-pw", "--password")
parser.add_argument("-i", "--ip")
parser.add_argument("-p", "--port")
parser.add_argument("-d", "--database")
parser.add_argument("-s", "--schema")
args = parser.parse_args()

# Allow script to run independent of Airflow if supplied with arguments
if args.user:
    warehouse_user = args.user
else:
    warehouse_user = Variable.get('warehouse_user')

if args.password:
    warehouse_password = args.password
else:
    warehouse_password = Variable.get('warehouse_password')

if args.ip:
    warehouse_ip = args.ip
else:
    warehouse_ip = Variable.get('warehouse_ip')

if args.port:
    warehouse_port = args.port
else:
    warehouse_port = Variable.get('warehouse_port')

if args.database:
    warehouse_database = args.database
else:
    warehouse_database = Variable.get('warehouse_database')

if args.schema:
    warehouse_schema = args.schema
else:
    warehouse_schema = Variable.get('warehouse_schema')

# Create database connection
conn_string = 'postgresql+psycopg2://' + warehouse_user + ':' + warehouse_password + '@' + warehouse_ip + ':' + warehouse_port +'/' + warehouse_database
db = create_engine(conn_string)
conn = db.connect()

root_url = 'https://api.mercadolibre.com'

# By default, API returns 50 items
discover_item_ids = requests.get(root_url + '/sites/MLA/search?category=MLA1577').json()['results']

item_id_list = []
# Loop through json response and extract all item ids and store them in a list
for id_value in discover_item_ids:
    if "id" in id_value:
        item_id_list.append(id_value["id"])

item_details_list = []
# Get the details for every item_id in item_id_list. Store each item detail json response in another list
for i in item_id_list:
    details = requests.get(root_url + '/items/' + i).json()
    item_details_list.append(details)

# Take contents of item_details_list and convert it into a Pandas DataFrame
df = pd.json_normalize(item_details_list)

# From the Pandas DataFrame, extract just the fields that we'll load into the DB
item_details_list = df[['id', 'site_id', 'title', 'price', 'sold_quantity', 'thumbnail']]

# Insert current timestamp into the existing DataFrame.
item_details_list.insert(6, 'created_date', pd.Timestamp.now())

# Write DataFrame to the postgres database
item_details_list.to_sql('mla_microwaves', con=conn, if_exists ='replace', schema=warehouse_schema, index=False)
conn.close()