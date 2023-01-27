from airflow.models import Variable
import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--user")
parser.add_argument("-pw", "--password")
parser.add_argument("-i", "--ip")
parser.add_argument("-p", "--port")
parser.add_argument("-d", "--database")
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

# Create database connection
conn_string = 'postgresql+psycopg2://' + warehouse_user + ':' + warehouse_password + '@' + warehouse_ip + ':' + warehouse_port +'/' + warehouse_database
db = create_engine(conn_string)
conn = db.connect()

# Get items that have earned more than $7,000,000
df = pd.read_sql_query('SELECT * from prod.mla_microwaves WHERE price * sold_quantity > 7000000', con=conn)
df.to_csv('/opt/airflow/dags/items.csv', encoding='utf-8', index=False)