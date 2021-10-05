import psycopg2
import config
import pandas as pd
from psycopg2 import Error

try:
    # Connect to an existing database
    connection = psycopg2.connect(user=config.user,
                                  password=config.password,
                                  host=config.host,
                                  port=config.port,
                                  database=config.database)

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Executing a SQL query
    cursor.execute("""
    SELECT paid_at::date,user_id 
    from laravel.payment_invoices 
    where status=2999 and verified_at notnull 
    limit 100""")
    # Fetch result
    record = []
    record = cursor.fetchall()


except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

df = pd.DataFrame(record )
print(df)