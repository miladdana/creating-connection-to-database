import psycopg2
from psycopg2 import Error

try:
    # Connect to an existing database
    connection = psycopg2.connect(user="analytics_lmtdr_dana",
                                  password=")F^Ykt[{9Y6!c4n'p5UXg4?2UAN2#w?J6-Mp<@;:",
                                  host="gw.basalam.com",
                                  port="12348",
                                  database="basalam.ir")

    # Create a cursor to perform database operations
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")
    # Executing a SQL query
    cursor.execute("SELECT paid_at::date,user_id from laravel.payment_invoices where status=2999 and verified_at notnull limit 100")
    # Fetch result
    record = []
    record = cursor.fetchall()
    print(record)

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

import pandas as pd
df = pd.DataFrame(record )
print(df)