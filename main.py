import mysql.connector
import pandas as pd
from dotenv import dotenv_values

db_config = dotenv_values(".env")

# Function to connect to the database
def connect_to_database(config):
    try:
        connection = mysql.connector.connect(**config)
        return connection
    except Exception as e:
        print(f"Error connecting to the MySQL database: {e}")
        return None

def query_tables(connection):
    if connection is None:
        return "Connection to database failed."

    cursor = connection.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    query = f"SELECT * FROM mensalidades order by dataemissao desc LIMIT 100000"
    df = pd.read_sql(query, connection)

    cursor.close()

    return df

# Connect to the database
db_connection = connect_to_database(db_config)

# Query the tables
result = query_tables(db_connection)

# Close the connection
if db_connection:
    db_connection.close()

# Print or process the result as needed
print(result)


result.columns

result['ano'] = result['dataemissao'].dt.year

result['desconto_perc'] = result['valordesconto']/result['valorbruto']

result['desconto_perc'].mean()

result.groupby('turma').agg({'desconto_perc':'mean'}).sort_values('desconto_perc', ascending = False)

result.query("ds_historico == 'MENSALIDADE'").groupby(['ano']).agg({'desconto_perc':'mean'}).sort_values('desconto_perc', ascending = False)

result.groupby(['ano', 'codigoaluno']).count().reset_index()[['ano', 'codigoaluno']].groupby('ano').count()

aa = result.query("turma == 'WORKPL2022A'")