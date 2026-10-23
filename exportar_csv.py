import sqlite3
import pandas as pd
import os

base_dir = 'D:/Projects/BI-SABADO/ENTREGA2/ETL_Project_actualizado_1/ETL'
db_path = os.path.join(base_dir, 'data', 'airbnb_etl.db')
data_dir = os.path.join(base_dir, 'data')

conn = sqlite3.connect(db_path)

tablas = ['listings', 'reviews', 'calendar']
for tabla in tablas:
    df = pd.read_sql(f'SELECT * FROM {tabla}', conn)
    csv_path = os.path.join(data_dir, f'{tabla}.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f'{tabla}: {len(df):,} filas -> {tabla}.csv')

conn.close()
print('Exportación completada')