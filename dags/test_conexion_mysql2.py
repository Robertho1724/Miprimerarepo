import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("mysql+pymysql://root:dwh@localhost:3306/dwh")
connection = engine.connect()


with engine.begin() as connection:
    base = pd.read_sql_query(f"SELECT * FROM jugadores_futbol_ejemplo1", con=connection)
    base.to_sql(name='LABORATORIO2', con=connection, if_exists='append', index=False)



