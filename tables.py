import asyncio
import asyncpg
from datetime import datetime

async def main():
    # Establish a connection to an existing database named "test"
    # as a "postgres" user.
    conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
    # Execute a statement to create a new table.

    #await conn.execute('DROP TABLE stats')

    # Insert a record into the created table.
    #p = await conn.execute('''
    #    INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
    #''', 'ffdgfdgfdg', 1, datetime.now())


    # Select a row from the table.
    #row = await conn.fetchrow(
    #    'SELECT * FROM token WHERE user_id = $1', 1)
    #print(row['token'])
    # *row* now contains
    # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))

    # Close the connection.
    await conn.close()

asyncio.get_event_loop().run_until_complete(main())

    # await conn.execute("""
    #      CREATE TABLE stats (
    #          domain text,
    #          user_id integer references users(id),
    #          https smallint,
    #          name text,
    #          time text,
    #          pages_count integer,
    #          avg_time_per_page text,
    #          max_time_per_page text,
    #          min_time_per_page text
    #      )
    #  """)

    # await conn.execute("""
    #      CREATE TABLE token (
    #          token text,
    #          user_id integer references users(id),
    #          expire_date timestamp
    #      )""")
    # print('made')

    # await conn.execute("""
    #      CREATE TABLE users (
    #          id serial PRIMARY KEY,
    #          email text,
    #          password text,
    #          name text,
    #          created_date timestamp,
    #          last_login_date timestamp
    #      )
    #  """)

    # Таблица CrawlerStats:
    # domain - text
    # author_id (fk to User) - int
    # https - int (0/1)
    # time - datetime
    # pages_count - int
    # avg_time_per_page - float
    # max_time_per_page - float
    # min_time_per_page - float
