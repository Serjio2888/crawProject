import asyncio
import asyncpg
from datetime import datetime

async def main():
    conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
    #await conn.execute('DROP TABLE stats')
    #p = await conn.execute('''
    #    INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
    #''', 'ffdgfdgfdg', 1, datetime.now())
    #row = await conn.fetchrow(
    #    'SELECT * FROM token WHERE user_id = $1', 1)
    await conn.close()

asyncio.get_event_loop().run_until_complete(main())

#КАК Я ДЕЛАЛ ТАБЛИЦЫ:
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
