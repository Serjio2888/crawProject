import asyncio
import aioamqp
import json
from datetime import datetime, timedelta
import hashlib
import uuid
import asyncpg

async def hashing(ll):
    ll = hashlib.md5(str(ll).encode())
    return ll.hexdigest()

async def dating():
    return datetime.now()

async def get_token():
    return uuid.uuid4()

async def us(conn, body):
    mail = body['mail']
    print('mail', mail)
    print('body', body)
    row = await conn.fetchrow('SELECT * FROM users WHERE email = $1', mail)
    print('row',row)
    return row

async def current(conn, body, dtm):
    token = body['tok']
    print(body)
    print('tokened')
    row = await conn.fetchrow('SELECT * FROM token WHERE token = $1', token) 
    if row['expire_date']>dtm:
        slova = dict()
        data = dict()
        row = await conn.fetchrow('SELECT * FROM users WHERE id = $1', row['user_id'])
        print('awaiting info')
        print('to do', row)
        slova['email']=row['email']
        slova['id']=row['id']
        slova['created']=str(row['created_date'])
        slova['last_login']=str(row['last_login_date'])
        data['data']=slova
        data['status']=200
        return json.dumps(data)#{row['email'], row['id'], str(row['created_date']), str(row['last_login_date'])})
    else:
        return 'failed token'

async def sign(conn, body, dtm):
    mail = body['mail']
    name = body['name']
    print('here')
    passw = await hashing(body['pass'])
    try:
        print('reg', await conn.execute('''
            INSERT INTO users(email, password, name, created_date, last_login_date) VALUES($1, $2, $3, $4, $5)
        ''', mail, passw, name, dtm, dtm))
        row = await us(conn, body)
        user_id = row['id']
        token = await get_token()
        print('tokeneddd',await conn.execute('''
            INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
        ''', str(token), user_id, dtm+timedelta(hours=23)))
        print('token')
        return json.dumps(('registred', name))
    except:
        return json.dumps(('some fu*king error'))


#datetime.now() + timedelta(minutes=30) - увеличим время
async def login(conn, body, dtm):
    passw = await hashing(body['pass'])
    print('p1: ', passw)
    try:
        row = await us(conn, body)
        print('p2: ', row['password'])
        if row['password']==passw:
            await conn.execute('''
                UPDATE users SET last_login_date = $1 WHERE email = $2;''',
                dtm, row['email'])
            new_token = str(uuid.uuid4())
            await conn.execute('''
                INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
                ''', new_token, row['id'], dtm+timedelta(hours=23))
            return json.dumps(new_token)
        else:
            return 'wrong password'
    except:
        return 'no such user'


async def callback(channel, body, envelope, properties):
    print(1)
    body = json.loads(body)
    conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
    dtm = await dating()
    if body['usage']=='signup':
        answ = await sign(conn, body, dtm)

    elif body['usage']=='login':
        answ = await login(conn, body, dtm)

    elif body['usage']=='index':
        answ = json.dumps(('here we go again'))

    elif body['usage']=='current':#############
        answ = await current(conn, body, dtm)

    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()
    await channel.queue_declare(queue_name='outreg', durable=True)
    await asyncio.sleep(0.2)
    await channel.basic_publish(
        payload=answ,
        exchange_name='',
        routing_key='outreg'
    )
    print(" [x] Senteeed json")
    await protocol.close()
    transport.close()
    print(" [x] Sented json")


async def receive():
    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='inreg', durable=True)
    await channel.basic_consume(callback, queue_name='inreg', no_ack=True)


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()

