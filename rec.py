import asyncio
import aioamqp
import json
import asynctnt
from datetime import datetime
import hashlib
import uuid
import asynctnt

async def hashing(ll):
    ll = hashlib.md5(str(ll).encode())
    return ll.hexdigest()

async def dating():
    return datetime.now()

async def get_token():
    return uuid.uuid4()

async def sign(conn, body, dtm):
    mail = body['mail']
    name = body['name']
    passw = await hashing(body['pass'])
    data = await conn.insert('user', [None, name, mail, passw, str(dtm), str(dtm)])
    user_id = data[0]['id']
    token = await get_token()
    await conn.insert('token', [None, str(token), user_id, str(dtm)])
    return json.dumps((str(token), str(dtm)))

async def login(conn, body, dtm):
    mail = body['mail']
    passw = await hashing(body['pass'])
    data = await conn.select('user', key={'email':mail}, index='sec')
    print(data)
    try:
        d = data[0]
        if d['pass']==passw:
            #надо апдейтнуть таблицу с токеном
            #апдейтнуть дату последнего логина
            new_token = str(uuid.uuid4())
            #print(await conn.update('token',  [ ['=', 'token', new_token ] ], key={'user_id':d['id']},)) - не вышло
            return new_token
        else:
            return 'wrong password'
    except:
        return 'no such user'


async def callback(channel, body, envelope, properties):
    print(1)
    body = json.loads(body)
    conn = asynctnt.Connection(host='127.0.0.1', port=3327)
    await conn.connect()
    dtm = await dating()
    if body['usage']=='signup':
        answ = await sign(conn, body, dtm)

    elif body['usage']=='login':
        answ = await login(conn, body, dtm)
        print(answ)

    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()
    await channel.queue_declare(queue_name='outreg', durable=True)

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

