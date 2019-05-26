import asyncio
import aioamqp
import json
import asynctnt
from datetime import datetime
import hashlib
import uuid

async def hashing(ll):
    ll = hashlib.md5(str(ll).encode())
    return ll.hexdigest()

async def dating():
    return datetime.now()

async def get_token():
    return uuid.uuid4()

async def callback(channel, body, envelope, properties):
    print(1)
    body = json.loads(body)
    mail = body['mail']
    name = body['name']
    pw = body['pass']
    conn = asynctnt.Connection(host='127.0.0.1', port=3323)
    await conn.connect()
    passw = await hashing(pw)
    dtm = await dating()
    data = await conn.insert('user', [None, name, mail, passw, str(dtm), str(dtm)])
    user_id = data[0]['id']
    token = await get_token()
    await conn.insert('token', [None, str(token), user_id, str(dtm)])

    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()
    await channel.queue_declare(queue_name='outbound', durable=True)

    await channel.basic_publish(
        payload=json.dumps((str(token), str(dtm))),
        exchange_name='',
        routing_key='outbound'
    )
    print(" [x] Senteeed json")
    await protocol.close()
    transport.close()

    print(" [x] Sented json")


async def receive():
    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='inbound', durable=True)
    await channel.basic_consume(callback, queue_name='inbound', no_ack=True)


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()

