import asyncio
import aioamqp
import json
from datetime import datetime, timedelta
import hashlib
import uuid
import asyncpg
from aioelasticsearch import Elasticsearch

async def hashing(ll):
    ll = hashlib.md5(str(ll).encode())
    return ll.hexdigest()

async def dating():
    return datetime.now()

async def get_token():
    return uuid.uuid4()

async def us(conn, body):
    mail = body['mail']
    row = await conn.fetchrow('SELECT * FROM users WHERE email = $1', mail)
    return row

async def current(conn, body, dtm):
    token = body[0]['tok']
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
        return json.dumps((data, body[1]))#{row['email'], row['id'], str(row['created_date']), str(row['last_login_date'])})
    else:
        return 'failed token'

async def sign(conn, body, dtm):
    mail = body[0]['mail']
    name = body[0]['name']
    passw = await hashing(body[0]['pass'])
    try:
        await conn.execute('''
            INSERT INTO users(email, password, name, created_date, last_login_date) VALUES($1, $2, $3, $4, $5)
        ''', mail, passw, name, dtm, dtm)
        row = await us(conn, body[0])
        user_id = row['id']
        token = await get_token()
        await conn.execute('''
            INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
        ''', str(token), user_id, dtm+timedelta(hours=23))
        js = [{'status':'ok', 'data':{'token':str(token), 'expire':str(dtm+timedelta(hours=23))}}, body[1]]
        return json.dumps(js)
    except:
        return json.dumps(('some fu*king error'))

async def login(conn, body, dtm):
    passw = await hashing(body[0]['pass'])
    try:
        row = await us(conn, body[0])
        print('p2: ', row['password'])
        if row['password']==passw:
            await conn.execute('''
                UPDATE users SET last_login_date = $1 WHERE email = $2;''',
                dtm, row['email'])
            new_token = str(uuid.uuid4())
            await conn.execute('''
                INSERT INTO token(token, user_id, expire_date) VALUES($1, $2, $3)
                ''', new_token, row['id'], dtm+timedelta(hours=23))
            js = [{'status':'ok', 'data':{'token':new_token, 'expire':str(dtm+timedelta(hours=23))}},body[1] ]
            return json.dumps(js)
        else:
            return  [{'status': 'wrong pass', 'data': {}}, body[1]]
    except:
        return [{'status': 'no such user', 'data': {}}, body[1]]

async def search(conn, body, dtm):
    #speaking honestly, i have no idea how can i search documents
    try:
        es = Elasticsearch()
        res = await es.get(index="test-index", doc_type='tweet')
        str(res['_source']).replace('\n','')
        ###print(str(res['_source']).replace('\n','')[body['offset']:body['offset']+body['limit']])???
        return json.dumps(body)
    except:
        return json.dumps(('error'))


async def callback(channel, body, envelope, properties):
    body = json.loads(body)
    conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
    dtm = await dating()
    if body[0]['usage']=='signup':
        answ = await sign(conn, body, dtm)

    elif body[0]['usage']=='login':
        answ = await login(conn, body, dtm)

    elif body[0]['usage']=='search':
        answ = await search(conn, body, dtm)

    elif body[0]['usage']=='current':
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
    print(" [x] Sented json")
    await protocol.close()
    transport.close()



async def receive():
    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='inreg', durable=True)
    await channel.basic_consume(callback, queue_name='inreg', no_ack=True)


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()

