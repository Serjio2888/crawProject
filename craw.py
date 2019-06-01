import asyncio
import aiohttp
from bs4 import BeautifulSoup
from time import sleep
import requests
import asyncpg
from datetime import datetime
from aioelasticsearch import Elasticsearch
import aioamqp
import json

async def callback(channel, body, envelope, properties):
    b = json.loads(body)
    conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
    row = await conn.fetchrow('SELECT * FROM token WHERE token = $1', b[0]['tok'])
    token = b[0]['tok']
    class Parsing:
        def __init__(self, domain, quantity, row, token):
            self.urls = set()
            self.main = domain
            self.counter = 0
            self.quantity = quantity
            self.channel_out = None
            self.token = token
            self.row = row

        async def waitress(self):
            await channel.queue_declare(queue_name='outcraw', durable=True)
            msg = await json.dumps('started crawler')
            await channel.basic_publish(
                payload=msg,
                exchange_name='',
                routing_key='outcraw'
            )

        async def crawling(self, q, session, es, glub):
            await asyncio.sleep(0.1)
            qua = 0
            now = datetime.now()
            while True: 
                if qua>glub:
                    await asyncio.sleep(0.4)
                    self.counter += 1
                    await es.close()
                    if self.counter >= self.quantity:
                        timer = datetime.now() - now
                        avg = str(timer/len(self.urls))
                        if self.main[:5]=='https': a = 1
                        else: a = 0
                        await conn.execute('''
                                            INSERT INTO stats(domain, user_id, https, name, time, pages_count,
                                            avg_time_per_page, max_time_per_page, min_time_per_page) 
                                            VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9)''',
                                            self.main, self.row['user_id'], a, 'serg', str(timer), len(self.urls), avg, avg, avg)
                        return 'done'
                    else:
                        break
                else:    
                    try:
                        link = await q.get()
                        print(link)
                        qua += 1
                        async with session.get(link) as r:                    
                            html = await r.read()
                            soup = BeautifulSoup(html, 'html.parser')
                            doc = {
                                'link': link,
                                'text': soup.get_text(),
                                'timestamp': datetime.now(),
                                }                   
                            await es.index(index="test-index", doc_type='tweet', body=doc, id=self.row['user_id'])
                            await asyncio.sleep(0.1)


                            findall = soup.find_all('link') + soup.find_all('a') 
                            for link in findall:
                                link = link.get('href')
                                if link not in self.urls:
                                    self.urls.add(link)
                                    if not link.startswith(self.main):
                                        if link.startswith('http'): #значит, это сайт другого домена
                                            continue
                                        if link.startswith('../'):
                                            link = self.main+link[3:]
                                            await q.put(link)
                                        else:
                                            link = self.main+link
                                            await q.put(link)
                            
                    except:
                        await es.close()
        
        async def myfun(self, quantity, domain, glub):
            tasks = list()
            q = asyncio.Queue()
            q.put_nowait(domain)
            async with aiohttp.ClientSession() as session:
                es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
                for _ in range(quantity):
                    task = asyncio.create_task(self.crawling(q, session, es, glub))
                    tasks.append(task)
                
                await asyncio.gather(*tasks)
                await self.waitress()
                

    domain = b[0]['domain']
    quantity = 10#число процессов
    p = Parsing(domain, quantity, row, token)
    glub = 7 #глубина
    await p.myfun(quantity, domain, glub)




async def receive():
    transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='incraw', durable=True)
    await channel.basic_consume(callback, queue_name='incraw', no_ack=True)
    print('READY')


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(receive())
event_loop.run_forever()
