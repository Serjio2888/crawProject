from aiohttp import web
from time import sleep
import json
import asyncio
import aioamqp
from queue import Queue
from random import randint
import asyncpg
from uuid import uuid4

#связь краулера и сервера - как лучше?
#возвращать нормальные ответы  - всё ещё актуально
#разобраться с апдейтами - yes
#мб сделать ОРМ - hhaha

#Запрос на проверку токена доступа (validate). В сообщении должен передаваться токен доступа. 
#В ответ должны возвратиться все данные пользователя (из таблицы User), иначе ошибка.


class Ahandler():
    def __init__(self):
        self.q = Queue()
        self.requests = {   }
        self.transport, self.protocol = None, None
        self.channel = None
        self.channel_in = None
        self.channel_out = None

    async def channeling(self):
        self.transport, self.protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        self.channel = await self.protocol.channel()
        self.channel_in = await self.channel.queue_declare(queue_name='inreg', durable=True)
        self.channel_out = await self.channel.queue_declare(queue_name='outreg', durable=True)
        self.channel_cr = await self.channel.queue_declare(queue_name='incraw', durable=True)
        self.channel_crr = await self.channel.queue_declare(queue_name='outcraw', durable=True)

    async def hello(self, request):
        print('yes, it works, dude')
        return web.Response(text='answer')

    async def stat(self, request):#working good
        token=request.match_info['token']
        conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
        row = await conn.fetchrow(
        'SELECT * FROM token WHERE token = $1', token)
        print(row['user_id'])
        info = await conn.fetchrow(
        'SELECT * FROM stats WHERE user_id = $1', row['user_id'])
        await asyncio.sleep(1)
        return web.Response(body=json.dumps((info['domain'], info['user_id'], info['https'],
                                            info['name'], info['time'], info['pages_count'],
                                            info['avg_time_per_page'])), status=200)

    async def login(self, request):
        req = await request.json()
        req['usage']='login'
        words = str(uuid4())
        await self.queues(req, words)
        await asyncio.sleep(0.3)
        js = await self.requests[words]
        if json.loads(js)['status'] == 'ok':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body=js, status=400)

    async def signup(self, request):
        req = await request.json()
        req['usage']='signup'
        words = str(uuid4())
        await self.queues(req, words)
        await asyncio.sleep(1.3)
        js = await self.requests[words]
        if json.loads(js)['status'] == 'ok':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body=js, status=400)

    async def current(self, request):
        req = dict()
        req['usage']='current'
        req['tok']=request.match_info['token']
        words = str(uuid4())
        await self.queues(req, words)
        await asyncio.sleep(0.3)
        js = await self.requests[words]
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def ind(self, request):
        req = await request.json()
        req['tok']=request.match_info['token']
        words = str(uuid4())
        await self.tocraw(req, words)
        #идем в craw.py
        await asyncio.sleep(0.3)
        js = await self.requests[words]
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def search(self, request):
        req = await request.json()
        if req['limit']>100:
            req['limit']=100
        if req['limit']<1 or req['offset']<0:
            return web.Response(body='wrong data', status=400)
        req['usage']='search'
        words = str(uuid4())
        await self.tocraw(req, words)
        await asyncio.sleep(0.3)
        js = await self.requests[words]
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def tocraw(self, req, words):
        if not self.protocol:
            await self.channeling()
        await self.channel.basic_publish(
            payload=json.dumps((req, words)),
            exchange_name='',
            routing_key='incraw'
        )
        print(" [x] Sent json")
        fut = asyncio.Future()
        self.requests[words] = fut
        await self.channel.basic_consume(self.callback, queue_name='outcraw', no_ack=True)
        return None

    async def queues(self, req, words):
        if not self.protocol:
            await self.channeling()
        await self.channel.basic_publish(
            payload=json.dumps((req, words)),
            exchange_name='',
            routing_key='inreg'
        )
        print(" [x] Sent json")
        fut = asyncio.Future()
        self.requests[words] = fut
        await self.channel.basic_consume(self.callback, queue_name='outreg', no_ack=True)
        return None

    async def callback(self, channel, body, envelope, properties):
        b = json.loads(body)
        fut = self.requests[b[1]]
        fut.set_result(json.dumps(b[0]))




app = web.Application()
h = Ahandler()#POST /index
app.add_routes([web.get('/', h.hello)])#for tests
app.add_routes([web.get('/current/{token}', h.current)])
app.add_routes([web.post('/ind/{token}', h.ind)])#'{"domain":"https://python.org"}'
app.add_routes([web.post('/login', h.login)])#'{"mail":"sdsd", "pass":12345}'
app.add_routes([web.post('/signup', h.signup)])
#curl -d '{"name":"rofl", "mail":"sdsd", "pass":12345}' http://localhost:8080/signup
app.add_routes([web.get('/stat/{token}', h.stat)])
app.add_routes([web.post('/search', h.search)])#'{"q":"info", "limit":100, "offset":20}'
web.run_app(app)



# Методы, которые не требуют авторизации:
# POST /signup - метод регистрации нового пользователя. Принимает email, password, name. 
# В случае успеха возвращает - 200 {“status”: “ok”, “data”: {}}. 
#     Иначе - 4ХХ {“status”: “<текстовый код ошибки>”, “data”: {}}
# POST /login - метод получения/обновления токена доступа. Принимает email, password. 
#     В случае успеха возвращает - 200 {“status”: “ok”, “data”: {“token”: “...”, “expire”: <timestamp>}}. Иначе - см. пункт 1
# GET /search - метод поиска по обкаченным документам. Принимает q (запрос), limit (не может превышать 100), offset. 
#     Возвращает список документом с вхождением “q” - 200 {“status”: “ok”, “data”: [...]}. 
#     В случае некорректных параметров - 400 {“status”: “bad_request”, “data”: {}}

#     Методы, которые требуют авторизации. Для вызова этих методов необходимо добавить заголовок X-Token. 
#     Значение X-Token необходимо получить из метода /login. 
#     Если заголовок не передан - методы ниже должны возвращать ошибку 403 {“status”: “forbidden”, “data”: {}}
# GET /current - метод, который возвращает текущего пользователя. 
#     Возвращает 200 {“status”: “ok”, “data”: {“id”: …, “email”:  …, “name”: …, “created_date”: …, “last_login_date”: …}}
# POST /index - метод, который ставит задачу краулеру на индексацию домена.
#     Принимает domain. Возвращает - 200 {“status”: “ok”, “data”: {“id”: … }}
# GET /stat - метод, который возвращает статистику по сайтам пользователя. 
#     Возвращает - 200 {“status”: “ok”, “data”: [...]}