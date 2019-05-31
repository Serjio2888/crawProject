from aiohttp import web
from time import sleep
import json
import asyncio
import aioamqp
from queue import Queue
from random import randint
import asyncpg

#связь краулера и сервера - как лучше?
#возвращать нормальные ответы  - всё ещё актуально
#разобраться с апдейтами - yes
#мб сделать ОРМ - hhaha

#Запрос на проверку токена доступа (validate). В сообщении должен передаваться токен доступа. 
#В ответ должны возвратиться все данные пользователя (из таблицы User), иначе ошибка.


class Ahandler():
    def __init__(self):
        self.q = Queue()
        #self.requests = {
        #    'irjeoigejero': asyncio.Future()}
        #self.conn = asynctnt.Connection(host='127.0.0.1', port=3666)

    async def hello(self, request):
        return web.Response(text="Hello, world\n")

    async def stat(self, request):
        token=request.match_info['token']
        conn = await asyncpg.connect('postgresql://serg@localhost/tp', password='qwerty')
        row = await conn.fetchrow(
        'SELECT * FROM token WHERE token = $1', token)
        info = await conn.fetchrow(
        'SELECT * FROM stats WHERE user_id = $1', row['user_id'])
        return web.Response(body=json.dumps((info['domain'], info['user_id'], info['https'],
                                            info['name'], info['time'], info['pages_count'],
                                            info['avg_time_per_page'])), status=200)


    async def index(self, request):#даже не пытайся меня вызвать
        req = await request.json()
        req['usage']='index'
        req['tok']=request.match_info['token']
        await self.queues(req)
        await asyncio.sleep(0.3)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def current(self, request):
        req = dict()
        req['usage']='current'
        req['tok']=request.match_info['token']
        await self.queues(req)
        await asyncio.sleep(0.3)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def login(self, request):
        req = await request.json()
        req['usage']='login'
        await self.queues(req)
        await asyncio.sleep(0.3)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def signup(self, request):
        req = await request.json()
        req['usage']='signup'
        fut = await self.queues(req)

        #await fut
        await asyncio.sleep(0.3)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def queues(self, req):
        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()
        await channel.queue_declare(queue_name='inreg', durable=True)
        await channel.basic_publish(
            payload=json.dumps(req),
            exchange_name='',
            routing_key='inreg'
        )
        fut = asyncio.Future()
        self.requests['rjwiejf'] = fut
        #return fut
        print(" [x] Sent json")
        await channel.queue_declare(queue_name='outreg', durable=True)
        await channel.basic_consume(self.callback, queue_name='outreg', no_ack=True)
        await protocol.close()
        transport.close()
        return None
        
    async def qqq(self):
        print('gotcha')
        return self.q.get()

    async def callback(self, channel, body, envelope, properties):
        #fut = self.requests[reqid]
        #fut.set_data(body)
        return self.q.put(body)




app = web.Application()
h = Ahandler()#POST /index
app.add_routes([web.get('/', h.hello)])
app.add_routes([web.get('/current/{token}', h.current)])
app.add_routes([web.post('/login', h.login)])#email, password
app.add_routes([web.post('/signup', h.signup)]) #curl -d '{"name":"rofl", "mail":"sdsd", "pass":12345}' http://localhost:8080/signup
app.add_routes([web.post('/index/{token}', h.index)])
app.add_routes([web.get('/stat/{token}', h.stat)])
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