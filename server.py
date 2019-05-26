from aiohttp import web
from time import sleep
import json
import asyncio
import aioamqp
from queue import Queue
from random import randint
import asynctnt
#возвращать нормальные ответы
#разобраться с апдейтами
#мб сделать ОРМ

class Ahandler():
    def __init__(self):
        self.q = Queue()
        self.conn = asynctnt.Connection(host='127.0.0.1', port=3327)

    async def hello(self, request):
        return web.Response(text="Hello, world\n")

    async def current(self, request):
        u_token = request.match_info['token']
        await self.conn.connect()
        data = await self.conn.select('token', key={'token':u_token}, index='tok')
        new_data = await self.conn.select('user', key={'id':data[0]['user_id']}, index='primary')
        print(new_data)
        inform = {'data':{'id': new_data[0]['id'], 'email':  new_data[0]['email'], 'name': new_data[0]['name'], 
            'created_date': new_data[0]['created'], 'last_login_date': new_data[0]['last_login']}}
        return web.Response(body=json.dumps(inform), status=200)


    async def login(self, request):
        req = await request.json()
        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()
        req['usage']='login'

        await channel.queue_declare(queue_name='inreg', durable=True)
        await channel.basic_publish(
            payload=json.dumps(req),
            exchange_name='',
            routing_key='inreg'
        )
        print(" [x] Senttttttt json")
        await protocol.close()
        transport.close()

        return web.Response(text="shit\n")

    async def signup(self, request):
        req = await request.json()
        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()
        req['usage']='signup'
        print(req)
        await channel.queue_declare(queue_name='inreg', durable=True)
        await channel.basic_publish(
            payload=json.dumps(req),
            exchange_name='',
            routing_key='inreg'
        )
        print(" [x] Sent json")
        await protocol.close()
        transport.close()

        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()

        await channel.queue_declare(queue_name='outreg', durable=True)
        await channel.basic_consume(self.callback, queue_name='outreg', no_ack=True)
        print('received')
        await asyncio.sleep(0.3)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def qqq(self):
        print('gotcha')
        return self.q.get()

    async def callback(self, channel, body, envelope, properties):
        return self.q.put(body)




app = web.Application()
h = Ahandler()
app.add_routes([web.get('/', h.hello)])
app.add_routes([web.get('/current/{token}', h.current)])
app.add_routes([web.post('/login', h.login)])#email, password
app.add_routes([web.post('/signup', h.signup)]) #curl -d '{"name":"rofl", "mail":"sdsd", "pass":12345}' http://localhost:8080/signup

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