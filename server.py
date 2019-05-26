from aiohttp import web
from time import sleep
import json
import asyncio
import aioamqp
from queue import Queue

class Ahandler():
    def __init__(self):
        self.q = Queue()

    async def hello(self, request):
        return web.Response(text="Hello, world\n")

    async def sh(self, request):
        print(request)
        print(request.match_info.get('do'))
        return web.Response(text="shit\n")

    async def signup(self, request):
        req = await request.json()
        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()

        await channel.queue_declare(queue_name='inbound', durable=True)

        await channel.basic_publish(
            payload=json.dumps(req),
            exchange_name='',
            routing_key='inbound'
        )
        print(" [x] Sent json")
        await protocol.close()
        transport.close()

        transport, protocol = await aioamqp.connect(host='localhost', login_method='PLAIN')
        channel = await protocol.channel()

        await channel.queue_declare(queue_name='outbound', durable=True)
        await channel.basic_consume(self.callback, queue_name='outbound', no_ack=True)

        await asyncio.sleep(0.2)
        js = await self.qqq()
        if js != 'error':
            return web.Response(body=js, status=200)
        else:
            return web.Response(body='shit happens', status=404)

    async def qqq(self):
        return self.q.get()

    async def callback(self, channel, body, envelope, properties):
        return self.q.put(body)




app = web.Application()
h = Ahandler()
app.add_routes([web.get('/', h.hello)])
app.add_routes([web.get('/shit/{do}', h.sh)])
app.add_routes([web.post('/shit/{do}', h.sh)])#email, password
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