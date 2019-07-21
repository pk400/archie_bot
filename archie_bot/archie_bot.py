import asyncio
import enum
import json

import aiopg
import requests
import websockets
import yaml

def run_in_loop(coro):
  return asyncio.get_event_loop().run_until_complete(coro)

def create_task(task):
  return asyncio.create_task(task)

class Payload:
  def __init__(self, op, d, s, t):
    self._op = op
    self._d = d
    self._s = s
    self._t = t

  def to_json(self):
    return json.dumps({
      'op': self._op,
      'd': self._d,
      's': self._s,
      't': self._t
    })

class DataStore:
  def __init__(self, dsn):
    self._dsn = dsn

  def open(self):
    self._connection_pool = run_in_loop(aiopg.create_pool(self._dsn))
    self._initialize()
    self._is_open = True
  
  async def store(self, timestamp, id, edited_timestamp, content, channel_id,
      author):
    await self._execute('''
      INSERT INTO messages (timestamp, id, edited_timestamp, content,
        channel_id, author)
      VALUES (%s, %s, %s, %s, %s, %s);
      ''', (timestamp, id, edited_timestamp, content, channel_id, author))
  
  def _initialize(self):
    run_in_loop(self._execute('''
      CREATE TABLE IF NOT EXISTS messages (
        timestamp TIMESTAMP, id BIGINT, edited_timestamp TIMESTAMP,
        content TEXT, channel_id BIGINT, author TEXT
      );
      '''))
  
  async def _execute(self, query, _vars={}):
    async with self._connection_pool.acquire() as connection:
      async with connection.cursor() as cursor:
        await cursor.execute(query, _vars)

class ArchieBot:
  def __init__(self, api_token, dsn,
      discord_http_url='https://discordapp.com/api/'):
    self._api_token = api_token
    self._dsn = dsn
    self._discord_http_url = discord_http_url
    self._data_store = None
    self._gateway = None

  def start(self):
    self._connect()
    self._identify()
    self._data_store = DataStore(self._dsn)
    self._data_store.open()
    run_in_loop(self._start_message_listener())

  def _connect(self):
    gateway_url = requests.get(self._discord_http_url + 'gateway').json()['url']
    self._gateway = run_in_loop(websockets.connect(gateway_url))

  def _identify(self):
    payload = Payload(2, {
        'token': self._api_token,
        'properties': {
          '$os': 'linux',
          '$browser': 'disco',
          '$device': 'disco'
        }
      }, None, None)
    run_in_loop(self._gateway.send(payload.to_json()))

  async def _start_message_listener(self):
    async for encoded_payload in self._gateway:
      message = json.loads(encoded_payload)
      if message['t'] == 'MESSAGE_CREATE':
        await self._on_message_create(message['d'])
  
  async def _on_message_create(self, message):
    await self._data_store.store(message['timestamp'], message['id'],
      message['edited_timestamp'], message['content'], message['channel_id'],
      message['author']['username'])

if __name__ == '__main__':
  configs = yaml.safe_load(open('config.yml', 'r'))
  bot = ArchieBot(api_token=configs['api_token'], dsn=configs['dsn'])
  bot.start()
