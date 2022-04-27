import decimal
import multiprocessing
import random
import time
from datetime import datetime
import asyncio
import numpy
import websockets
import requests
import json
import signal
import sys
from functools import partial
import logging
import argparse
import socket
import copy
import array
import numpy as np
import httpx
import ssl
import os
from os import getpid
from ROOT import TObject, TFile, TTree, AddressOf
import pickle

def get_obj(path_to_file):
    with open(path_to_file, 'rb') as f:
        return pickle.load(f)


def save_obj(obj, path_to_file):
    with open(path_to_file, 'wb') as f:
        pickle.dump(
            obj, f, pickle.HIGHEST_PROTOCOL)


index_dict = {
    "type": {"order": "0",
             "trade": "1"},
    "side": {"ask": "0",
             "bid": "1"},
    "action": {"place": "0",
               "cancel": "1"}
}


def get_binance_symbols(path_to_symbols):
    symbols = []
    if not os.path.exists(path_to_symbols):
        data = requests.get("https://api.binance.com/api/v3/exchangeInfo")
        data = data.json()
        print("data", data)
        for el in data:
            print(el)
        if "code" in data:
            if data["code"] == -1003:
                print("code -1003", data["msg"])
                return symbols
        for item in data["symbols"]:
            if item["status"] == "TRADING":
                symbols.append(item["symbol"])
        save_obj(symbols, path_to_symbols)
    else:
        symbols = get_obj(path_to_symbols)

    return symbols


class WSClient():
    def __init__(self, **kwargs):

        self.events = {}
        self.orderbooks_events = {}
        self.orderbook_response = {}
        self.trees = {}

        # self.delayWatcher = list()
        # with open("delayWatcher.txt", 'w') as f:
        #     f.flush()

        self.url = kwargs.get('url')
        self.sync_list = kwargs.get('sync_list')
        self.requests_list = kwargs.get('requests_list')
        self.thread_list = kwargs.get('thread_list')
        self.id_subprocess = multiprocessing.current_process().name
        self.id_subprocess = int(self.id_subprocess[self.id_subprocess.rfind('-') + 1:])
        self.thread_list.append(self.id_subprocess)
        time.sleep(1)

        self.thread_to_index = None
        self.index_to_thread = None

        self.reply_timeout = kwargs.get('reply_timeout') or 10
        self.ping_timeout = kwargs.get('ping_timeout') or 10
        self.sleep_time = kwargs.get('sleep_time') or 10
        self.callback = kwargs.get('callback')
        self.market_name = kwargs.get('market_name')
        self.symbols = kwargs.get('symbols')
        self.num_of_threads = kwargs.get('num_of_threads')
        self.lock_time = kwargs.get('lock_time')
        self.lock = kwargs.get('lock')
        self.block_time = kwargs.get('block_time') or 61
        self.was_lock = kwargs.get('was_lock') or False
        # httpcore.ConnectTimeout
        self.orderbook_depth = 1000
        self.delay = 0.15  # возможно 1200 запросов в минуту, 20 запросов в секунду
        self.last_request_timestamp = kwargs.get('last_request_timestamp')
        self.number_of_requests = 0
        self.total_symbol_num = len(get_binance_symbols('symbols.pkl'))

        for symbol in self.symbols:
            self.events[symbol] = {}
            self.orderbooks_events[symbol] = {}

        for symbol in self.symbols:
            self.events[symbol]['price'] = array.array('d', [0])
            self.events[symbol]['volume'] = array.array('d', [0])
            self.events[symbol]['timestamp'] = array.array('i', [0])
            self.events[symbol]['index'] = array.array('i', [0])
            self.orderbooks_events[symbol]['price'] = array.array('d', [0])
            self.orderbooks_events[symbol]['volume'] = array.array('d', [0])
            self.orderbooks_events[symbol]['timestamp'] = array.array('i', [0])
            self.orderbooks_events[symbol]['side'] = array.array('B', [0])

        self.orderbooks = {self.symbols[i]: {"ask": {}, "bid": {}} for i in range(len(self.symbols))}
        for tree_name in self.symbols:
            try:
                self.get_tree(tree_name)
            except BufferError:
                print("Can't get tree", tree_name)

            try:
                self.get_orderbook_tree(tree_name)
            except BufferError:
                print("Can't get tree", tree_name)

        self.orderbook_updates = {self.symbols[i]: {"U": [0], "u": [0], "timestamp": [0]} for i in
                                  range(len(self.symbols))}

        for symbol in self.symbols:
            self.orderbook_response[symbol] = {}

        self.autosave_number = 100 #100
        # словарь содержит пару (синхронизирован(bool), время последнего обновления(int)) для каждой торговой пары
        self.sync_orderbooks = {self.symbols[idx]: (False, 0) for idx
                                in range(len(self.symbols))}

    def form_orderbook(self, symbol):
        for ask in self.orderbook_response[symbol]['asks']:
            self.orderbooks[symbol]['ask'][ask[0]] = ask[1]
        for bid in self.orderbook_response[symbol]['bids']:
            self.orderbooks[symbol]['bid'][bid[0]] = bid[1]

    def get_request(self):
        request = copy.deepcopy(self.url)
        for symbol in self.symbols:
            request += symbol.lower() + "@trade/"
            request += symbol.lower() + "@depth/"
        return request[:-1]

    def get_tree(self, tree_name):
        self.trees[tree_name] = TTree(tree_name, tree_name)
        root_filename = self.market_name + '/' + tree_name + '.root'
        try:
            root_file = TFile(root_filename, 'update')
            if not root_file.IsZombie():
                has_tree = root_file.GetListOfKeys().Contains(tree_name)
                if has_tree:
                    tree_ff = root_file.Get(tree_name)
                    if not tree_ff:
                        print("====>CANNOT read tree header", tree_name)
                    self.trees[tree_name] = tree_ff.CloneTree(-1, "fast")
                    self.trees[tree_name].SetDirectory(0)
                    root_file.Close()
                branch_name = "Price"
                has_branch = False
                for b in self.trees[tree_name].GetListOfBranches():
                    if b.GetName() == branch_name:
                        has_branch = True
                        break
                if not has_branch:
                    self.trees[tree_name].Branch('Timestamp', self.events[tree_name]['timestamp'], 'timestamp/i')
                    self.trees[tree_name].Branch('Price', self.events[tree_name]['price'], 'price/d')
                    self.trees[tree_name].Branch('Volume', self.events[tree_name]['volume'], 'volume/d')
                    self.trees[tree_name].Branch('Index', self.events[tree_name]['index'], 'index/s')
                    #self.trees[tree_name].Write()
                else:
                    self.trees[tree_name].SetBranchAddress('Timestamp', self.events[tree_name]['timestamp'])
                    self.trees[tree_name].SetBranchAddress('Price', self.events[tree_name]['price'])
                    self.trees[tree_name].SetBranchAddress('Volume', self.events[tree_name]['volume'])
                    self.trees[tree_name].SetBranchAddress('Index', self.events[tree_name]['index'])
        except BufferError:
            print("File", root_filename, "can't open")

    def get_orderbook_tree(self, tree_name):
        symbol = tree_name
        root_filename = self.market_name + '/' + symbol + '.root'
        tree_name += '_orderbook'
        try:
            self.trees[tree_name] = TTree(tree_name, tree_name)
        except:
            print("Can't create", tree_name)

        try:
            root_file = TFile(root_filename, 'update')
            if not root_file.IsZombie():
                has_tree = root_file.GetListOfKeys().Contains(tree_name)
                if has_tree:
                    tree_ff = root_file.Get(tree_name)
                    if not tree_ff:
                        print("====>CANNOT read tree header", tree_name)
                    self.trees[tree_name] = tree_ff.CloneTree(-1, "fast")
                    self.trees[tree_name].SetDirectory(0)
                    root_file.Close()
                branch_name = "Price"
                has_branch = False
                for b in self.trees[tree_name].GetListOfBranches():
                    if b.GetName() == branch_name:
                        has_branch = True
                        break
                if not has_branch:
                    self.trees[tree_name].Branch('Timestamp', self.orderbooks_events[symbol]['timestamp'], 'timestamp/i')
                    self.trees[tree_name].Branch('Price', self.orderbooks_events[symbol]['price'], 'price/d')
                    self.trees[tree_name].Branch('Volume', self.orderbooks_events[symbol]['volume'], 'volume/d')
                    self.trees[tree_name].Branch('Side', self.orderbooks_events[symbol]['side'], 'side/O')
                    #self.trees[tree_name].Write()

                else:
                    self.trees[tree_name].SetBranchAddress('Timestamp', self.orderbooks_events[symbol]['timestamp'])
                    self.trees[tree_name].SetBranchAddress('Price', self.orderbooks_events[symbol]['price'])
                    self.trees[tree_name].SetBranchAddress('Volume', self.orderbooks_events[symbol]['volume'])
                    self.trees[tree_name].SetBranchAddress('Side', self.orderbooks_events[symbol]['side'])

        except BufferError:
            print("Orderbook file", root_filename, "can't open")

    def apply_orderbook_diff(self, market_data):
        symbol = market_data['s']
        timestamp = int(market_data['E'] / 1000)
        for side in ["ask", "bid"]:
            for ev in market_data[side[:1]]:
                if float(ev[1]):
                    self.orderbooks[symbol][side][ev[0]] = ev[1]
                else:
                    if self.orderbooks[symbol][side].get(ev[0], ""):
                        del self.orderbooks[symbol][side][ev[0]]
            self.orderbooks[symbol][side] = {k: self.orderbooks[symbol][side][k] for k in
                                             sorted(self.orderbooks[symbol][side], key=lambda x: float(x),
                                                    reverse=side == 'bid')}

        # if symbol not in ['BTCUSDT', 'ETHBTC']:
        #     return

        min_ask = min(self.orderbooks[symbol]['ask'].items(), key=lambda x: float(x[0]))
        max_bid = max(self.orderbooks[symbol]['bid'].items(), key=lambda x: float(x[0]))

        if True or int(time.time()) % 1 == 0:
            self.lock.acquire()

            if int(self.lock_time.value - time.time()) < -5*self.block_time:
                pass
                #TODO написать логику получения несинхронизированных пар и спросить стаканы

            try:
                sync = 0
                for el in self.sync_list:
                    sync += el

                req = 0
                for el in self.requests_list:
                    req += el

                # timestamp = int(market_data["E"] / 1000)
                # for side in ["ask", "bid"]:
                #     for order in market_data[side[:1]]:
                #         quantity = float(order[1])
                #         price = float(order[0])
                # with open("write_price.txt", "a") as fw:
                #     fw.write(str(self.record_event_number) + " " + str(round((float(min_ask[0])+float(max_bid[0]))/2., 2)) + "\n")
                print('sync1', sync, '/', self.total_symbol_num, 'req', req, symbol, 'EvNum', self.record_event_number, round(time.time() - global_time, 1),
                      int(self.lock_time.value - time.time()), (float(min_ask[0])+float(max_bid[0]))/2.)
            finally:
                self.lock.release()

    def write_event(self, tree_name, timestamp, price, quantity, index):

        self.events[tree_name]['price'][0] = price
        self.events[tree_name]['volume'][0] = quantity
        self.events[tree_name]['timestamp'][0] = timestamp
        self.events[tree_name]['index'][0] = int(index, 2)
        self.trees[tree_name].Fill()
        is_orderbook = tree_name.rfind('_orderbook') > 0

        # write_frequency = self.orderbook_depth if not is_orderbook else self.number_of_records_in_event
        # print('tree_name', tree_name, write_frequency, is_orderbook)
        if self.trees[tree_name].GetEntries() == self.record_event_number:
            # print('999999999999',self.record_event_number)
            try:
                sync = 0
                for el in self.sync_list:
                    sync += el
                root_filename = self.market_name + '/' + tree_name + '.root'
                rf = TFile(root_filename, 'update')
                if rf.IsOpen and not rf.IsZombie():
                    # self.trees[tree_name].Write("", TObject.kOverwrite)
                    self.trees[tree_name].Write()
                    self.trees[tree_name].AutoSave('SaveSelf')
                rf.Close()
                # self.delayWatcher.insert(0, self.record_event_number)
                # self.delayWatcher.append(int(time.time_ns()/1000000))
                    # time.sleep(1)
            except Exception as e:
                print("Event in", tree_name, "can't be written", e)

            # with open("delayWatcher.txt", 'a') as f:
            #     f.write('|'.join(str(elem) for elem in self.delayWatcher) + '\n')

            # print(self.eventType, 'sync1', sync, '/', self.total_symbol_num, price, self.record_event_number, round(time.time() - global_time, 1),
            #       quantity)

    def write_events(self, data):

        tree_name = data["s"]
        event_type = data["e"]
        self.eventType = event_type
        index_prefix = "00000"
        if event_type == "trade":
            self.record_event_number = self.trees[tree_name].GetEntries() + 1
            index = copy.deepcopy(index_prefix)
            index += index_dict["type"]["trade"]
            timestamp = int(data["T"] / 1000)  # convert to seconds
            price = float(data["p"])
            quantity = float(data["q"])
            if not data["m"]:
                index += index_dict["side"]["ask"]
            elif data["m"]:
                index += index_dict["side"]["bid"]
            index += index_dict["action"]["place"]

            #TODO update orderbook

            # update_orderbook_trades
            self.write_event(tree_name, timestamp, price, quantity, index)

        elif event_type == "depthUpdate":
            self.record_event_number = self.trees[tree_name].GetEntries() + len(data['a']) + len(data['b'])
            timestamp = int(data["E"] / 1000)
            for side in ["ask", "bid"]:
                for order in data[side[:1]]:
                    index = copy.deepcopy(index_prefix)
                    index += index_dict["type"]["order"]
                    index += index_dict["side"][side]
                    quantity = float(order[1])
                    price = float(order[0])
                    if quantity == 0:
                        index += index_dict["action"]["cancel"]
                        if self.orderbooks[tree_name][side].get(price, ""):
                            quantity = float(self.orderbooks[tree_name][side][price])
                    else:
                        index += index_dict["action"]["place"]
                    self.write_event(tree_name, timestamp, price, quantity, index)

    async def async_get_timeserver(self):
        async with httpx.AsyncClient() as client:
            try:
                r = await client.get('https://api.binance.com/api/v3/time')
                return r
            except BaseException as be:
                print("exeption", be)
                return 0




    async def async_write_orderbook(self, symbol):
        async with httpx.AsyncClient() as client:
            print("in orderbook hh", symbol)

            try:
                print("try get orderbook", symbol)
                # r = await client.get(
                #     f"https://www.binance.com/api/v1/depth?symbol={symbol}&limit=1000", timeout=300)

                r = await client.get(
                    f"https://www.binance.com/api/v1/depth?symbol={symbol}&limit={self.orderbook_depth}", timeout=5)
                print("r9567", r)

            except Exception as e:
                print("error2334546", str(e))
                self.sync_orderbooks[symbol] = (False, 0)
                with self.lock:
                    self.lock_time.value = 5 * self.block_time if self.was_lock.value else self.block_time
                    self.lock_time.value += time.time()
                    self.was_lock.value = True
                return None
            finally:
                with self.lock:
                    print("thread_to_index", self.thread_to_index)
                    if self.thread_to_index is not None:
                        self.requests_list[self.thread_to_index[self.id_subprocess]] += 1

            print('code', r.status_code, r.json())
            with self.lock:
                if r.status_code != 200:
                    print('r', r)
                    print("json", r.json())
                    print('blockkkkkkk')
                    self.lock_time.value = 5 * self.block_time if self.was_lock.value else self.block_time
                    self.lock_time.value += time.time()
                    self.was_lock.value = True
                    print(" self.lock_time.value", int(self.lock_time.value))
                    return
                else:
                    self.was_lock.value = False

            try:
                self.orderbook_response[symbol] = r.json()
            except Exception as e:
                print("Exception json conversion", str(e))
                self.lock_time.value = time.time() + self.block_time

            self.form_orderbook(symbol)
            self.sync_orderbooks[symbol] = (False, self.orderbook_response[symbol]['lastUpdateId'])
            tree_name = symbol + '_orderbook'
            for side in ["asks", "bids"]:
                for price, quantity in self.orderbook_response[symbol][side]:
                    price = float(price)
                    quantity = float(quantity)
                    self.orderbooks_events[symbol]['timestamp'][0] = int(
                        self.orderbook_response[symbol]['lastUpdateId'] / 1000)
                    # self.orderbooks_events[symbol]['timestamp'][0] = int(time.time()) #timestamp = int(data["T"] / 1000)  # convert to seconds
                    self.orderbooks_events[symbol]['price'][0] = price
                    self.orderbooks_events[symbol]['volume'][0] = quantity
                    self.orderbooks_events[symbol]['side'][0] = False if side == "asks" else True
                    self.trees[tree_name].Fill()
            root_filename = self.market_name + '/' + symbol + '.root'

            try:

                rf = TFile(root_filename, 'update')
                if rf.IsOpen and not rf.IsZombie():
                    self.trees[tree_name].Write("", TObject.kOverwrite)
                    self.trees[tree_name].AutoSave('SaveSelf')
                rf.Close()

            except BufferError as e:
                print("Cant' write orderbook", tree_name, str(e))
                self.sync_orderbooks[symbol] = (False, 0)

    async def update_orderbook(self, data):
        if data['e'] == "depthUpdate":
            symbol = data["s"]
            if not self.sync_orderbooks[symbol][0]:
                print(f'orderbook {symbol} not synchronized')
                # if orderbook was taken after last update
                # in this case we simple wait next update
                if self.sync_orderbooks[symbol][1] > data["u"]:
                    print(f'orderbook {symbol} update index above data update index synchronized', self.sync_orderbooks[symbol][1], data["u"])
                    return
                # if orderbook was taken after before update
                # in this case we take orderbook
                elif self.sync_orderbooks[symbol][1] + 1 < data["U"]:
                    print(f'orderbook {symbol} update index lower that update data index', self.sync_orderbooks[symbol][1] + 1, data["U"])

                    if (self.lock_time.value < time.time()) \
                            and (self.last_request_timestamp.value + self.delay < time.time()):
                        self.last_request_timestamp.value = time.time()
                        print('call orderbook', symbol)
                        await self.async_write_orderbook(symbol)
                        self.lock.acquire()
                        try:
                            req = 0
                            for el in self.requests_list:
                                req += el
                            # TODO magic number correspond allowed requests number
                            # начинает блочить со 120го запроса, так что каждые 110 запросов блочим на block_time 120 - threadnumber
                            if req and req % 110 == 0:

                                # если было залоченачинает блочить со 120го запроса, так что каждые 119 запросов блочим на block_time
                                self.lock_time.value = 5 * self.block_time if self.was_lock.value else self.block_time
                                self.lock_time.value += time.time()
                                print("huivam", int(self.lock_time.value), self.was_lock.value)
                                self.was_lock.value = True
                            else:
                                self.was_lock.value = False
                        finally:
                            self.lock.release()

                # in this case we can update current orderbook with given updates
                else:
                    print(f'orderbook {symbol} synchronized')
                    self.sync_orderbooks[symbol] = (True, self.orderbook_response[symbol]['lastUpdateId'])
                    tree_name = symbol + '_orderbook'
                    for side in ["asks", "bids"]:
                        for price, quantity in self.orderbook_response[symbol][side]:
                            price = float(price)
                            quantity = float(quantity)

                            self.orderbooks_events[symbol]['timestamp'][0] = int(data["E"] / 1000)  # convert to seconds
                            self.orderbooks_events[symbol]['price'][0] = price
                            self.orderbooks_events[symbol]['volume'][0] = quantity
                            self.orderbooks_events[symbol]['side'][0] = False if side == "asks" else True
                            self.trees[tree_name].Fill()
                    try:
                        root_filename = self.market_name + '/' + symbol + '.root'
                        rf = TFile(root_filename, 'update')
                        if rf.IsOpen and not rf.IsZombie():
                            self.trees[tree_name].Write("", TObject.kOverwrite)
                            self.trees[tree_name].AutoSave('SaveSelf')
                        rf.Close()

                    except BufferError as e:
                        print("Cant' write orderbook", tree_name, str(e))
                        self.sync_orderbooks[symbol] = (False, 0)

                    if self.thread_to_index is not None:
                        self.sync_list[self.thread_to_index[self.id_subprocess]] += 1

            if self.sync_orderbooks[symbol][0]:
                if self.sync_orderbooks[symbol][1] == data["U"] - 1 or \
                        self.sync_orderbooks[symbol][1] == data["U"] or (
                        (data["U"] < self.sync_orderbooks[symbol][1]) and (
                        self.sync_orderbooks[symbol][1] < data["u"])):
                    self.apply_orderbook_diff(data)
                    # orderbook last update
                    self.sync_orderbooks[symbol] = (True, data["u"])
                else:

                    # соответствует ситуации, когда в синхронизированный стакан приходи обновление до времени стакана.
                    # простоигнорируем. не рассинхронизируем стакан.
                    if self.sync_orderbooks[symbol][1] == data["u"]:
                        pass
                    else:
                        print("hhhuuul", self.sync_orderbooks[symbol][1], data["U"], data["u"])
                        self.sync_orderbooks[symbol] = (False, self.sync_orderbooks[symbol][1])
                        if self.thread_to_index is not None:
                            # print('sync -1')
                            self.sync_list[self.thread_to_index[self.id_subprocess]] -= 1

    async def listen_forever(self, requests):
        while True:
            # outer loop restarted every time the connection fails
            logger.debug('Creating new connection...')

            try:
                print("self.get_request()", requests)

                # async with websockets.unix_connect(requests) as ws:
                async with websockets.connect(requests, ping_interval=None, ssl=ssl.SSLContext(ssl.CERT_REQUIRED)) as ws:

                    while True:
                        # listener loop
                        try:
                            # logging.info(
                            #     "I am the child, with PID {}".format(getpid()))

                            try:
                                reply = await asyncio.wait_for(ws.recv(), timeout=self.reply_timeout)
                            except Exception as e:
                                print("Exeption in wait for recv", str(e))
                            try:
                                if 'stream' in reply:
                                    pass
                                else:
                                    print("stream is not in reply", reply.json)

                            except Exception as e:
                                self.lock_time.value = time.time() + self.block_time
                                logger.debug('Stream is not in reply. Requests will be lock until {} '.format(self.lock_time.value))

                            data = None
                            try:
                                data = json.loads(reply)['data']
                                # print(data)
                            except BufferError:
                                logger.debug('Cant convert to json string {}'.format(reply))
                            if data is None:
                                continue

                            # self.delayWatcher = [data["e"], data["T"] if data["e"] == "trade" else data["E"], int(time.time_ns()/1000000)]
                            if self.thread_to_index is None and len(self.thread_list) == len(self.sync_list):
                                self.thread_to_index = {self.thread_list[ix]: ix for ix in range(len(self.thread_list))}
                                self.index_to_thread = {ix: self.thread_list[ix] for ix in range(len(self.thread_list))}

                            self.write_events(data)

                            await self.update_orderbook(data)

                        except (asyncio.TimeoutError, websockets.ConnectionClosed):
                            # except :
                            ping = requests.get("https://api.binance.com/api/v3/ping")
                            print('in ping', ping)
                            if 'code' in ping:
                                pass
                            if ping.status_code == "200":
                                logger.debug('Ping OK, keeping connection alive...')
                                continue
                            else:
                                logger.debug(
                                    'Ping error - retrying connection in {} sec (Ctrl-C to quit)'.format(
                                        self.sleep_time))
                                await asyncio.sleep(self.sleep_time)
                                break

                        except Exception as e:
                            print("Extension", str(e))
                            logging.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", e)
                        # logger.debug('Server said > {}'.format(reply))

            except socket.gaierror:
                print("upal tut")
                logger.debug(
                    'Socket error - retrying connection in {} sec (Ctrl-C to quit)'.format(self.sleep_time))
                await asyncio.sleep(self.sleep_time)
                continue
            except ConnectionRefusedError:
                logger.debug('Nobody seems to listen to this endpoint. Please check the URL.')
                logger.debug('Retrying connection in {} sec (Ctrl-C to quit)'.format(self.sleep_time))
                await asyncio.sleep(self.sleep_time)
                continue
            except Exception as e:
                print(e.__context__)


logger = logging.getLogger(__name__)

logging.basicConfig(
    # stream=sys.stdout,
    level=logging.DEBUG,
    filename="sample.log",
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def start_ws_client(client):
    # Blocking call interrupted by loop.stop()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(client.listen_forever(client.get_request()))
    # try:
    #     loop.run_forever(client.listen_forever(client.get_request()))
    # finally:
    #     loop.close()

    # loop.create_unix_connection(client.listen_forever(client.get_request()))


def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--url',
                        required=False,
                        default="wss://stream.binance.com:9443/stream?streams=",
                        dest='url',
                        help='Websocket URL')

    parser.add_argument('--reply-timeout',
                        required=False,
                        dest='reply_timeout',
                        type=int,
                        help='Timeout for reply from server')

    parser.add_argument('--ping-timeout',
                        required=False,
                        dest='ping_timeout',
                        default=None,
                        help='Timeout when pinging the server')

    parser.add_argument('--sleep',
                        required=False,
                        type=int,
                        dest='sleep_time',
                        default=None,
                        help='Sleep time before retrieving connection')

    parser.add_argument('--symbols',
                        required=False,
                        default=['BTCUSDT'],
                        dest='symbols',
                        help='Symbol list')

    parser.add_argument('--num_of_threads',
                        dest='num_of_threads',
                        action="store",
                        type=int,
                        required=False,
                        default=1,
                        help="Количество используемых тредов")

    parser.add_argument('--num_symbol_in_request',
                        required=False,
                        default=10,
                        dest='num_symbol_in_request',
                        help='Количество символов, запрашиваемых одним запросом')

    return parser.parse_args()


def listen_socket(args, sync_list, requests_list, thread_list, lock_time, last_request_timestamp, lock, was_lock):
    args.sync_list = sync_list
    args.requests_list = requests_list
    args.thread_list = thread_list
    args.lock_time = lock_time
    args.last_request_timestamp = last_request_timestamp
    args.lock = lock
    args.was_lock = was_lock
    ws_client = WSClient(**vars(args))
    start_ws_client(ws_client)


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def show_sync(sync, requsts, threads):
    id_subprocess = multiprocessing.current_process().name
    id_subprocess = int(id_subprocess[id_subprocess.rfind('-') + 1:])
    while True:

        synced = 0
        for el in sync:
            synced += el

        num_req = 0
        for el in requsts:
            num_req += el
        process_duration = (time.time() - global_time)
        print("sync ", synced, num_req, num_req / process_duration, process_duration, threads)
        # print(sync)
        # print(requsts)

        time.sleep(1)


global_time = time.time()
time.sleep(2)

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    args = get_arguments()
    params = []
    logging.info(
        "I am the parent, with PID {}".format(getpid()))

    binance_symbols = get_binance_symbols('symbols.pkl')[:20]
    # binance_symbols = ['USDTTRY'] #['BTCUSDT', 'ETHBTC', 'ETHUSDT']
    print(binance_symbols, len(binance_symbols))

    # args.num_of_threads = 50 # len(binance_symbols)
    symbols_lists = np.array_split(binance_symbols, args.num_of_threads)

    for list in symbols_lists:
        list = list.tolist()
        params.append(copy.deepcopy(args))
        params[-1].symbols = list

        if args.url == "wss://stream.binance.com:9443/stream?streams=":
            market_name = 'binance'
        else:
            market_name = 'market_data'
        if not os.path.exists(market_name):
            os.mkdir(market_name)
        params[-1].market_name = market_name
        print('params[-1]', params[-1])

    manager = multiprocessing.Manager()
    sync_list = manager.list()
    requests_list = manager.list()
    thread_list = manager.list()
    last_request_timestamp = manager.Value('i', 0)
    lock_time = manager.Value('i', 0)
    lock = manager.Lock()

    # flag shows blocking state
    was_lock = manager.Value('i', False)

    print("thread_list ", thread_list)



    for i in range(args.num_of_threads):
        sync_list.append(0)
        requests_list.append(0)

    with multiprocessing.Pool(len(params), init_worker) as pool:
        try:
            # pool.apply_async(show_sync, args=(sync_list, requests_list, thread_list))
            for arg in params:
                res = pool.apply_async(listen_socket, args=(
                arg, sync_list, requests_list, thread_list, lock_time, last_request_timestamp, lock, was_lock))
                # print('res', res.get(timeout=10))

            pool.close()
            pool.join()

        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            pool.join()
