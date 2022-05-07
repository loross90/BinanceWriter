import argparse
from ROOT import TObject, TFile, TTree, AddressOf
import array
import asyncio
import random
import json
import time
import copy
import os
import numpy as np
import multiprocessing
os.environ["OMP_NUM_THREADS"] = "1"
import PairsPrepairer
from prettytable import PrettyTable

async def read_tree(root_file_name, tree_name, inverted_pair, all_dict, base_dict, lock):
        rf = TFile(root_file_name, 'read')

        # with open("delayReader_" + tree_name + ".txt", "w") as f:
        #     f.flush()

        # map_dict = {'USDT': 0, 'BTC': 1, 'ETH': 2}
        #
        # all_dict[map_dict[tree_name[0:3]]] = [0] * len(all_dict[0])

        # if tree_name == 'BTCUSDT':
        #     all_list[1] = [1,2,3,4,5]
        #
        # if tree_name == 'ETHBTC':
        #     all_list[2] = [6,7,8,9,10]

        orderbook_price = array.array('d', [0])
        orderbook_volume = array.array('d', [0])
        orderbook_timestamp = array.array('i', [0])
        orderbook_side = array.array('B', [0])
        orderbook_tree_name = tree_name + '_orderbook'

        orderbook_tree = rf.Get(orderbook_tree_name)
        orderbook_tree.SetBranchAddress("Timestamp", orderbook_timestamp)
        orderbook_tree.SetBranchAddress("Price", orderbook_price)
        orderbook_tree.SetBranchAddress("Volume", orderbook_volume)
        orderbook_tree.SetBranchAddress("Side", orderbook_side)

        current_orderbook_entry = orderbook_tree.GetEntries()
        orderbook_tree.GetEntry(current_orderbook_entry - 1)
        print(orderbook_tree_name, 'i', current_orderbook_entry, orderbook_price[0], orderbook_volume[0], orderbook_timestamp[0], orderbook_side[0])

        current_orderbook_time = orderbook_timestamp[0]

        price = array.array('d', [0])
        volume = array.array('d', [0])
        timestamp = array.array('i', [0])
        index = array.array('i', [0])
        orderbook_updates_tree = rf.Get(tree_name)

        orderbook_updates_tree.SetBranchAddress("Timestamp", timestamp)
        orderbook_updates_tree.SetBranchAddress("Price", price)
        orderbook_updates_tree.SetBranchAddress("Volume", volume)
        orderbook_updates_tree.SetBranchAddress("Index", index)

        orderbook_updates_tree.BuildIndex("Timestamp", "Index")

        # receive = array.array('i', [0])
        # write = array.array('i', [0])
        # event_timestamps = rf.Get(tree_name + '_timestamp')
        # event_timestamps.SetBranchAddress('Receive', receive)
        # event_timestamps.SetBranchAddress('Write', write)


        ind = 0
        orderbook = {}
        # USDT_list = [1.0, 10.0, 100.0, 1000.0, 10000.0]
        all_dict[tree_name] = {'ask': [(0, 0)] * len(all_dict['USDT']['ask']), 'bid': [(0, 0)] * len(all_dict['USDT']['bid'])}
        orderbook[tree_name] = {"ask": {}, "bid": {}, "timestamp": current_orderbook_time}

        side_map = {0: "ask", 1: "bid"}
        prev_val = None
        curr_val = None
        min_diff = None

        await asyncio.sleep(0)

        while current_orderbook_time == orderbook_timestamp[0] and current_orderbook_entry - ind > 0:
            # print("current_orderbook_time", current_orderbook_time, "orderbook_timestamp[0]",orderbook_timestamp[0])
            if curr_val is not None:
                prev_val = curr_val
            ind += 1
            orderbook_tree.GetEntry(current_orderbook_entry - ind)
            yeap = current_orderbook_time != orderbook_timestamp[0]
            if yeap:
                continue
            # print(orderbook_tree_name, 'i', current_orderbook_entry - ind, orderbook_price[0], orderbook_volume[0],
            #       orderbook_timestamp[0], orderbook_side[0], yeap)

            curr_val = orderbook_price[0]
            if prev_val is not None:
                if min_diff is None:
                    min_diff = np.abs(curr_val - prev_val)
                    # print('1', curr_val, prev_val, min_diff)
                else:
                    min_diff = np.minimum(min_diff, np.abs(curr_val - prev_val))
                    # print('2', curr_val, prev_val, abs(curr_val - prev_val), min_diff)

            orderbook[tree_name][side_map[orderbook_side[0]]][orderbook_price[0]] = orderbook_volume[0]

            # print("0.0078125", orderbook_price[0]/0.0078125, type(orderbook_price[0]))
        # print(orderbook[tree_name])
        # print("min", min_diff)
        # print(orderbook)
        # last_entry = None
        # print("here")
        # current_entry = orderbook_updates_tree.GetEntries()
        # print('current_entry', current_entry)
        #
        # orderbook_updates_tree.GetEntry(current_entry - 1)
        # print(tree_name, 'i', current_entry, price[0], volume[0], timestamp[0], index[0])
        #
        # action = "{0:b}".format(index[0])
        # if len(action) > 2: # remove trades
        #     pass
        #
        # print('action', action)

        # orderbook_updates_tree.Print()

        # print(orderbook)
        # last_entry = None
        #
        # while True:
        #     current_entry = orderbook_updates_tree.GetEntries()
        #
        #     if last_entry is None:
        #         last_entry = current_entry
        #     for i in range(last_entry, current_entry):
        #         orderbook_updates_tree.GetEntry(i)
        #         print('i', i, "price", price, "volume", volume, "timestamp", timestamp, "index", index)
        #     last_entry = current_entry
        #     orderbook_updates_tree.Refresh()

        updates_index = orderbook_updates_tree.GetEntryNumberWithBestIndex(current_orderbook_time, 0) # текущий номер записи в дереве обновлений ордербука
        orderbook_updates_tree.GetEntry(updates_index)
        while timestamp[0] == current_orderbook_time:
            updates_index -= 1
            orderbook_updates_tree.GetEntry(updates_index)
        updates_index += 1
        printCounter = 0

        await asyncio.sleep(0)

        while True:
            try:
                # current_entry = orderbook_updates_tree.GetEntries()
                current_time = int(time.time())
                # print('current_entry', current_orderbook_time, current_time)
                while updates_index + 1 < orderbook_updates_tree.GetEntries():
                    # print('current_orderbook_time', current_orderbook_time)

                    read_bytes = orderbook_updates_tree.GetEntry(updates_index)

                    # with open("delayReader_" + tree_name + ".txt", "a") as f:
                    #     f.write(str(updates_index) + "|" + str(int(time.time_ns()/1000000)) + "\n")

                    if read_bytes == 14:

                        if index[0] < 4:
                            printCounter = 0

                            # разместить ордер в аске
                            if index[0] == 0:
                                # if price[0] in orderbook['ask']:
                                #     print("hurayyyy")
                                # else:
                                #     print("hui")
                                orderbook[tree_name]['ask'][price[0]] = volume[0]
                                # print(tree_name, 'placed ask', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                #       index[0], read_bytes, current_orderbook_time)

                            # отменить ордер в аске
                            #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 1)
                            if index[0] == 1:
                                orderbook[tree_name]['ask'][price[0]] = 0
                                del orderbook[tree_name]['ask'][price[0]]
                                # print(tree_name, 'canceled ask', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                #       index[0], read_bytes, current_orderbook_time)

                            # разместить ордер в биде
                            #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 2)
                            if index[0] == 2:
                                orderbook[tree_name]['bid'][price[0]] = volume[0]
                                # print(tree_name, 'placed bid', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                #       index[0], read_bytes, current_orderbook_time)

                            # отменить ордер в биде
                            #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 3)
                            if index[0] == 3:
                                orderbook[tree_name]['bid'][price[0]] = 0
                                del orderbook[tree_name]['bid'][price[0]]
                                # print(tree_name, 'canceled bid', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                #       index[0], read_bytes, current_orderbook_time)

                        current_orderbook_time = timestamp[0]

                    updates_index += 1

                    await asyncio.sleep(0)

                await asyncio.sleep(0)

                # Если больше ничего не читается (якобы синхронизировался стакан) - пишем цену в стакане
                if printCounter == 0:
                    printCounter += 1
                    with lock:
                        try:
                            min_ask = min(orderbook[tree_name]['ask'].items(), key=lambda x: float(x[0]))
                            max_bid = max(orderbook[tree_name]['bid'].items(), key=lambda x: float(x[0]))

                            if not inverted_pair:
                                ask_list = fill_availables(orderbook[tree_name]['ask'],
                                                           'ask',
                                                           False,
                                                           (i[0] for i in all_dict[base_dict]['ask']),
                                                           inverted_pair) #False for ask and True for bid; it affects sort method for keys in orderbook
                                bid_list = fill_availables(orderbook[tree_name]['bid'],
                                                           'bid',
                                                           True,
                                                           (i[0] for i in all_dict[base_dict]['bid']),
                                                           inverted_pair)
                            else:
                                ask_list = fill_availables(orderbook[tree_name]['bid'],
                                                           'bid',
                                                           True,
                                                           (i[0] for i in all_dict[base_dict]['bid']),
                                                           inverted_pair)
                                bid_list = fill_availables(orderbook[tree_name]['ask'],
                                                           'ask',
                                                           False,
                                                           (i[0] for i in all_dict[base_dict]['ask']),
                                                           inverted_pair)

                            all_dict[tree_name] = {'ask': ask_list, 'bid': bid_list}

                            # print(json.dumps(all_dict[tree_name], indent=2, default=str))
                            print('sync1', tree_name, (float(min_ask[0]) + float(max_bid[0])) / 2., str(all_dict[tree_name]['ask']), '\n')  # current_orderbook_time - тоже писать надо потом

                            # print_orderbook(tree_name, orderbook[tree_name], 4) # IMPORTIANT THING

                            # arb_table = PrettyTable(['USDT amount', 'ETHUSDT', 'ETHBTC -> BTCUSDT', 'diff'])
                            # for i in range(len(all_dict['ETHUSDT']['ask'])):
                            #     arb_table.add_row([all_dict['USDT']['ask'][i][0],
                            #                       all_dict['ETHUSDT']['ask'][i][0],
                            #                       all_dict['ETHBTC']['ask'][i][0],
                            #                       abs(all_dict['ETHUSDT']['ask'][i][0] - all_dict['ETHBTC']['ask'][i][0])])
                            # print(arb_table)

                            # print_dollar_eqv_table(tree_name, all_dict['USDT'], all_dict[tree_name]) # IMPORTIANT THING

                        except Exception:
                            pass

                orderbook_updates_tree.Refresh()
                orderbook_updates_tree.BuildIndex("Timestamp", "Index")

                await asyncio.sleep(0)
                # min_ask = min(orderbook['ask'].items(), key=lambda x: float(x[0]))
                # max_bid = max(orderbook['bid'].items(), key=lambda x: float(x[0]))
                #
                # print('sync1', (float(min_ask[0]) + float(max_bid[0])) / 2.)

                # time.sleep(2.0)

            except BaseException as e:
                print("exep", e)
                break

        # while True:
        #     try:
        #         current_entry = orderbook_updates_tree.GetEntries()
        #         read_bytes = orderbook_updates_tree.GetEntry(current_entry - 1)
        #         if read_bytes <= 0:
        #             print(tree_name, 'hui', current_entry, price[0], volume[0], timestamp[0], index[0], read_bytes)
        #             # continue
        #         print(tree_name, 'current', current_entry, price[0], volume[0], timestamp[0], index[0], read_bytes)
        #
        #         # if current_orderbook_time < timestamp[0]:
        #         #"", TObject.kOverwrite
        #
        #         #     current_orderbook_time += 1
        #         #     print(tree_name, 'i2', current_orderbook_time, price[0], volume[0], timestamp[0], index[0])
        #         #
        #         #     action = "{0:b}".format(index[0])
        #         #     print('action', action)
        #
        #         orderbook_updates_tree.Refresh()
        #         time.sleep(0.5)
        #     except BaseException as e:
        #         print("exep", e)
        #         break
        # orderbook_updates_tree.GetUserInfo().Add(orderbook_updates_tree, "eto hui")




        # orderbook_updates_tree.Scan("Price:Volume")

        # orderbook_updates_tree.StartViewer()



        # for entry in orderbook_updates_tree:
        #     # Now you have acess to the leaves/branches of each entry in the tree, e.g.
        #     # events = entry.events
        #     print('entry', entry)

        # self.orderbooks_events[symbol]['price'] = array.array('d', [0])
        # self.orderbooks_events[symbol]['volume'] = array.array('d', [0])
        # self.orderbooks_events[symbol]['timestamp'] = array.array('i', [0])
        # self.orderbooks_events[symbol]['side'] = array.array('B', [0])
        #
        # self.trees[tree_name].SetBranchAddress('Timestamp', self.events[tree_name]['timestamp'])
        # self.trees[tree_name].SetBranchAddress('Price', self.events[tree_name]['price'])
        # self.trees[tree_name].SetBranchAddress('Volume', self.events[tree_name]['volume'])
        # self.trees[tree_name].SetBranchAddress('Index', self.events[tree_name]['index'])
        # # while True:
        #
        #     current_entry = rf.Get(tree_name).GetEntries()
        #     print()
        #     if last_entry is None:
        #         print("last_entry", last_entry)
        #         print("current_entry", current_entry)
        #         last_entry = current_entry
        #     for i in range(last_entry, current_entry):
        #         rf.Get(tree_name).GetEntry(i)
        #         print(tree_name, 'i', i, price[0], volume[0], timestamp[0], index[0])
        #
        #     last_entry = current_entry
        #     rf.Get(tree_name).Refresh()

def print_orderbook(tree_name, ob, deepth):
    table = PrettyTable(['Price', 'Volume'])
    table.title = tree_name + ' orderbook'
    for price in reversed(sorted(ob['ask'])[:deepth]):
        table.add_row([price, ob['ask'][price]])
    table.add_row(['-----'] * 2)
    for price in sorted(ob['bid'], reverse=True)[:deepth]:
        table.add_row([price, ob['bid'][price]])
    print(table)

def print_dollar_eqv_table(tree_name, dollar_dict, pair_dict):
    pair_table = PrettyTable(['', '$ amount', 'volume', 'av. price'])
    pair_table.title = tree_name
    pair_table.add_row(['ask', '', '', ''])
    for i in range(len(dollar_dict['ask'])):
        pair_table.add_row(['', dollar_dict['ask'][i][0],
                            pair_dict['ask'][i][0],
                            pair_dict['ask'][i][1]])
    pair_table.add_row(['bid', '', '', ''])
    for i in range(len(dollar_dict['bid'])):
        pair_table.add_row(['', dollar_dict['bid'][i][0],
                            pair_dict['bid'][i][0],
                            pair_dict['bid'][i][1]])
    print(pair_table)

def get_tree_name(filename):
    root_file = TFile(filename, 'read')
    symbols = []
    for key in root_file.GetListOfKeys():
        print("key", key)
        if key.GetClassName() == 'TTree':
            symbols.append(key.GetName())
    symbols = set(symbols)
    print(symbols)
    return symbols

def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--market',
                        required=False,
                        dest='market',
                        default="binance",
                        type=str,
                        help='crypto market')

    parser.add_argument('--symbols',
                        required=False,
                        dest='symbols',
                        default="BTCUSDT",
                        type=str,
                        help='symbols array')

    parser.add_argument('--num_of_threads',
                        dest='num_of_threads',
                        action="store",
                        type=int,
                        required=False,
                        default=1,
                        help="Количество используемых тредов")
    return parser.parse_args()

def fill_availables(ob, ob_part, reverse_sort, base_list, inv_pair):
    """
    Used to count amount of coins available to buy.
    @param ob: Orderbook
    @param ob_part: ask or bid
    @param reverse_sort: Type of sort aplied to orderbook; reversed for bid, straight for ask
    @param base_list: List with available amount of coins of base pair.
    @param inv_pair: Pair is reversed or not. f.e.: BTCUSDT is not reversed, but USDTVAL is reversed
    @return: List of tuples, each corresponding to amount of $, given in 'ref_amounts'
    """
    target_list = []
    fee = 0.001 # Later fee needed to be calculated properly
    sorted_prices = sorted(ob, reverse=reverse_sort)
    for price in base_list:
        if not inv_pair:
            if ob_part == 'ask':
                target_list.append(get_available_volume_ask(ob, sorted_prices, price, fee))
            if ob_part == 'bid':
                target_list.append(get_available_volume_bid(ob, sorted_prices, price, fee))
        if inv_pair:
            if ob_part == 'ask':
                target_list.append(get_available_volume_bid_r(ob, sorted_prices, price, fee))
            if ob_part == 'bid':
                target_list.append(get_available_volume_ask_r(ob, sorted_prices, price, fee))
    return target_list

def get_available_volume_ask(ob, sorted_prices, amount, fee_rate): # ob - orderbook; amount - available quantity of qAsset; fee_rate - in r.u., not in percents
    volume = 0
    init = amount
    for i in sorted_prices:
        if ob[i] * i >= amount * (1 - fee_rate):
            volume += amount * (1 - fee_rate) / i # in fact, here 'volume =  (amount / (ob[i] * i)) * ob[i]'
            # since we need the fraction (amount / (ob[i] * i)) of the available ob[i]
            amount = 0
            break
        volume += ob[i]
        amount -= ob[i] * i / (1 - fee_rate)
    if amount > 0:
        volume = 0 # Here we assume that we can not satisfy deal for 'amount' of qAsset
        # and we would not make a deal at all - the volume changed = 0
        # raise ValueError("There is not enough volume in orderbook!")
    return (volume, init/volume if volume != 0 else 0)

def get_available_volume_bid(ob, sorted_prices, amount, fee_rate):
    volume = 0
    init = amount
    for i in sorted_prices:
        if ob[i] * i * (1 - fee_rate) >= amount:
            volume += amount / ((1 - fee_rate) * i)  # in fact, here 'volume =  (amount / (ob[i] * i)) * ob[i]'
            # since we need the fraction (amount / (ob[i] * i)) of the available ob[i]
            amount = 0
            break
        volume += ob[i]
        amount -= ob[i] * i * (1 - fee_rate)
    if amount > 0:
        volume = 0  # Here we assume that we can not satisfy deal for 'amount' of qAsset
        # and we would not make a deal at all - the volume changed = 0
        # raise ValueError("There is not enough volume in orderbook!")
    return (volume, init / volume if volume != 0 else 0)

def get_available_volume_ask_r(ob, sorted_prices, amount, fee_rate): # ob - orderbook; amount - available quantity of bAsset;
    # Эта функция для случаев, когда работа идет с инвертироваными парами (USDT-COIN или BTC-COIN например); отмечены флагом reversed в пути
    # При заполнении доступных ask в долларовых эквивалентах для той пары нужно на вход здесь подавать bid стакан пары
    # А функция будет уменьшать amount на основе значений словаря стакана, а не на основе произведений ключа и значения
    # Как в обычной функции
    volume = 0
    init = amount
    for i in sorted_prices:
        if ob[i] >= amount:
            volume += i * amount / (1 - fee_rate)
            amount = 0
            break
        volume += i * ob[i] / (1 - fee_rate)
        amount -= ob[i]
    if amount > 0:
        volume = 0 # Here we assume that we can not satisfy deal for 'amount' of qAsset
        # and we would not make a deal at all - the volume changed = 0
        # raise ValueError("There is not enough volume in orderbook!")
    return (volume, init/volume if volume != 0 else 0)

def get_available_volume_bid_r(ob, sorted_prices, amount, fee_rate):# ob - orderbook; amount - available quantity of bAsset;
    # Эта функция для случаев, когда работа идет с инвертироваными парами (USDT-COIN или BTC-COIN например); отмечены флагом reversed в пути
    # При заполнении доступных ask в долларовых эквивалентах для той пары нужно на вход здесь подавать bid стакан пары
    # А функция будет уменьшать amount на основе значений словаря стакана, а не на основе произведений ключа и значения
    # Как в обычной функции
    volume = 0
    init = amount
    for i in sorted_prices:
        if ob[i] >= amount:
            volume += i * amount * (1 - fee_rate)
            amount = 0
            break
        volume += i * ob[i] * (1 - fee_rate)
        amount -= ob[i]
    if amount > 0:
        volume = 0 # Here we assume that we can not satisfy deal for 'amount' of qAsset
        # and we would not make a deal at all - the volume changed = 0
        # raise ValueError("There is not enough volume in orderbook!")
    return (volume, init/volume if volume != 0 else 0)


def fee_rate_calculator(turnover, balance, base_symbol, trade_type):

    return fee_reduce_multiplier(base_symbol) * 0.001

def fee_reduce_multiplier(base_symbol):
    if base_symbol in fee_discounts:
        return (1 - fee_discounts[base_symbol])
    return 1

fee_discounts = {
    'BNB' : 0.25
}

index_dict = {
    "type": {"order": "0",
             "trade": "1"},
    "side": {"ask": "0",
             "bid": "1"},
    "action": {"place": "0",
               "cancel": "1"}
}

async def run_read_trees(loop, pairs_list, all_pairs, all_pair_dict, lock):
    fs = []
    for pair_name in pairs_list:
        root_filename = 'binance' + '/' + pair_name + '.root'
        tree_name = root_filename[root_filename.rfind('/') + 1:root_filename.rfind(
            '.')]  # makes (for example) from 'binance/BTCUSDT.root' 'BTCUSDT'
        if tree_name.__contains__('USDT'):
            base_dict = 'USDT'
        else:
            base_dict = all_pairs[tree_name]['path'][1]
        inverted_pair = all_pairs[tree_name]['reversed'][0]
        fs.append(loop.create_task(read_tree(root_filename, tree_name, inverted_pair, all_pair_dict, base_dict, lock)))
    await asyncio.wait(fs)

def start_reading_processes(pairs_list, all_pairs, all_pair_dict, lock):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_read_trees(loop, pairs_list, all_pairs, all_pair_dict, lock))
    loop.close()

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    params = []
    start = time.time()
    args = get_arguments()

    PairsPrepairer.pairs_prepare('https://api3.binance.com/api/v3/exchangeInfo', 'pairs.pkl', True)
    pairs = PairsPrepairer.get_obj('pairs.pkl')

    ref_amounts = [(1, 0), (10, 0), (100, 0), (1000, 0), (10000, 0), (100000, 0)]#, (100000, 0), (1000000, 0), (10000000, 0)]

    manager = multiprocessing.Manager()
    lock = manager.Lock()
    all_pair_dict = manager.dict()
    all_pair_dict['USDT'] = {'ask': ref_amounts,
                             'bid': ref_amounts}

    binance_symbols = PairsPrepairer.get_obj('symbols.pkl')[:20]
    # binance_symbols = ['BTCUSDT']

    # args.symbols = binance_symbols
    # args.num_of_threads = len(args.symbols)
    print(binance_symbols, len(binance_symbols))

    symbols_lists = np.array_split(binance_symbols, args.num_of_threads)

    for list in symbols_lists:
        list = list.tolist()
        params.append(copy.deepcopy(args))
        params[-1].symbols = list
        print('params[-1]', params[-1])

    with multiprocessing.Pool(len(params)) as pool:
        try:
            for arg in params:
                pool.apply_async(start_reading_processes, args=(arg.symbols, pairs, all_pair_dict, lock))
            pool.close()
            pool.join()

        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            pool.join()