import argparse
from ROOT import TObject, TFile, TTree, AddressOf
import array
import random
import time
import os
import numpy as np
import multiprocessing
import asyncio
from shared_memory_dict import SharedMemoryDict
from threading import Thread
os.environ["OMP_NUM_THREADS"] = "1"
import binance_writer
import  PairsPrepairer

def read_tree(root_file_name, tree_name, all_dict, base_dict, lock):
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

        ind = 0
        orderbook = {}
        # USDT_list = [1.0, 10.0, 100.0, 1000.0, 10000.0]
        all_dict[tree_name] = {'ask': [(0, 0)] * len(all_dict['USDT']['ask']), 'bid': [(0, 0)] * len(all_dict['USDT']['bid'])}
        orderbook[tree_name] = {"ask": {}, "bid": {}, "timestamp": current_orderbook_time}

        side_map = {0: "ask", 1: "bid"}
        prev_val = None
        curr_val = None
        min_diff = None

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
        print(orderbook[tree_name])
        #print("min", min_diff)
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

        orderbook_updates_tree.Print()

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

                # Если больше ничего не читается (якобы синхронизировался стакан) - пишем цену в стакане
                if printCounter == 0:
                    printCounter += 1
                    min_ask = min(orderbook[tree_name]['ask'].items(), key=lambda x: float(x[0]))
                    max_bid = max(orderbook[tree_name]['bid'].items(), key=lambda x: float(x[0]))
                    print('sync1', tree_name, (float(min_ask[0]) + float(max_bid[0])) / 2.) #current_orderbook_time - тоже писать надо потом
                    try:
                        ask_list = fill_availables(orderbook[tree_name]['ask'], False, (i[0] for i in all_dict[base_dict]['ask'])) #False for ask and True for bid; it affects sort method for keys in orderbook
                        bid_list = fill_availables(orderbook[tree_name]['bid'], True, (i[0] for i in all_dict[base_dict]['bid']))
                        all_dict[tree_name] = {'ask': ask_list, 'bid': bid_list}
                        print(tree_name + ' ask', all_dict[tree_name]['ask'], sep=', ', end='\n')
                        str(all_dict[tree_name]['ask'])

                    except Exception:
                        pass
                    # print('bid ', end='\n')
                    # print(all_dict[base_dict]['bid'], sep=', ', end='\n')
                    # fill_availables(tree_name, 'ask', orderbook[tree_name], locals()[tree_name[0:3] + '_list'], locals()[tree_name[3:] + '_list'])
                    # print(print(*locals()[tree_name[0:3] + '_list'], sep=', '))
                # print("left ", current_time - current_orderbook_time)

                orderbook_updates_tree.Refresh()
                orderbook_updates_tree.BuildIndex("Timestamp", "Index")

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
    return parser.parse_args()

def fill_availables(ob, reverse_sort, base_list):
    target_list = []
    sorted_prices = sorted(ob, reverse=reverse_sort)
    for price in base_list:
        target_list.append(get_available_volume(ob, sorted_prices, price))
    return target_list

def get_available_volume(ob, sorted_prices, amount): # ob - orderbook; amount - available of qAsset;
    volume = 0
    init = amount
    for i in sorted_prices:
        if ob[i] * i >= amount:
            volume += amount / i
            amount = 0
            break
        volume += ob[i]
        amount -= ob[i] * i
    if amount > 0:
        raise ValueError("There is not enough volume in orderbook!")
    return (volume, init/volume if volume != 0 else 0)

index_dict = {
    "type": {"order": "0",
             "trade": "1"},
    "side": {"ask": "0",
             "bid": "1"},
    "action": {"place": "0",
               "cancel": "1"}
}

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    params = []
    start = time.time()
    args = get_arguments()

    PairsPrepairer.pairs_prepare('https://api3.binance.com/api/v3/exchangeInfo', 'pairs.pkl', True)
    pairs = PairsPrepairer.get_obj('pairs.pkl')

    # root_filename = args.market + '/' + args.symbol + '.root'
    # tree_name = root_filename[root_filename.rfind('/') + 1:root_filename.rfind('.')]
    #read_tree(root_filename, tree_name)

    manager = multiprocessing.Manager()
    lock = manager.Lock()
    all_pair_dict = manager.dict()
    all_pair_dict['USDT'] = {'ask': [(1, 0), (10, 0), (100, 0), (1000, 0), (10000, 0), (100000, 0)], 'bid': [(1, 0), (10, 0), (100, 0), (1000, 0), (10000, 0), (100000, 0)]}
    # BTC_list = manager.list()
    # ETH_list = manager.list()
    args.symbols = ['BTCUSDT', 'ETHBTC', 'ETHUSDT'] #['BTCUSDT']
    # args.symbols = binance_writer.get_binance_symbols('symbols.pkl')[:19]

    with multiprocessing.Pool(len(args.symbols)) as pool:
        try:
            # for i in range(5):
                # BTC_list.append(0)
                # ETH_list.append(0)

            for pairName in args.symbols:
                all_pair_dict[pairName] = {}
                root_filename = args.market + '/' + pairName + '.root'
                tree_name = root_filename[root_filename.rfind('/') + 1:root_filename.rfind('.')] # makes (for example) from 'binance/BTCUSDT.root' 'BTCUSDT'
                if tree_name.endswith('USDT'):
                    base_dict = 'USDT'
                else:
                    base_dict = pairs[tree_name]['path'][1]
                pool.apply_async(read_tree, args=(root_filename, tree_name, all_pair_dict, base_dict, lock))
            pool.close()
            pool.join()

        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
            pool.join()