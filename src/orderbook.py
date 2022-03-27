import argparse

from ROOT import TObject, TFile, TTree, AddressOf
import array
import random
import time
import os
import numpy as np

os.environ["OMP_NUM_THREADS"] = "1"
import multiprocessing


def read_tree(root_file_name, tree_name):
    rf = TFile(root_file_name, 'read')


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
    orderbook = {"ask": {}, "bid": {}, "timestamp": current_orderbook_time}
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

        orderbook[side_map[orderbook_side[0]]][orderbook_price[0]] = orderbook_volume[0]

        # print("0.0078125", orderbook_price[0]/0.0078125, type(orderbook_price[0]))
    print(orderbook)
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

                if read_bytes == 14:

                    if index[0] < 4:
                        printCounter = 0

                        # разместить ордер в аске
                        if index[0] == 0:
                            # if price[0] in orderbook['ask']:
                            #     print("hurayyyy")
                            # else:
                            #     print("hui")
                            orderbook['ask'][price[0]] = volume[0]
                            print(tree_name, 'placed ask', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                  index[0], read_bytes, current_orderbook_time)

                        # отменить ордер в аске
                        #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 1)
                        if index[0] == 1:
                            orderbook['ask'][price[0]] = 0
                            del orderbook['ask'][price[0]]
                            print(tree_name, 'canceled ask', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                  index[0], read_bytes, current_orderbook_time)

                        # разместить ордер в биде
                        #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 2)
                        if index[0] == 2:
                            orderbook['bid'][price[0]] = volume[0]
                            print(tree_name, 'placed bid', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                  index[0], read_bytes, current_orderbook_time)

                        # отменить ордер в биде
                        #read_bytes = orderbook_updates_tree.GetEntryWithIndex(current_orderbook_time, 3)
                        if index[0] == 3:
                            orderbook['bid'][price[0]] = 0
                            del orderbook['bid'][price[0]]
                            print(tree_name, 'canceled bid', current_time - timestamp[0], price[0], volume[0], timestamp[0],
                                  index[0], read_bytes, current_orderbook_time)

                    current_orderbook_time = timestamp[0]

                updates_index += 1

            # Если больше ничего не читается (якобы синхронизировался стакан) - пишем цену в стакане
            if printCounter == 0:
                printCounter += 1
                min_ask = min(orderbook['ask'].items(), key=lambda x: float(x[0]))
                max_bid = max(orderbook['bid'].items(), key=lambda x: float(x[0]))
                print('sync1', current_orderbook_time, (float(min_ask[0]) + float(max_bid[0])) / 2.)

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


index_dict = {
    "type": {"order": "0",
             "trade": "1"},
    "side": {"ask": "0",
             "bid": "1"},
    "action": {"place": "0",
               "cancel": "1"}
}

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

    parser.add_argument('--symbol',
                        required=False,
                        dest='symbol',
                        default="BTCUSDT",
                        type=str,
                        help='symbol')
    return parser.parse_args()



if __name__ == '__main__':

    start = time.time()
    args = get_arguments()
    root_filename = args.market + '/' + args.symbol + '.root'

    tree_name = root_filename[root_filename.rfind('/') + 1:root_filename.rfind('.')]
    read_tree(root_filename, tree_name)
