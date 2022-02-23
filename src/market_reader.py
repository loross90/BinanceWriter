import argparse

from ROOT import TObject, TFile, TTree, AddressOf
import array
import random
import time
import os

os.environ["OMP_NUM_THREADS"] = "1"
import multiprocessing


def read_tree(root_file_name, tree_name):
    rf = TFile(root_file_name, 'read')
    price = array.array('d', [0])
    volume = array.array('d', [0])
    timestamp = array.array('i', [0])
    index = array.array('b', [0])
    rf.Get(tree_name).SetBranchAddress("Timestamp", timestamp)
    rf.Get(tree_name).SetBranchAddress("Price", price)
    rf.Get(tree_name).SetBranchAddress("Volume", volume)
    rf.Get(tree_name).SetBranchAddress("Index", index)
    last_entry = None
    print("here")
    while True:

        current_entry = rf.Get(tree_name).GetEntries()
        if last_entry is None:
            print("last_entry", last_entry)
            print("current_entry", current_entry)
            last_entry = current_entry
        for i in range(last_entry, current_entry):
            rf.Get(tree_name).GetEntry(i)
            print(tree_name, 'i', i, price[0], volume[0], timestamp[0], index[0])

        last_entry = current_entry
        rf.Get(tree_name).Refresh()


def parse_args():
    parser = argparse.ArgumentParser(description="Запускает чтение данных из файла")
    parser.add_argument('--root_file', dest='root_file', action="store", type=str, required=False, default='data.root',
                        help="Путь к root файлу, где хранятся биржевые данные")


def get_symbols(filename):
    root_file = TFile(filename, 'read')
    symbols = []
    for key in root_file.GetListOfKeys():
        if key.GetClassName() == 'TTree':
            symbols.append(key.GetName())
    symbols = set(symbols)
    print(symbols)
    return symbols


def read_trees_in_parallel(filename):

    symbols = get_symbols(filename)
    with multiprocessing.Pool(len(symbols)) as pool:
        for tree_name in symbols:
            pool.apply_async(read_tree, args=(root_filename, tree_name))
        pool.close()
        pool.join()

if __name__ == '__main__':

    start = time.time()

    # root_filename = 'data.root'
    root_filename = 'binance/BTCUSDT.root'

    read_trees_in_parallel(root_filename)

