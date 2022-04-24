import argparse

import requests
import json
import os
from igraph import *
import pickle

def get_obj(path_to_file):
    if os.path.exists(path_to_file):
        with open(path_to_file, 'rb') as f:
            return pickle.load(f)
    else:
        raise ValueError("No file found!")


def save_obj(obj, path_to_file):
    with open(path_to_file, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def make_get_request(address):
    return json.loads(requests.get(address).text)

def form_paths(interested):
    qAssets = list(map(lambda x: x.get('quoteAsset'), interested))
    bAssets = list(map(lambda x: x.get('baseAsset'), interested))
    symbol = list(map(lambda x: x.get('symbol'), interested))
    uniqueSym = list(set(qAssets + bAssets))
    uniqueSymDictForGraph = dict(zip(uniqueSym, list(range(len(uniqueSym)))))
    g = Graph()
    g.add_vertices(len(uniqueSymDictForGraph))
    g.vs["name"] = list(uniqueSymDictForGraph.keys())
    for i in interested:
        g.add_edge(uniqueSymDictForGraph[i['baseAsset']], uniqueSymDictForGraph[i['quoteAsset']])

    pathtousd = {}

    for i in range(0, len(qAssets)):
        pathtousd[symbol[i]] = {}
        pathtousd[symbol[i]]['b'] = bAssets[i]
        pathtousd[symbol[i]]['q'] = qAssets[i]
        pathtousd[symbol[i]]['path'] = []
        pathtousd[symbol[i]]['reversed'] = []
        pathtousd[symbol[i]]['targetAsset'] = pathtousd[symbol[i]]['b']

        pathtousd[symbol[i]]['path'] += [symbol[i]]
        if symbol[i].__contains__('USDT'):
            if symbol[i].startswith('USDT'):
                pathtousd[symbol[i]]['reversed'] = [True]
                pathtousd[symbol[i]]['targetAsset'] = pathtousd[symbol[i]]['q']
            else:
                pathtousd[symbol[i]]['reversed'] = [False]
                pathtousd[symbol[i]]['targetAsset'] = pathtousd[symbol[i]]['b']
            continue

        path = g.get_shortest_paths(qAssets[i], to="USDT", mode=OUT, output='vpath')
        if g.vs[path[0][1]]['name'] == bAssets[i]:
            pathtousd[symbol[i]]['path'] = []
            pathtousd[symbol[i]]['targetAsset'] = pathtousd[symbol[i]]['q']
        else:
            pathtousd[symbol[i]]['reversed'] = [False]

        for n in range(0, len(path[0]) - 1):
            pair = str(g.vs[path[0][n]]['name'] + g.vs[path[0][n + 1]]['name'])
            if pair in symbol:
                pathtousd[symbol[i]]['path'] += [pair]
                pathtousd[symbol[i]]['reversed'] += [False]
                continue
            pair = str(g.vs[path[0][n + 1]]['name'] + g.vs[path[0][n]]['name'])
            if pair in symbol:
                pathtousd[symbol[i]]['path'] += [pair]
                pathtousd[symbol[i]]['reversed'] += [True]
                continue
            raise ValueError("No such pair " + pair + " found!")

    return pathtousd

def pairs_prepare(stockaddress, filepath, force):
    if not (not force and os.path.exists(filepath)):
        objects = make_get_request(stockaddress)
        interested = [item for item in objects['symbols'] if item['status'] == 'TRADING']
        obj = form_paths(interested)
        save_obj(obj, filepath)

    return get_obj(filepath)

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--forceUpdate',
                        required=False,
                        dest='forceUpdate',
                        default=True,
                        type=bool,
                        help='Force update pairs')
    parser.add_argument('--link',
                        required=False,
                        dest='link',
                        default="https://api3.binance.com/api/v3/exchangeInfo",
                        type=str,
                        help='Link on pairs on stock')
    return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    a = pairs_prepare(args.link, "pairs.pkl", args.forceUpdate)
    print(json.dumps(a, indent=2, default=str))





