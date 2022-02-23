from ROOT import TObject, TFile, TTree, AddressOf
import array
import random
import time
if __name__ == '__main__':

    root_file = TFile('data.root', 'recreate')
    fval = 3.14
    ival = 119
    tree = TTree("tree", "The Tree Title")
    price = array.array('d', [fval])
    volume = array.array('d', [fval])
    timestamp = array.array('i', [ival])
    index = array.array('i', [ival])
    tree.Branch("Price", price, "price/d")
    tree.Branch("Volume", volume, "volume/d")
    tree.Branch("Timestamp", timestamp, "timestamp/i")
    tree.Branch("Index", index, "index/s")
    tree.Write()

    for i in range(1000000000000000000000):
        price[0] =  random.random()
        volume[0] = 154*random.random()
        timestamp[0] = i
        index[0] = i
        tree.Fill()

        if tree.GetEntries() % 1000 == 0:
            print("Entries", tree.GetEntries(),
                  timestamp,
                  price,
                  volume,
                  index
                  )
            # print("price", price, "volume", volume, "timestamp", timestamp, "index", index)
            tree.AutoSave('SaveSelf')
            time.sleep(1)
