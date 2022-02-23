from ROOT import TObject, TFile, TTree, AddressOf
import array
import random
import time

if __name__ == '__main__':

    start = time.time()
    root_file = TFile('data.root', 'read')
    price = array.array('d', [0])
    volume = array.array('d', [0])
    timestamp = array.array('i', [0])
    index = array.array('i', [0])
    root_file.tree.SetBranchAddress("Price", price)
    root_file.tree.SetBranchAddress("Volume", volume)
    root_file.tree.SetBranchAddress("Timestamp", timestamp)
    root_file.tree.SetBranchAddress("Index", index)
    last_entry = None

    while True:
        current_entry = root_file.tree.GetEntries()

        if last_entry is None:
            last_entry = current_entry
        for i in range(last_entry, current_entry):
            root_file.tree.GetEntry(i)
            print('i', i, "price", price, "volume", volume, "timestamp", timestamp, "index", index)
        last_entry = current_entry
        root_file.tree.Refresh()
