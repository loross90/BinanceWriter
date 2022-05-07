import matplotlib.pyplot as plt
from numpy import mean
from numpy import median

if __name__ == '__main__':
    #data = dict.fromkeys(['trade','depthUpdate'], [{},{}])
    data = {}
    with open("delayWatcher.txt", 'r') as f:
        for line in f:
            x = line.rstrip().split('|')
            if not x[1] in data:
                data[x[1]] = []
            data[x[1]].append(list(map(int, [x[0], x[2], x[3], x[4], 0])))

    with open("delayReader.txt", 'r') as f:
        trades = list(x[0] for x in data['trade'])
        depthUpdates = list(x[0] for x in data['depthUpdate'])
        for line in f:
            x = list(map(int, line.rstrip().split('|')))
            if x[0] in trades:
                data['trade'][trades.index(x[0])][4] = x[1]
            if x[0] in depthUpdates:
                data['depthUpdate'][depthUpdates.index(x[0])][4] = x[1]

    for recordType in data.keys():
        plt.cla()
        histdata = list((x[2] - x[1]) for x in data[recordType])
        plt.hist(histdata)
        plt.title("Разница между временем получения записи " + str(recordType) + " и временем биржи \n" +
                  "среднее: " + str(mean(histdata)) + " медиана: " + str(median(histdata)) + "\n"
                  "минимум: " + str(min(histdata)) + " максимум: " + str(max(histdata)))
        plt.ylabel("Число записей")
        plt.xlabel("мс")
        plt.savefig(str(recordType) + "1.png")
        plt.cla()

        histdata = list((x[3] - x[1]) for x in data[recordType])
        plt.hist(histdata)
        plt.title("Разница между временем записи " + str(recordType) + " и временем биржи \n" +
                  "среднее: " + str(mean(histdata)) + " медиана: " + str(median(histdata)) + "\n"
                  "минимум: " + str(min(histdata)) + " максимум: " + str(max(histdata)))
        plt.ylabel("Число записей")
        plt.xlabel("мс")
        plt.savefig(str(recordType) + "2.png")
        plt.cla()

        histdata = list((x[4] - x[1]) for x in data[recordType] if x[4] > 0)
        plt.hist(histdata)
        plt.title("Разница между временем чтения записи " + str(recordType) + " и временем биржи \n" +
                  "среднее: " + str(mean(histdata)) + " медиана: " + str(median(histdata)) + "\n"
                  "минимум: " + str(min(histdata)) + " максимум: " + str(max(histdata)))
        plt.ylabel("Число записей")
        plt.xlabel("мс")
        plt.savefig(str(recordType) + "3.png")
        plt.cla()
    0
    # plt.bar(data['trade'].keys(), data['trade'].values())
    # plt.bar(data['depthUpdate'].keys(), data['depthUpdate'].values())
    # if int(int(x[2])-int(x[1])) not in :
    #     data[x[0]][int(x[2])-int(x[1])] = 1
    # else:
    #     data[x[0]][int(x[2]) - int(x[1])] += 1