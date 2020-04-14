""" Visualize the topics. """


from matplotlib import pyplot as plt
from db import DB


def gather_data(persist):
    cursor = persist.get_cursor()
    cursor.execute("SELECT * FROM topics")
    data = dict()
    for row in cursor.fetchall():
        if row[1] not in data.keys():
            data[row[1]] = list()
        data[row[1]].append(row[2])
    return data


def plot_data(data):
    for topic in data:
        plt.plot(data[topic], label=topic)
    plt.legend()
    plt.show()


if __name__ == '__main__':
    persist = DB()
    persist.connect_db(1586845685)
    data = gather_data(persist)
    plot_data(data)
