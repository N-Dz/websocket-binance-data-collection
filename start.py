import time
import json
import websocket
import pandas as pd

symbol = 'btcbusd'
socket_futures = 'wss://fstream.binance.com/ws/{}@bookTicker'.format(symbol) # visit Binance API docs to see all the streams
socket_spot = "wss://stream.binance.com:9443/ws/{}@bookTicker".format(symbol)

# Creating lists to store the desired data
bid_list = []
b_volume = []
ask_list = []
a_volume = []
t_time = []


# called when receiving data
def on_message(ws, message):
    transform_to_p = json.loads(message)
    bid_list.append(float(transform_to_p['b']))
    b_volume.append((float(transform_to_p['B'])))
    ask_list.append(float(transform_to_p['a']))
    a_volume.append((float(transform_to_p['A'])))
    t_time.append(float(transform_to_p['T']))
    print(message)


# called when establishing connection
def on_open(ws):
    print("Opened connection")


# called when closing connection
def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


ws = websocket.WebSocketApp(socket_futures,
                            on_message=on_message,
                            on_open=on_open,
                            on_close=on_close)

ws.run_forever()

# storing the data as dataframe
dataframe_data = pd.DataFrame({'Bids': bid_list, 'B_vol': b_volume, 'Asks': ask_list,
                               'A_vol': a_volume,
                               'Time': t_time})

# save dataframe
dataframe_data.to_csv("order_book_data.csv")
print(dataframe_data.info())
