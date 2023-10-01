import sys
import os

basedir = os.path.abspath(os.path.dirname(__name__))
sys.path.append(basedir)

from market import ManageCoins
from bot import ManageBots
import time
import schedule
from db import Database
import plotly.express as px
import plotly.graph_objects as go
import pandas
import config
import logging.config

import json



def rsi_heatmap(df):
    func = lambda x: x["topic"].split("_")[0][:-4]
    df["topic"] = df.apply(func, axis=1)
    plot = go.Figure(data=go.Scatter(x=df['topic'],
                                     y=df['rsi'],
                                     mode='markers+text',
                                     marker_color=df['rsi'],
                                     marker=dict(
                                         colorscale=px.colors.sequential.Bluered,
                                         size=12
                                     ),
                                     texttemplate='%{text}',
                                     text=df['topic'],
                                     textposition='bottom center'))  # hover text goes here

    plot.update_traces(
        name='',
        hoverinfo='text',
        customdata=[
            [
                '<BR><b>Symbol: </b> ' + str(df.loc[i, 'topic']),
                '<BR><b>RSI: </b> ' + str(df.loc[i, 'rsi']),
                '<BR><b>NATR: </b> ' + str(df.loc[i, 'natr']),
                '<BR><b>Volume: </b> ' + str(df.loc[i, 'volume']),
                '<BR><b>Last Update: </b> ' + str(df.loc[i, 'timestamp'])

            ]
            for i in df.index
        ],
        hovertemplate='%{customdata}',
        selector=dict(type='scatter')
    )

    plot.layout.xaxis.fixedrange = True
    plot.layout.yaxis.fixedrange = True
    plot.update_yaxes(range=[0, 100], dtick=10, showgrid=False)
    plot.update_xaxes(showgrid=False)
    plot.update_layout(width=1800, height=800, autosize=False)

    plot.add_shape(type="rect", xref='paper', x0=0, y0=69.99, x1=1, y1=99.9,
                   line=dict(color='#EF5A5A', dash='dot', width=2),
                   label=dict(text='OVERBOUGHT', textposition='middle right', font=dict(size=20, color='#AFAABD')),
                   fillcolor='#F5CCCC')

    plot.add_shape(type="rect", xref='paper', x0=0, y0=59.99, x1=1, y1=69.99,
                   line=dict(color="#F8BDBD", dash='dot', width=2),
                   label=dict(text='STRONG', textposition='middle right', font=dict(size=20, color='#AFAABD')),
                   fillcolor='#FFEBEB')

    plot.add_shape(type="rect", xref='paper', x0=0, y0=40, x1=1, y1=60, line=dict(color="#E5ECF6", dash='dot', width=2),
                   label=dict(text='NEUTRAL', textposition='middle right', font=dict(size=20, color='#AFAABD')),
                   fillcolor='#E5ECF6')

    plot.add_shape(type="rect", xref='paper', x0=0, y0=40, x1=1, y1=30, line=dict(color="#38D486", dash='dot', width=2),
                   label=dict(text='WEAK', textposition='middle right', font=dict(size=20, color='#AFAABD')),
                   fillcolor='#CCF5E0')

    plot.add_shape(type="rect", xref='paper', x0=0, y0=30, x1=1, y1=0.2,
                   line=dict(color="#388638", dash='dot', width=2),
                   label=dict(text='OVERSOLD', textposition='middle right', font=dict(size=20, color='#AFAABD')),
                   fillcolor='#CCE0CC')

    plot.update_shapes(opacity=1, layer='below')
    plot.show(config={'displayModeBar': False})


def get_pair_list():
    with open(config.COIN_DATA_PATH) as f:
        pair_list = [row.split(',')[1].rstrip() for row in f]
        return pair_list


def init_coins(pair_list, db):
    coins = ManageCoins(intervals=config.INTERVALS, testnet=False, db=db)
    #coins.update_coin_info()
    
    coins.add_coin_connection(pair_list)
    for coin in coins.object_dict.values():
        time.sleep(0.2)
        coin.start()
        for interval in config.INTERVALS:
            coin.calculate_indicators(interval=interval)
    
    return coins


def init_bot(manage_coin, db):
    bots = ManageBots(coins=manage_coin, db=db)
    bots.start_bots()


def setup_logging(
        default_path='logging.json',
        default_level=logging.INFO,
        env_key='LOG_CFG'
):
    """ Setup logging configuration. """

    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            conf = json.load(f)
        logging.config.dictConfig(conf)
    else:
        logging.basicConfig(level=default_level)


def main():
    setup_logging()
    db = Database(config=config)
    pair_list = ['ETHUSDT']
    manage_coin = init_coins(pair_list=pair_list, db=db)
    #schedule.every(1).minute.at(':30').do(manage_coin.update_coin_info)

    # db = Database(config)
    # val = db.get_coindata_values('15')
    # rsi_heatmap(df=val)
    init_bot(manage_coin=manage_coin, db=db)
    while True:
        #schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
