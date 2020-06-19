import pandas as pd

def __run_script(session, db_path, table_name, db_data):
    df_data = pd.DataFrame(db_data)
    session.upload({'t': df_data})
    script = """
login("{user}","{pwd}")
loadTable('{db_path}','{tablename}').append!(t)
    """.format(db_path=db_path, tablename=table_name, user=settings.DDP['USER'], pwd=settings.DDP['PWD'])
    session.run(script)


def latest_coins(session, raw_data):
    db_path = settings.DDP['WORK_DIR'] + "coins_price"
    table_name = 'coins_price'
    timestamp = int(raw_data.pop('timestamp'))
    today_do = ddb.Date.from_date(datetime.date.today())  # 生成table必须使用此日期类型
    db_data = []
    for coin in raw_data:
        d = collections.OrderedDict()
        d['coin_symbol'] = coin
        d['price_btc'] = float(raw_data[coin]['price_btc'])
        d['price_usd'] = float(raw_data[coin]['price_usd'])
        d['price_time'] = raw_data[coin]['price_time']
        d['timestamp'] = timestamp
        d['date'] = today_do
        db_data.append(d)

    __run_script(session, db_path, table_name, db_data)