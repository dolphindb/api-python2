import datetime

import os
#from db.models import *
#from db.models import BbDao
#from config import settings
import dolphindb as ddb

#from utils import common

cs_session = ddb.session()
cs_session.connect(settings.DDP['DB_HOST'], settings.DDP['DB_PORT'])
cs_db_path = settings.DDP['file'] + "coins_summary"
cs_dfs_path = settings.DDP['WORK_DIR'] + "coins_summary"
cs_head = ["coin_symbol", "coin_name", "coin_type", "coin_mc_id",
            "price_usd", "price_btc",
            "usd_percent_change_1h", "usd_percent_change_24h","usd_percent_change_7d", "usd_volume_24h",
            "marketcap", "marketcap_btc",
            "btc_percent_change_1h", "btc_percent_change_24h", "btc_percent_change_7d", "btc_volume_24h",
            "md_time", "timestamp", "date"]

cp_session = ddb.session()
cp_session.connect(settings.DDP['DB_HOST'], settings.DDP['DB_PORT'])
cp_db_path = settings.DDP['file'] + "coins_price"
cp_dfs_path = settings.DDP['WORK_DIR'] + "coins_price"
cp_head = ['coin_symbol', 'price_btc', 'price_usd', 'price_time', 'timestamp', 'date']


def load_csv(session, filenames, tablename, db_path, schemascript=''):
    for filename in filenames:
        common.elk_logger(logger_name=settings.NAME,
                          log_level='info',
                          messages='generate csv file [%s] ...' % filename)
        if schemascript:
            coin_script = """
login("admin","123456")
schemaTb=extractTextSchema('{filename}')
{schemascript}
cs_table=loadTable('{db_path}','{table_name}');
t = loadText('{filename}',,schemaTb)
cs_table.append!(t);
            """.format(schemascript=schemascript, db_path=db_path, table_name=tablename, filename=filename)
        else:
            coin_script = """
login("admin","123456")
cs_table=loadTable('{db_path}','{table_name}');
t = loadText('{filename}')
cs_table.append!(t);
            """.format(db_path=db_path, table_name=tablename, filename=filename)
        print(coin_script)
        # session.run(coin_script)


# coin_summary 表的导入
# dao = BbDao()
# do_cs_filenames = []
# do_cp_filenames = []
# local_cs_filenames = []
# local_cp_filenames = []
# with dao:
#     coins = [coin for coin in dao.get_all_coins()]
#     for coin in coins:
#         cs_rows = []
#         cp_rows = []
#
#         cs_filename = "%s/%s" % (cs_db_path, "cs_%s.csv" % coin.symbol)
#         cs_local =  'D:/DolphinDB/cs/cs_%s.csv' % coin.symbol
#         local_cs_filenames.append(cs_local)
#         do_cs_filenames.append(cs_filename)
#
#         cp_filename = "%s/%s" % (cp_db_path, "cp_%s.csv" % coin.symbol)
#         cp_local =  'D:/DolphinDB/cp/cp_%s.csv' % coin.symbol
#         local_cp_filenames.append(cp_local)
#         do_cp_filenames.append(cp_filename)
#
#         cds = CoinData(coin_id=coin.id).select().order_by(CoinData.timestamp.desc())
#         for i in cds:
#             coin_date = datetime.fromtimestamp(i.timestamp).strftime('%Y.%m.%d')
#             cs_rows.append([coin.symbol, coin.name, int(coin.type), coin.coin_id,
#                          i.price_usd, i.price_btc,
#                          float(coin.usd_percent_change_1h or 0.0), float(coin.usd_percent_change_24h or 0.0), float(coin.usd_percent_change_7d or 0.0), float(coin.usd_volume_24h or 0.0),
#                          float(coin.market_cap_usd or 0.0), float(coin.market_cap_btc or 0.0),
#                          float(coin.btc_percent_change_1h or 0.0), float(coin.btc_percent_change_24h or 0.0), float(coin.btc_percent_change_7d or 0.0), float(coin.btc_volume_24h or 0.0),
#                          int(i.timestamp), int(i.timestamp), coin_date])
#
#             price_time = datetime.fromtimestamp(i.timestamp).strftime('%Y-%m-%d %H:%M:%S')
#             cp_rows.append([coin.symbol, i.price_btc, i.price_usd, price_time, int(i.timestamp), coin_date])
#         common.__gen_csv(cs_local, cs_head, cs_rows)
#         common.__gen_csv(cp_local, cp_head, cp_rows)
# print(do_cs_filenames)
# print(do_cp_filenames)

# do_cs_filenames = ['/root/BBIndex/coins_summary/cs_BTC.csv', '/root/BBIndex/coins_summary/cs_ETH.csv', '/root/BBIndex/coins_summary/cs_BCH.csv', '/root/BBIndex/coins_summary/cs_XRP.csv', '/root/BBIndex/coins_summary/cs_LTC.csv', '/root/BBIndex/coins_summary/cs_DASH.csv', '/root/BBIndex/coins_summary/cs_XEM.csv', '/root/BBIndex/coins_summary/cs_XMR.csv', '/root/BBIndex/coins_summary/cs_MIOTA.csv', '/root/BBIndex/coins_summary/cs_ETC.csv', '/root/BBIndex/coins_summary/cs_NEO.csv', '/root/BBIndex/coins_summary/cs_BCC.csv', '/root/BBIndex/coins_summary/cs_LSK.csv', '/root/BBIndex/coins_summary/cs_STRAT.csv', '/root/BBIndex/coins_summary/cs_ZEC.csv', '/root/BBIndex/coins_summary/cs_WAVES.csv', '/root/BBIndex/coins_summary/cs_HSR.csv', '/root/BBIndex/coins_summary/cs_STEEM.csv', '/root/BBIndex/coins_summary/cs_XLM.csv', '/root/BBIndex/coins_summary/cs_BCN.csv', '/root/BBIndex/coins_summary/cs_ARK.csv', '/root/BBIndex/coins_summary/cs_BTS.csv', '/root/BBIndex/coins_summary/cs_KMD.csv', '/root/BBIndex/coins_summary/cs_NXS.csv', '/root/BBIndex/coins_summary/cs_FCT.csv', '/root/BBIndex/coins_summary/cs_PIVX.csv', '/root/BBIndex/coins_summary/cs_SC.csv', '/root/BBIndex/coins_summary/cs_GBYTE.csv', '/root/BBIndex/coins_summary/cs_DCR.csv', '/root/BBIndex/coins_summary/cs_DOGE.csv', '/root/BBIndex/coins_summary/cs_DGB.csv', '/root/BBIndex/coins_summary/cs_GAME.csv', '/root/BBIndex/coins_summary/cs_BTCD.csv', '/root/BBIndex/coins_summary/cs_SYS.csv', '/root/BBIndex/coins_summary/cs_LKK.csv', '/root/BBIndex/coins_summary/cs_XVG.csv', '/root/BBIndex/coins_summary/cs_BLOCK.csv', '/root/BBIndex/coins_summary/cs_NXT.csv', '/root/BBIndex/coins_summary/cs_GXS.csv', '/root/BBIndex/coins_summary/cs_UBQ.csv', '/root/BBIndex/coins_summary/cs_NAV.csv', '/root/BBIndex/coins_summary/cs_PART.csv', '/root/BBIndex/coins_summary/cs_FRST.csv', '/root/BBIndex/coins_summary/cs_CLOAK.csv', '/root/BBIndex/coins_summary/cs_OK.csv', '/root/BBIndex/coins_summary/cs_XEL.csv', '/root/BBIndex/coins_summary/cs_IOC.csv', '/root/BBIndex/coins_summary/cs_NLG.csv', '/root/BBIndex/coins_summary/cs_ADK.csv', '/root/BBIndex/coins_summary/cs_NLC2.csv', '/root/BBIndex/coins_summary/cs_LEO.csv', '/root/BBIndex/coins_summary/cs_RISE.csv', '/root/BBIndex/coins_summary/cs_FAIR.csv', '/root/BBIndex/coins_summary/cs_PPC.csv', '/root/BBIndex/coins_summary/cs_DCT.csv', '/root/BBIndex/coins_summary/cs_RDD.csv', '/root/BBIndex/coins_summary/cs_XCP.csv', '/root/BBIndex/coins_summary/cs_EMC.csv', '/root/BBIndex/coins_summary/cs_VTC.csv', '/root/BBIndex/coins_summary/cs_EXP.csv', '/root/BBIndex/coins_summary/cs_TCC.csv', '/root/BBIndex/coins_summary/cs_MONA.csv', '/root/BBIndex/coins_summary/cs_XZC.csv', '/root/BBIndex/coins_summary/cs_ETP.csv', '/root/BBIndex/coins_summary/cs_ION.csv', '/root/BBIndex/coins_summary/cs_XAS.csv', '/root/BBIndex/coins_summary/cs_VIA.csv', '/root/BBIndex/coins_summary/cs_NMC.csv', '/root/BBIndex/coins_summary/cs_CRW.csv', '/root/BBIndex/coins_summary/cs_BAY.csv', '/root/BBIndex/coins_summary/cs_BURST.csv', '/root/BBIndex/coins_summary/cs_MUE.csv', '/root/BBIndex/coins_summary/cs_POT.csv', '/root/BBIndex/coins_summary/cs_RADS.csv', '/root/BBIndex/coins_summary/cs_CLAM.csv', '/root/BBIndex/coins_summary/cs_UNO.csv', '/root/BBIndex/coins_summary/cs_MOON.csv', '/root/BBIndex/coins_summary/cs_SIB.csv', '/root/BBIndex/coins_summary/cs_LBC.csv', '/root/BBIndex/coins_summary/cs_XDN.csv', '/root/BBIndex/coins_summary/cs_OMNI.csv', '/root/BBIndex/coins_summary/cs_PPY.csv', '/root/BBIndex/coins_summary/cs_SKY.csv', '/root/BBIndex/coins_summary/cs_NEBL.csv', '/root/BBIndex/coins_summary/cs_SHIFT.csv', '/root/BBIndex/coins_summary/cs_ZEN.csv', '/root/BBIndex/coins_summary/cs_RBY.csv', '/root/BBIndex/coins_summary/cs_SLS.csv', '/root/BBIndex/coins_summary/cs_SPR.csv', '/root/BBIndex/coins_summary/cs_DMD.csv', '/root/BBIndex/coins_summary/cs_ENRG.csv', '/root/BBIndex/coins_summary/cs_GOLOS.csv', '/root/BBIndex/coins_summary/cs_BLK.csv', '/root/BBIndex/coins_summary/cs_XVC.csv', '/root/BBIndex/coins_summary/cs_NEOS.csv', '/root/BBIndex/coins_summary/cs_GRC.csv', '/root/BBIndex/coins_summary/cs_ECC.csv', '/root/BBIndex/coins_summary/cs_LMC.csv', '/root/BBIndex/coins_summary/cs_AEON.csv', '/root/BBIndex/coins_summary/cs_OMG.csv', '/root/BBIndex/coins_summary/cs_QTUM.csv', '/root/BBIndex/coins_summary/cs_USDT.csv', '/root/BBIndex/coins_summary/cs_EOS.csv', '/root/BBIndex/coins_summary/cs_PAY.csv', '/root/BBIndex/coins_summary/cs_MAID.csv', '/root/BBIndex/coins_summary/cs_REP.csv', '/root/BBIndex/coins_summary/cs_BAT.csv', '/root/BBIndex/coins_summary/cs_MTL.csv', '/root/BBIndex/coins_summary/cs_GNT.csv', '/root/BBIndex/coins_summary/cs_VERI.csv', '/root/BBIndex/coins_summary/cs_ICN.csv', '/root/BBIndex/coins_summary/cs_DGD.csv', '/root/BBIndex/coins_summary/cs_PPT.csv', '/root/BBIndex/coins_summary/cs_CVC.csv', '/root/BBIndex/coins_summary/cs_GAS.csv', '/root/BBIndex/coins_summary/cs_ARDR.csv', '/root/BBIndex/coins_summary/cs_ZRX.csv', '/root/BBIndex/coins_summary/cs_SNGLS.csv', '/root/BBIndex/coins_summary/cs_GNO.csv', '/root/BBIndex/coins_summary/cs_BNB.csv', '/root/BBIndex/coins_summary/cs_BNT.csv', '/root/BBIndex/coins_summary/cs_AE.csv', '/root/BBIndex/coins_summary/cs_SNT.csv', '/root/BBIndex/coins_summary/cs_EDG.csv', '/root/BBIndex/coins_summary/cs_FUN.csv', '/root/BBIndex/coins_summary/cs_MCO.csv', '/root/BBIndex/coins_summary/cs_ANT.csv', '/root/BBIndex/coins_summary/cs_TNT.csv', '/root/BBIndex/coins_summary/cs_ETHOS.csv', '/root/BBIndex/coins_summary/cs_WINGS.csv', '/root/BBIndex/coins_summary/cs_TRIG.csv', '/root/BBIndex/coins_summary/cs_MGO.csv', '/root/BBIndex/coins_summary/cs_WTC.csv', '/root/BBIndex/coins_summary/cs_BTM.csv', '/root/BBIndex/coins_summary/cs_MTH.csv', '/root/BBIndex/coins_summary/cs_STORJ.csv', '/root/BBIndex/coins_summary/cs_CFI.csv', '/root/BBIndex/coins_summary/cs_PLR.csv', '/root/BBIndex/coins_summary/cs_MLN.csv', '/root/BBIndex/coins_summary/cs_BDL.csv', '/root/BBIndex/coins_summary/cs_KNC.csv', '/root/BBIndex/coins_summary/cs_LINK.csv', '/root/BBIndex/coins_summary/cs_MCAP.csv', '/root/BBIndex/coins_summary/cs_CTR.csv', '/root/BBIndex/coins_summary/cs_ADA.csv', '/root/BBIndex/coins_summary/cs_SMART.csv', '/root/BBIndex/coins_summary/cs_PURA.csv', '/root/BBIndex/coins_summary/cs_GRS.csv', '/root/BBIndex/coins_summary/cs_VET.csv', '/root/BBIndex/coins_summary/cs_TRX.csv', '/root/BBIndex/coins_summary/cs_SALT.csv', '/root/BBIndex/coins_summary/cs_ATM.csv', '/root/BBIndex/coins_summary/cs_AION.csv', '/root/BBIndex/coins_summary/cs_BTG.csv', '/root/BBIndex/coins_summary/cs_EMC2.csv', '/root/BBIndex/coins_summary/cs_QASH.csv', '/root/BBIndex/coins_summary/cs_NANO.csv', '/root/BBIndex/coins_summary/cs_NAS.csv', '/root/BBIndex/coins_summary/cs_NKN.csv', '/root/BBIndex/coins_summary/cs_QKC.csv', '/root/BBIndex/coins_summary/cs_ONT.csv', '/root/BBIndex/coins_summary/cs_GSE.csv', '/root/BBIndex/coins_summary/cs_ICX.csv']
#
# schemascript = """
# update schemaTb set type=`DOUBLE where name=`price_btc
# """
# load_csv(cs_session, do_cs_filenames, 'coins_summary', cs_dfs_path, schemascript)

do_cp_filenames = ['/root/BBIndex/coins_price/cp_BTC.csv', '/root/BBIndex/coins_price/cp_ETH.csv', '/root/BBIndex/coins_price/cp_BCH.csv', '/root/BBIndex/coins_price/cp_XRP.csv', '/root/BBIndex/coins_price/cp_LTC.csv', '/root/BBIndex/coins_price/cp_DASH.csv', '/root/BBIndex/coins_price/cp_XEM.csv', '/root/BBIndex/coins_price/cp_XMR.csv', '/root/BBIndex/coins_price/cp_MIOTA.csv', '/root/BBIndex/coins_price/cp_ETC.csv', '/root/BBIndex/coins_price/cp_NEO.csv', '/root/BBIndex/coins_price/cp_BCC.csv', '/root/BBIndex/coins_price/cp_LSK.csv', '/root/BBIndex/coins_price/cp_STRAT.csv', '/root/BBIndex/coins_price/cp_ZEC.csv', '/root/BBIndex/coins_price/cp_WAVES.csv', '/root/BBIndex/coins_price/cp_HSR.csv', '/root/BBIndex/coins_price/cp_STEEM.csv', '/root/BBIndex/coins_price/cp_XLM.csv', '/root/BBIndex/coins_price/cp_BCN.csv', '/root/BBIndex/coins_price/cp_ARK.csv', '/root/BBIndex/coins_price/cp_BTS.csv', '/root/BBIndex/coins_price/cp_KMD.csv', '/root/BBIndex/coins_price/cp_NXS.csv', '/root/BBIndex/coins_price/cp_FCT.csv', '/root/BBIndex/coins_price/cp_PIVX.csv', '/root/BBIndex/coins_price/cp_SC.csv', '/root/BBIndex/coins_price/cp_GBYTE.csv', '/root/BBIndex/coins_price/cp_DCR.csv', '/root/BBIndex/coins_price/cp_DOGE.csv', '/root/BBIndex/coins_price/cp_DGB.csv', '/root/BBIndex/coins_price/cp_GAME.csv', '/root/BBIndex/coins_price/cp_BTCD.csv', '/root/BBIndex/coins_price/cp_SYS.csv', '/root/BBIndex/coins_price/cp_LKK.csv', '/root/BBIndex/coins_price/cp_XVG.csv', '/root/BBIndex/coins_price/cp_BLOCK.csv', '/root/BBIndex/coins_price/cp_NXT.csv', '/root/BBIndex/coins_price/cp_GXS.csv', '/root/BBIndex/coins_price/cp_UBQ.csv', '/root/BBIndex/coins_price/cp_NAV.csv', '/root/BBIndex/coins_price/cp_PART.csv', '/root/BBIndex/coins_price/cp_FRST.csv', '/root/BBIndex/coins_price/cp_CLOAK.csv', '/root/BBIndex/coins_price/cp_OK.csv', '/root/BBIndex/coins_price/cp_XEL.csv', '/root/BBIndex/coins_price/cp_IOC.csv', '/root/BBIndex/coins_price/cp_NLG.csv', '/root/BBIndex/coins_price/cp_ADK.csv', '/root/BBIndex/coins_price/cp_NLC2.csv', '/root/BBIndex/coins_price/cp_LEO.csv', '/root/BBIndex/coins_price/cp_RISE.csv', '/root/BBIndex/coins_price/cp_FAIR.csv', '/root/BBIndex/coins_price/cp_PPC.csv', '/root/BBIndex/coins_price/cp_DCT.csv', '/root/BBIndex/coins_price/cp_RDD.csv', '/root/BBIndex/coins_price/cp_XCP.csv', '/root/BBIndex/coins_price/cp_EMC.csv', '/root/BBIndex/coins_price/cp_VTC.csv', '/root/BBIndex/coins_price/cp_EXP.csv', '/root/BBIndex/coins_price/cp_TCC.csv', '/root/BBIndex/coins_price/cp_MONA.csv', '/root/BBIndex/coins_price/cp_XZC.csv', '/root/BBIndex/coins_price/cp_ETP.csv', '/root/BBIndex/coins_price/cp_ION.csv', '/root/BBIndex/coins_price/cp_XAS.csv', '/root/BBIndex/coins_price/cp_VIA.csv', '/root/BBIndex/coins_price/cp_NMC.csv', '/root/BBIndex/coins_price/cp_CRW.csv', '/root/BBIndex/coins_price/cp_BAY.csv', '/root/BBIndex/coins_price/cp_BURST.csv', '/root/BBIndex/coins_price/cp_MUE.csv', '/root/BBIndex/coins_price/cp_POT.csv', '/root/BBIndex/coins_price/cp_RADS.csv', '/root/BBIndex/coins_price/cp_CLAM.csv', '/root/BBIndex/coins_price/cp_UNO.csv', '/root/BBIndex/coins_price/cp_MOON.csv', '/root/BBIndex/coins_price/cp_SIB.csv', '/root/BBIndex/coins_price/cp_LBC.csv', '/root/BBIndex/coins_price/cp_XDN.csv', '/root/BBIndex/coins_price/cp_OMNI.csv', '/root/BBIndex/coins_price/cp_PPY.csv', '/root/BBIndex/coins_price/cp_SKY.csv', '/root/BBIndex/coins_price/cp_NEBL.csv', '/root/BBIndex/coins_price/cp_SHIFT.csv', '/root/BBIndex/coins_price/cp_ZEN.csv', '/root/BBIndex/coins_price/cp_RBY.csv', '/root/BBIndex/coins_price/cp_SLS.csv', '/root/BBIndex/coins_price/cp_SPR.csv', '/root/BBIndex/coins_price/cp_DMD.csv', '/root/BBIndex/coins_price/cp_ENRG.csv', '/root/BBIndex/coins_price/cp_GOLOS.csv', '/root/BBIndex/coins_price/cp_BLK.csv', '/root/BBIndex/coins_price/cp_XVC.csv', '/root/BBIndex/coins_price/cp_NEOS.csv', '/root/BBIndex/coins_price/cp_GRC.csv', '/root/BBIndex/coins_price/cp_ECC.csv', '/root/BBIndex/coins_price/cp_LMC.csv', '/root/BBIndex/coins_price/cp_AEON.csv', '/root/BBIndex/coins_price/cp_OMG.csv', '/root/BBIndex/coins_price/cp_QTUM.csv', '/root/BBIndex/coins_price/cp_USDT.csv', '/root/BBIndex/coins_price/cp_EOS.csv', '/root/BBIndex/coins_price/cp_PAY.csv', '/root/BBIndex/coins_price/cp_MAID.csv', '/root/BBIndex/coins_price/cp_REP.csv', '/root/BBIndex/coins_price/cp_BAT.csv', '/root/BBIndex/coins_price/cp_MTL.csv', '/root/BBIndex/coins_price/cp_GNT.csv', '/root/BBIndex/coins_price/cp_VERI.csv', '/root/BBIndex/coins_price/cp_ICN.csv', '/root/BBIndex/coins_price/cp_DGD.csv', '/root/BBIndex/coins_price/cp_PPT.csv', '/root/BBIndex/coins_price/cp_CVC.csv', '/root/BBIndex/coins_price/cp_GAS.csv', '/root/BBIndex/coins_price/cp_ARDR.csv', '/root/BBIndex/coins_price/cp_ZRX.csv', '/root/BBIndex/coins_price/cp_SNGLS.csv', '/root/BBIndex/coins_price/cp_GNO.csv', '/root/BBIndex/coins_price/cp_BNB.csv', '/root/BBIndex/coins_price/cp_BNT.csv', '/root/BBIndex/coins_price/cp_AE.csv', '/root/BBIndex/coins_price/cp_SNT.csv', '/root/BBIndex/coins_price/cp_EDG.csv', '/root/BBIndex/coins_price/cp_FUN.csv', '/root/BBIndex/coins_price/cp_MCO.csv', '/root/BBIndex/coins_price/cp_ANT.csv', '/root/BBIndex/coins_price/cp_TNT.csv', '/root/BBIndex/coins_price/cp_ETHOS.csv', '/root/BBIndex/coins_price/cp_WINGS.csv', '/root/BBIndex/coins_price/cp_TRIG.csv', '/root/BBIndex/coins_price/cp_MGO.csv', '/root/BBIndex/coins_price/cp_WTC.csv', '/root/BBIndex/coins_price/cp_BTM.csv', '/root/BBIndex/coins_price/cp_MTH.csv', '/root/BBIndex/coins_price/cp_STORJ.csv', '/root/BBIndex/coins_price/cp_CFI.csv', '/root/BBIndex/coins_price/cp_PLR.csv', '/root/BBIndex/coins_price/cp_MLN.csv', '/root/BBIndex/coins_price/cp_BDL.csv', '/root/BBIndex/coins_price/cp_KNC.csv', '/root/BBIndex/coins_price/cp_LINK.csv', '/root/BBIndex/coins_price/cp_MCAP.csv', '/root/BBIndex/coins_price/cp_CTR.csv', '/root/BBIndex/coins_price/cp_ADA.csv', '/root/BBIndex/coins_price/cp_SMART.csv', '/root/BBIndex/coins_price/cp_PURA.csv', '/root/BBIndex/coins_price/cp_GRS.csv', '/root/BBIndex/coins_price/cp_VET.csv', '/root/BBIndex/coins_price/cp_TRX.csv', '/root/BBIndex/coins_price/cp_SALT.csv', '/root/BBIndex/coins_price/cp_ATM.csv', '/root/BBIndex/coins_price/cp_AION.csv', '/root/BBIndex/coins_price/cp_BTG.csv', '/root/BBIndex/coins_price/cp_EMC2.csv', '/root/BBIndex/coins_price/cp_QASH.csv', '/root/BBIndex/coins_price/cp_NANO.csv', '/root/BBIndex/coins_price/cp_NAS.csv', '/root/BBIndex/coins_price/cp_NKN.csv', '/root/BBIndex/coins_price/cp_QKC.csv', '/root/BBIndex/coins_price/cp_ONT.csv', '/root/BBIndex/coins_price/cp_GSE.csv', '/root/BBIndex/coins_price/cp_ICX.csv']
schemascript = """
update schemaTb set type=`DOUBLE where name=`price_btc; update schemaTb set type=`SYMBOL where name=`price_time
"""
load_csv(cp_session, do_cp_filenames, 'coins_price', cp_dfs_path, schemascript)

'''
// 创建三个业务目的的数据库
cs_db = database("/root/DolphinDB/BBIndex/DB/coins_summary", VALUE, 1970.01M..2100.12M)
cr_db = database("/root/DolphinDB/BBIndex/DB/coins_rebalance", VALUE, 1970.01M..2100.12M)
realtim_db = database("/root/DolphinDB/BBIndex/DB/coins_price", VALUE, 1970.01M..2100.12M)
idx_db = database("/root/DolphinDB/BBIndex/DB/index", VALUE, 1970.01M..2100.12M)
// 创建表
cp = table("default" as coin_symbol,0.00 as price_btc, 0.00 as price_usd, "default" as price_time,0 as timestamp, 1970.01.01 as date)
idx = table("default" as index_name,0.00 as value_btc, 0.00 as value_usd, 0 as timestamp, "default" as rebalance_at, 1970.01.01 as date)
cr = table("default" as coin_symbol,"default" as coin_name,0 as coin_type,0 as coin_mc_id, 0.00 as price_usd, 0.00 as price_btc,0 as timestamp, 1970.01.01 as date)
cs = table("default" as coin_symbol,"default" as coin_name,0 as coin_type, 0 as coin_mc_id,
      0.00 as price_usd, 0.00 as price_btc,
      0.00 as usd_percent_change_1h, 0.00 as usd_percent_change_24h, 0.00 as usd_percent_change_7d, 0.00 as usd_volume_24h,
      0.00 as marketcap, 0.00 as marketcap_btc,
      0.00 as btc_percent_change_1h, 0.00 as btc_percent_change_24h, 0.00 as btc_percent_change_7d, 0.00 as btc_volume_24h,
      0 as md_time,0 as timestamp, 1970.01.01 as date)

cs_db.createPartitionedTable(cs,'coins_summary','date');
cr_db.createPartitionedTable(cr,'coins_rebalance','date');
realtim_db.createPartitionedTable(cp,'coins_price','date');
idx_db.createPartitionedTable(idx,'index','date');

'''