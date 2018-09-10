import pandas as pd
from sqlalchemy import create_engine
import os
import datetime
chunksize =50000
engine = create_engine('postgresql://postgres:postgres123@/act', client_encoding='LATIN1')

#DATA_DIR= '/home/anshul/act/HYD/Collections/'
DATA_DIR= '/datadir/hyd_act/'

table_name = 'hyd_collections'


data_l1= [#('2016/1781461_ACT_DAYWISE_COLLECTION JAN 16.CSV','jan','2016'),
           # ('2016/2070141_ACT_DAYWISE_COLLECTION FEB .CSV','feb','2016'),
            #('2016/2383166_ACT_DAYWISE_COLLECTION mar.CSV','mar','2016'),
            #('2016/2697631_ACT_DAYWISE_COLLECTION APR.CSV','apr','2016'),
            #('2016/3060550_ACT_DAYWISE_COLLECTION MAY.CSV','may','2016'),
            #('2016/3391032_ACT_DAYWISE_COLLECTION JUN 16.CSV','jun','2016'),
            #('2016/3740264_ACT_DAYWISE_COLLECTION  JUL.CSV','jul','2016'),
            #('2016/4102022_ACT_DAYWISE_COLLECTION AUG.CSV','aug','2016'),
            #('2016/4434723_ACT_DAYWISE_COLLECTION SEP.CSV','sep','2016'),
            #('2016/4773852_ACT_DAYWISE_COLLECTION OCT.CSV','oct','2016'),
            #('2016/5093995_ACT_DAYWISE_COLLECTION nov.CSV','nov','2016'),
            #('2016/5433164_ACT_DAYWISE_COLLECTION DEC.CSV','dec','2016'),
            #('2017/5783908_ACT_DAYWISE_COLLECTION JAN.CSV', 'jan','2017'),
            ]

data_l2= [#('2017/6111833_ACT_DAYWISE_COLLECTION FEB 17.CSV', 'feb','2017'),
          #('2017/6488040_ACT_DAYWISE_COLLECTION MAR 17.CSV', 'mar','2017'),
          #('2017/6857476_ACT_DAYWISE_COLLECTION APR.CSV', 'apr','2017'),
          #('2017/7245136_ACT_DAYWISE_COLLECTION MAY 17.CSV', 'may','2017'),
          #('2017/7554150_ACT_DAYWISE_COLLECTION JUN 17.CSV', 'jun','2017'),
	]

data_l3 = [#('2017/7825939_ACT_DAYWISE_COLLECTION JUL 17.CSV', 'jul','2017'),
          #('2017/ACT_DAYWISE_COLLECTION_AUG_2017.CSV', 'aug','2017'),
        #   ('2017/8286126_ACT_DAYWISE_COLLECTION_SEP_2017.CSV','sep','2017'),
        # ('2017/8647264_ACT_DAYWISE_COLLECTION_OCT_2017.CSV','oct','2017'),
        # ('2017/8854739_ACT_DAYWISE_COLLECTION_NOV_2017.CSV','nov','2017'),
        # ('2017/9089853_ACT_DAYWISE_COLLECTION_DEC_2017.CSV','dec','2017'),
        # ('2018/hyd_collections_jan18.csv','jan','2018'),
        # ('2018/hyd_collections_feb18.csv','feb','2018'),
        # ('2018/hyd_collection_mar18.csv','mar','2018'),
        # ('2018/hyd_collections_apr18.CSV','apr','2018'),
	# ('2018/hyd_collections_may18.CSV','may','2018'),
	# ('jun18/ACT_DAYWISE_COLLECTION_June18.CSV','jun','2018')
	# ('july18/11475319_ACT_DAYWISE_COLLECTION_July18.CSV','jul','2018')
          ('aug18/11816893_ACT_DAYWISE_COLLECTION(2).csv','aug','2018')	

	]
TsFormat = '%d/%m/%Y %H:%M:%S %p'
DateFormat ='%m-%d-%Y'

def send_data_to_sql(csv,element, logic):
    print "loading {}".format(csv)
    connection = engine.connect()
    for chunk in pd.read_csv(csv, chunksize=chunksize, dtype=str,index_col=False, error_bad_lines=False):
        if logic ==1:            
            logic1(element,chunk)
        elif logic ==2:
            logic2(element,chunk)
	elif logic==3:
	    logic3(element,chunk)

    connection.close()
    print "load finished"
        

def date_clean(date):
    if isinstance(date,str):
        return get_dt_string(get_dt_from_string(date))

def get_dt_from_string(dt_str):
        return datetime.datetime.strptime(dt_str,TsFormat)

def get_dt_string(date):
        return date.strftime(DateFormat)

def date_clean2(date):
    if isinstance(date,str):
        return get_dt_string(datetime.datetime.strptime(date,'%d-%m-%Y'))

def list_values_to_lowercase(l):
    return [val.lower() for val in l]

def logic1(element,chunk):
    new_columns = ['ENTITY_CODE','BRANCH','AREANAME','BONU','BONTNAME','BONT_4X','ACCOUNT_NO','SUBSCRIBERTYPE','PARENTTYPE','PARENTID','NAME',
                    'ADDRESS1','ADDRESS2','HOME_PHONE','CELL_PHONE','EMAIL','USERNAME','PAYTERM','PACKAGE_NAME','SUBSCRIPTIONPERIOD',
                    'TRANSACTION_TYPE','BILLING_CYCLE_CODE','AGREEMENT_START_DATE','ACTUALDATE','CURRENTSTATUS','COLLECTIONDATE',
                    'COLLECTIONEXENAME','MODEOFPAYMENT','CHEQUENO','CHEQUEDATE','BANKBRANCH','BANK_NAME','COLLECTOR_CODE','COLLECTIONSBRANCH',
                    'RECEIPT_NO','PAYMENT_NO','RECEIPT_DATE','BOOK_NO','USER_ID','PACKAGEPRICE','OPENINGBALANCE','BILLAMOUNT','INSTALLATION',
                    'ADJUSTMENTS','TOTALPAYABLEAMOUNT','TOTALAMOUNTPAID','CLOSINGBALANCE','EXTERNAL_ENTITY','TRANSACTION_DATE',]
    #chunk = chunk[columns]
    chunk.columns = list_values_to_lowercase(new_columns)
    chunk['report_month'] = element[1]
    chunk['report_year'] = element[2]
    print 'Transferring chunk to postgres'
    chunk.to_sql(table_name, engine, index=False,if_exists='append')


def logic2(element,chunk):
    new_columns = ['ENTITY_CODE','BRANCH','AREANAME','BONU','BONTNAME','BONT_4X','ACCOUNT_NO','SUBSCRIBERTYPE','PARENTTYPE','PARENTID','NAME',
                    'ADDRESS1','ADDRESS2','HOME_PHONE','CELL_PHONE','EMAIL','USERNAME','PAYTERM','PACKAGE_NAME','SUBSCRIPTIONPERIOD',
                    'TRANSACTION_TYPE','BILLING_CYCLE_CODE','AGREEMENT_START_DATE','ACTUALDATE','CURRENTSTATUS','COLLECTIONDATE',
                    'COLLECTIONEXENAME','MODEOFPAYMENT','CHEQUENO','CHEQUEDATE','BANKBRANCH','BANK_NAME','COLLECTOR_CODE','COLLECTIONSBRANCH',
                    'RECEIPT_NO','PAYMENT_NO','RECEIPT_DATE','BOOK_NO','USER_ID','PACKAGEPRICE','OPENINGBALANCE','BILLAMOUNT','INSTALLATION',
                    'ADJUSTMENTS','TOTALPAYABLEAMOUNT','TOTALAMOUNTPAID','CLOSINGBALANCE','EXTERNAL_ENTITY','TRANSACTION_DATE','REMARKS']
    #chunk = chunk[columns]
    chunk.columns = list_values_to_lowercase(new_columns)
    chunk['report_month'] = element[1]
    chunk['report_year'] = element[2]
    print 'Transferring chunk to postgres'
    chunk.to_sql(table_name, engine, index=False,if_exists='append')

def logic3(element,chunk):
    new_columns = ['ENTITY_CODE','BRANCH','AREANAME','BONU','BONTNAME','BONT_4X','ACCOUNT_NO','SUBSCRIBERTYPE','PARENTTYPE','PARENTID','NAME',
                    'ADDRESS1','ADDRESS2','HOME_PHONE','CELL_PHONE','EMAIL','USERNAME','PAYTERM','PACKAGE_NAME','SUBSCRIPTIONPERIOD',
                    'TRANSACTION_TYPE','BILLING_CYCLE_CODE','AGREEMENT_START_DATE','ACTUALDATE','CURRENTSTATUS','COLLECTIONDATE',
                    'COLLECTIONEXENAME','MODEOFPAYMENT','CHEQUENO','CHEQUEDATE','BANKBRANCH','BANK_NAME','COLLECTOR_CODE','COLLECTIONSBRANCH',
                    'RECEIPT_NO','PAYMENT_NO','RECEIPT_DATE','BOOK_NO','USER_ID','PACKAGEPRICE','OPENINGBALANCE','BILLAMOUNT','INSTALLATION',
                    'ADJUSTMENTS','TOTALPAYABLEAMOUNT','TOTALAMOUNTPAID','CLOSINGBALANCE','EXTERNAL_ENTITY','TRANSACTION_DATE','REMARKS','NEW_PAYMODE']
    #chunk = chunk[columns]
    chunk.columns = list_values_to_lowercase(new_columns)
    chunk['report_month'] = element[1]
    chunk['report_year'] = element[2]
    print 'Transferring chunk to postgres'
    chunk.to_sql(table_name, engine, index=False,if_exists='append')

def create_table_name(csv):
    return csv.replace('/','_').replace(' ','_').split('(')[0]

def run():
    for file in data_l1:
        path = DATA_DIR + file[0]
        print path
        print os.path.isfile(path)
        #send_data_to_sql(path, file,1)

    for file in data_l2:
        path = DATA_DIR + file[0]
        print path
        print os.path.isfile(path)
        #send_data_to_sql(path, file,2)

    for file in data_l3:
        path = DATA_DIR + file[0]
        print path
        print os.path.isfile(path)
        send_data_to_sql(path, file,3)

if __name__=='__main__':
    run()


