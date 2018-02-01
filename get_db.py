# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     
   Description :
   Author :       ianchen
   date：          
-------------------------------------------------
   Change Activity:
                   2017/11/22:
-------------------------------------------------
"""
import pymssql


def get_db(companyid):
    conn = pymssql.connect(host='39.108.1.170', port='3433', user='Python', password='pl,okmPL<OKM',
                           database='CompanyCenter', autocommit=True, charset='utf8')
    cur = conn.cursor()
    sql = "[dbo].[Platform_Company_GetDBUrl]"
    params = (companyid, pymssql.output(str, ''))
    foo = cur.callproc(sql, params)
    jdbc = foo[-1]
    import re
    match = re.search(r'jdbc:sqlserver://(.*?):(\d+);database=(.*)', jdbc)
    host = match.group(1)
    port = int(match.group(2))
    db = match.group(3)
    conn.close()
    return host, port, db


def add_task(host, port, db, batchid, batchyear, batchmonth, companyid, customerid, jobname, jobparam):
    conn = pymssql.connect(host=host, port=port, user='Python', password='pl,okmPL<OKM', database=db, autocommit=True,
                           charset='utf8')
    cur = conn.cursor()
    sql = '[dbo].[Python_Serivce_Job_Add]'
    params = (batchid, batchyear, batchmonth, companyid, customerid, jobname, jobparam)
    foo = cur.callproc(sql, params)
    print(foo[-1])
    conn.close()

def job_finish(host, port, db,batchid,companyid,customerid,status,result):
    conn = pymssql.connect(host=host, port=port, user='Python', password='pl,okmPL<OKM', database=db, autocommit=True,
                           charset='utf8')
    cur = conn.cursor()
    sql = '[dbo].[Python_Serivce_Job_Finish]'
    params = (batchid,companyid,customerid,status,result)
    foo = cur.callproc(sql, params)
    conn.close()


