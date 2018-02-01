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
import hashlib
import json

import os

import datetime
import redis
import re
from suds.client import Client
import suds
from selenium.webdriver import DesiredCapabilities
from log_ging.log_01 import *
import requests
from lxml import etree
import time
from selenium import webdriver
from selenium.webdriver.support import ui
from get_db import job_finish, get_db
import pymssql

from guoshui import guoshui

class daikai(guoshui):
    def __init__(self, user,pwd, batchid, companyid, customerid,logger):
        self.headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Language': 'zh-CN,zh;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Connection': 'keep-alive',
                        'Host': 'www.szcredit.org.cn',
                        'Cookie': 'UM_distinctid=160a1f738438cb-047baf52e99fc4-e323462-232800-160a1f73844679; ASP.NET_SessionId=4bxqhcptbvetxqintxwgshll',
                        'Origin': 'https://www.szcredit.org.cn',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Referer': 'https://www.szcredit.org.cn/web/gspt/newGSPTList.aspx?keyword=%u534E%u88D4&codeR=28',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36',
                        'X-Requested-With': 'XMLHttpRequest',
                        }
        self.user=user
        self.pwd=pwd
        self.batchid = batchid
        self.companyid = companyid
        self.customerid = customerid
        self.host, self.port, self.db = get_db(companyid)
        self.logger=logger

    def login(self):
        try_times = 0
        while try_times <= 14:
            self.logger.info('customerid:{},开始尝试登陆'.format(self.customerid))
            try_times += 1
            if try_times > 10:
                time.sleep(1)
            session = requests.session()
            # proxy_list = get_all_proxie()
            # proxy = proxy_list[random.randint(0, len(proxy_list) - 1)]
            try:
                session.proxies = sys.argv[1]

            except:
                self.logger.info("未传代理参数，启用本机IP")
            # session.proxies = {'https': 'http://116.22.211.55:6897', 'http': 'http://116.22.211.55:6897'}
            headers = {'Host': 'dzswj.szgs.gov.cn',
                       'Accept': 'application/json, text/javascript, */*; q=0.01',
                       'Accept-Language': 'zh-CN,zh;q=0.8',
                       'Content-Type': 'application/json; charset=UTF-8',
                       'Referer': 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                       'x-form-id': 'mobile-signin-form',
                       'X-Requested-With': 'XMLHttpRequest',
                       'Origin': 'http://dzswj.szgs.gov.cn'}
            session.get("http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html", headers=headers)
            captcha_url = 'http://dzswj.szgs.gov.cn/tipCaptcha'
            tupian_resp = session.get(url=captcha_url, timeout=10)
            tupian_resp.encoding = 'utf8'
            tupian = tupian_resp.json()
            image = tupian['image']
            tipmessage = tupian["tipMessage"]
            tupian = json.dumps(tupian, ensure_ascii=False)
            m = hashlib.md5()
            tupian1 = tupian.encode(encoding='utf8')
            m.update(tupian1)
            md = m.hexdigest()
            print(md)
            # logger.info("customerid:{},:{}".format(self.customerid,tupian))
            tag = self.tagger(tupian, md)
            self.logger.info("customerid:{}，获取验证码为：{}".format(self.customerid, tag))
            if tag is None:
                continue
            jyjg = session.post(url='http://dzswj.szgs.gov.cn/api/checkClickTipCaptcha', data=tag)
            self.logger.info("customerid:{}，验证验证码{}".format(self.customerid, tag))
            time_l = time.localtime(int(time.time()))
            time_l = time.strftime("%Y-%m-%d %H:%M:%S", time_l)
            self.logger.info("customerid:{}，转换tag".format(self.customerid))
            tag = json.dumps(tag)
            self.logger.info("customerid:{}，转换tag完成".format(self.customerid))
            self.logger.info("customerid:{}，{},{},{},{}".format(self.customerid, self.user, self.jiami(), tag, time_l))
            login_data = '{"nsrsbh":"%s","nsrpwd":"%s","redirectURL":"","tagger":%s,"time":"%s"}' % (
                self.user, self.jiami(), tag, time_l)
            login_url = 'http://dzswj.szgs.gov.cn/api/auth/clientWt'
            resp = session.post(url=login_url, data=login_data)
            self.logger.info("customerid:{},成功post数据".format(self.customerid))
            # panduan=resp.json()['message']
            # self.logger(panduan)
            try:
                if "验证码正确" in jyjg.json()['message']:
                    if "登录成功" in resp.json()['message']:
                        print('登录成功')
                        self.logger.info('customerid:{}pass'.format(self.customerid))
                        cookies = {}
                        for (k, v) in zip(session.cookies.keys(), session.cookies.values()):
                            cookies[k] = v
                        return cookies, session
                    elif "账户和密码不匹配" in resp.json()['message'] or "不存在" in resp.json()['message'] or "已注销" in \
                            resp.json()['message']:
                        print('账号和密码不匹配')
                        self.logger.info('customerid:{}账号和密码不匹配'.format(self.customerid))
                        status = "账号和密码不匹配"
                        return status, session
                    else:
                        time.sleep(3)
            except Exception as e:
                self.logger.warn("customerid:{}登录失败".format(self.customerid))
            self.logger.warn("customerid:{}登录失败,开始重试".format(self.customerid))
        try_handed = 0
        while try_handed <= 3:
            self.logger.info("customerid:{}手动登陆".format())
            try_handed += 1
            session = requests.session()
            # proxy_list = get_all_proxie()
            # proxy = proxy_list[random.randint(0, len(proxy_list) - 1)]
            try:
                session.proxies = sys.argv[1]
            except:
                print("未传入代理参数")
            # session.proxies = {'https': 'http://116.22.211.55:6897', 'http': 'http://116.22.211.55:6897'}
            headers = {'Host': 'dzswj.szgs.gov.cn',
                       'Accept': 'application/json, text/javascript, */*; q=0.01',
                       'Accept-Language': 'zh-CN,zh;q=0.8',
                       'Content-Type': 'application/json; charset=UTF-8',
                       'Referer': 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html',
                       'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                       'x-form-id': 'mobile-signin-form',
                       'X-Requested-With': 'XMLHttpRequest',
                       'Origin': 'http://dzswj.szgs.gov.cn'}
            session.get("http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/login/login.html", headers=headers)
            captcha_url = 'http://dzswj.szgs.gov.cn/tipCaptcha'
            tupian_resp = session.get(url=captcha_url, timeout=10)
            tupian_resp.encoding = 'utf8'
            tupian = tupian_resp.json()
            image = tupian['image']
            tipmessage = tupian["tipMessage"]
            tupian = json.dumps(tupian, ensure_ascii=False)
            m = hashlib.md5()
            tupian1 = tupian.encode(encoding='utf8')
            m.update(tupian1)
            md = m.hexdigest()
            print(md)
            tag = self.taggertwo(tupian, md)
            jyjg = session.post(url='http://dzswj.szgs.gov.cn/api/checkClickTipCaptcha', data=tag)
            time_l = time.localtime(int(time.time()))
            time_l = time.strftime("%Y-%m-%d %H:%M:%S", time_l)
            tag = json.dumps(tag)
            login_data = '{"nsrsbh":"%s","nsrpwd":"%s","redirectURL":"","tagger":%s,"time":"%s"}' % (
                self.user, self.jiami(), tag, time_l)
            login_url = 'http://dzswj.szgs.gov.cn/api/auth/clientWt'
            resp = session.post(url=login_url, data=login_data)
            panduan = resp.json()['message']
            if "验证码正确" in jyjg.json()['message']:
                if "登录成功" in resp.json()['message']:
                    print('登录成功')
                    cookies = {}
                    for (k, v) in zip(session.cookies.keys(), session.cookies.values()):
                        cookies[k] = v
                    return cookies, session
                elif "账户和密码不匹配" in resp.json()['message'] or "不存在" in resp.json()['message'] or "已注销" in resp.json()[
                    'message']:
                    print('账号和密码不匹配')
                    self.logger.info('customerid:{}账号和密码不匹配'.format(self.customerid))
                    status = "账号和密码不匹配"
                    return status, session
                else:
                    time.sleep(3)
            else:
                self.logger.warn("customerid:{}登录失败,重试".format(self.customerid))
        self.logger.warn("{}登陆失败".format(self.customerid))
        return False

    def parse(self,browser):
        content=browser.page_source
        root = etree.HTML(content)
        select = root.xpath('//table[@id="mini-grid-table-bodydbsxgrid"]/tbody/tr')
        a = 0
        for i in select[1:]:
            sum = {}
            wenshujieguo = i.xpath('.//text()')
            if "发票代开" in wenshujieguo[1]:
                deb=browser.page_source
                browser.find_element_by_xpath('//*[@id="mini-20${}$5"]/a[1]'.format(a)).click()
                time.sleep(2)
                tanchuang=browser.page_source
                iframe=browser.find_element_by_css_selector('.mini-panel iframe')
                browser.switch_to_frame(iframe)
                dkfw=browser.find_element_by_css_selector('#dkfpjefw_view').text
                dkhw=browser.find_element_by_css_selector('#dkfpje_view').text
                zpfw=browser.find_element_by_css_selector('#zpdkjefw_view').text
                zphw=browser.find_element_by_css_selector('#zpdkjehw_view').text
                dkfp = {}
                dkfp["普通发票：服务"] = dkfw
                dkfp["普通发票：货物"] = dkhw
                dkfp["专用发票：服务"] = zpfw
                dkfp["专用发票：货物"] = zphw
                sum["代开发票信息概览"] = dkfp
                xf = {}
                xf['纳税人识别号'] = browser.find_element_by_css_selector('#xfnsrsbh_view').text
                xf['纳税人名称'] = browser.find_element_by_css_selector('#xfnsrmc_view').text
                xf['地址'] = browser.find_element_by_css_selector('#xfdz_view').text
                xf['经营范围'] = browser.find_element_by_css_selector('#xfjyfw_view').text
                xf['开户银行'] = browser.find_element_by_css_selector('#yhhbmc_view').text
                xf['银行账号'] = browser.find_element_by_css_selector('#xfyhzh_view').text
                xf['经办人'] = browser.find_element_by_css_selector('#xfjbr_view').text
                xf['联系电话'] = browser.find_element_by_css_selector('#xflxdh_view').text
                xf['备注'] = browser.find_element_by_css_selector('#bz_view').text
                sum["销售方纳税人信息"] = xf
                gf = {}
                gf['纳税人识别号'] =browser.find_element_by_css_selector('#gfnsrsbh_view').text
                gf['纳税人名称'] = browser.find_element_by_css_selector('#gfnsrmc_view').text
                gf['地址'] = browser.find_element_by_css_selector('#gfdz_view').text
                gf['银行营业网点名称'] = browser.find_element_by_css_selector('#ghfyhyywdmc_view').text
                gf['开户银行'] = browser.find_element_by_css_selector('#gfkhyh_view').text
                gf['银行账号'] = browser.find_element_by_css_selector('#gfyhzh_view').text
                gf['联系电话'] = browser.find_element_by_css_selector('#gflxdh_view').text
                gf['代开类型'] = browser.find_element_by_css_selector('#dkfplx_view').text
                gf['征收品目'] = browser.find_element_by_css_selector('#zspm_view').text
                sum["购买方纳税人信息"] = gf
                tc=browser.page_source
                root2 = etree.HTML(tc)
                select2 = root2.xpath('//table[@id="mini-grid-table-bodyzzsdkfpGrid_view"]/tbody/tr')
                b = 1
                fwmc = {}
                for i in select2[1:]:
                    fwlist = i.xpath('.//text()')
                    trans = {}
                    for j in fwlist:
                        trans['货物或应税劳务名称、服务名称'] = fwlist[0]
                        trans['金额'] = fwlist[5]
                        trans['数量'] = fwlist[3]
                        trans['单位'] = fwlist[2]
                        trans['单价'] = fwlist[4]
                        trans['规格型号'] = fwlist[1]
                        # trans['金额合计'] = xq['jehj']
                        fwmc[b] = trans
                        b += 1
                sum["货物或应税劳务名称、服务名称"] = fwmc
                select3 = root2.xpath('//table[@id="mini-grid-table-bodyynskGrid_view"]/tbody/tr')
                b = 1
                ynsk = {}
                for i in select3[1:]:
                    fwlist = i.xpath('.//text()')
                    trans = {}
                    for j in fwlist:
                        trans['征收项目'] = fwlist[0]
                        trans['征收品目'] = fwlist[1]
                        trans['税款所属期起'] = fwlist[2]
                        trans['税款所属期止'] = fwlist[3]
                        trans['收入总额'] = fwlist[4]
                        trans['计税依据'] = fwlist[5]
                        trans['税率'] = fwlist[6]
                        trans['应纳税额'] = fwlist[7]
                        trans['减免税费额'] = fwlist[8]
                        trans['已缴税额'] = fwlist[9]
                        trans['应缴税额'] = fwlist[10]
                        ynsk[b] = trans
                        b += 1
                sum["应纳税款信息"] = ynsk
                sum=json.dumps(sum,ensure_ascii=False)
            a+=1
            params=(self.companyid,self.customerid,str(wenshujieguo[1]),str(wenshujieguo[2]),str(wenshujieguo[3]),str(wenshujieguo[4]),sum)
            self.insert_db("[dbo].[Python_Serivce_GSInvoiceDetail_Add]",params)
            browser.switch_to_default_content()
            browser.find_element_by_css_selector('.mini-tools-close').click()

    def excute_spider(self):
        try:
            cookies, session = self.login()
            self.logger.info("customerid:{}获取cookies".format(self.customerid))
            jsoncookies = json.dumps(cookies, ensure_ascii=False)
            if "账号和密码不匹配" in jsoncookies:
                self.logger.warn("customerid:{}账号和密码不匹配".format(self.customerid))
                job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-2',
                           "账号和密码不匹配")
                return
            with open('cookies/{}cookies.json'.format(self.batchid), 'w') as f:  # 将login后的cookies提取出来
                f.write(jsoncookies)
                f.close()
        except Exception as e:
            self.logger.warn(e)
            self.logger.warn("customerid:{}登陆失败".format(self.customerid))
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "登录失败")
            return False
        try:
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = (
                'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36')
            dcap["phantomjs.page.settings.loadImages"] = True
            service_args = []
            service_args.append('--webdriver=szgs')
            # browser = webdriver.PhantomJS(
            #     executable_path='D:/BaiduNetdiskDownload/phantomjs-2.1.1-windows/bin/phantomjs.exe',
            #     desired_capabilities=dcap,service_args=service_args)
            browser = webdriver.PhantomJS(
                executable_path='/home/tool/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
                desired_capabilities=dcap)
            browser.implicitly_wait(10)
            browser.viewportSize = {'width': 2200, 'height': 2200}
            browser.set_window_size(1400, 1600)  # Chrome无法使用这功能
            # options = webdriver.ChromeOptions()
            # options.add_argument('disable-infobars')
            # options.add_argument("--start-maximized")
            # browser = webdriver.Chrome(executable_path='D:/BaiduNetdiskDownload/chromedriver.exe',chrome_options=options)  # 添加driver的路径
        except Exception as e:
            self.logger.warn(e)
            self.logger.warn("浏览器启动失败")
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "浏览器启动失败")
            return False
        try:
            index_url = "http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html"
            browser.get(url=index_url)
            browser.delete_all_cookies()
            with open('cookies/{}cookies.json'.format(self.batchid), 'r', encoding='utf8') as f:
                cookielist = json.loads(f.read())
            for (k, v) in cookielist.items():
                browser.add_cookie({
                    'domain': '.szgs.gov.cn',  # 此处xxx.com前，需要带点
                    'name': k,
                    'value': v,
                    'path': '/',
                    'expires': None})
            shenbao_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sscx/wsxxcx/wsxxcx.html'
            browser.get(url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html")
            browser.get(url=shenbao_url)
            time.sleep(3)
            sfzrd = self.parse(browser)

        except Exception as e:
            self.logger.info("customerid:{}SFZ出错".format(self.customerid))
            self.logger.warn(e)
            self.logger.info("SFZ查询失败")
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "SFZ查询失败")
            browser.quit()
            return False

import sys
logger = create_logger(path=os.path.dirname(sys.argv[0]).split('/')[-1])
redis_cli = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
def run_test(user, pwd, batchid, companyid, customerid):
    print("++++++++++++++++++++++++++++++++++++")
    print('jobs[ts_id=%s] running....' % batchid)
    time.sleep(3)
    try:
        hz = daikai(user=user,pwd=pwd , batchid=batchid, companyid=companyid,
                          customerid=customerid,logger=logger)
        hz.excute_spider()
    except Exception as e:
        logger.error(e)
    print('jobs[ts_id=%s] done' % batchid)
    result = True
    return result


while True:
    # ss=redis_cli.lindex("list",0)
    ss = redis_cli.lpop("daikai")
    if ss is not None:
        # print(redis_cli.lpop("list"))
        sd = json.loads(ss)
        run_test(sd["1"], sd["2"], sd["3"], sd["4"], sd["5"])
    else:
        time.sleep(10)
        print("no task waited")
