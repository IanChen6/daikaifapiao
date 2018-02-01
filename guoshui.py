# -*- coding:utf-8 -*-
import calendar
import random
import re
from urllib.parse import urlencode
from selenium.webdriver import ActionChains

__author__ = 'IanChen'
# selinium需要专用的driver来调用浏览器
import os
from selenium import webdriver
import time
import requests
import json
import base64
from lxml import etree
import pymssql
import threading
from selenium.webdriver.support import ui
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LTTextBoxHorizontal, LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
from pdfminer.pdfparser import PDFParser, PDFDocument
from suds.client import Client
import suds

try:
    import urlparse as parse
except:
    from urllib import parse
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import hashlib
from log_ging.log_01 import *
from get_db import job_finish
from get_db import get_db
import sys


#


class guoshui(object):
    def __init__(self, user, pwd, batchid, batchyear, batchmonth, companyid, customerid, logger):
        # self.logger = create_logger(path=os.path.basename(__file__) + str(customerid))
        self.logger = logger
        self.user = user
        self.pwd = pwd
        self.batchid = batchid
        self.batchyear = batchyear
        if 0 < batchmonth < 10:
            self.batchmonth = "0" + str(batchmonth)
            self.wholeyear = False
        elif batchmonth == 0:
            self.wholeyear = True
            self.batchmonth = 0
        else:
            self.batchmonth = batchmonth
            self.wholeyear = False
        self.companyid = companyid
        self.customerid = customerid
        self.host, self.port, self.db = get_db(companyid)
        if batchmonth != 0:
            monthRange = calendar.monthrange(batchyear, batchmonth)
            self.days = monthRange[1]
        if not os.path.exists('resource/{}'.format(user)):
            os.mkdir('resource/{}'.format(user))

    def upload_img(self, path):
        with open(path, 'rb') as a:
            upload_url = 'http://39.108.112.203:8687/uploadFile.php'
            split = path.split('.')
            if split[1] == 'png':
                data = {'fileType': '.png'}
            elif split[1] == 'html':
                data = {'fileType': '.html'}
            else:
                data = {'fileType': '.pdf'}
            files = {"imgfile": a.read()}
            r = requests.post(upload_url, data=data, files=files, timeout=10)
            imgname = re.search(r'filePath":"(.*?)"', r.text)
            imgname = imgname.group(1)
            return imgname

    def insert_db(self, sql, params):
        conn = pymssql.connect(host=self.host, port=self.port, user='Python', password='pl,okmPL<OKM',
                               database=self.db, charset='utf8', autocommit=True)
        cur = conn.cursor()
        if not cur:
            raise Exception("数据库连接失败")
        # cur.callproc('[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]', (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14))
        len(params)
        cur.callproc(sql, params)
        # conn.commit()
        cur.close()

    def get_db(self):
        conn = pymssql.connect(host='39.108.1.170', port='3433', user='Python', password='pl,okmPL<OKM',
                               database='CompanyCenter', autocommit=True, charset='utf8')
        cur = conn.cursor()
        sql = "[dbo].[Platform_Company_GetDBUrl]"
        params = (18282900, pymssql.output(str, ''))
        foo = cur.callproc(sql, params)
        jdbc = foo[-1]
        print(jdbc)
        import re
        match = re.search(r'jdbc:sqlserver://(.*?):(\d+);database=(.*)', jdbc)
        print(match.group(1), match.group(2), match.group(3))
        conn.close()

    def img2json(self, list):
        rawdata = {}
        for i in range(len(list)):
            rawdata["{}".format(i)] = list[i]
        json_data = json.dumps(rawdata)
        return json_data

    def jiami(self):
        h = hashlib.sha1(self.pwd.encode('utf8')).hexdigest()
        return h

    def save_png(self, browser, path):
        browser.save_screenshot(path)
        # browser.get_screenshot_as_file(path)
        img = self.upload_img(path)
        return img

    def taggertwo(self, tupian, md):
        while True:
            # formdata = {'CompanyID': 123456, 'BatchID': "1215454545", 'JobName': "pyj", 'CodeMD5': md, 'CodeData': tupian}
            # resp=requests.get(url="http://192.168.18.101:1421/SZYZService.asmx?wsdl",data=formdata)
            try:
                client = suds.client.Client(url="http://39.108.112.203:8701/SZYZService.asmx?wsdl")
                # client = suds.client.Client(url="http://192.168.18.101:1421/SZYZService.asmx?wsdl")
                # auto = client.service.GetYZCodeForDll(tupian)
                # if auto is not None:
                #     result1 = str(auto)
                #     return result1
                result = client.service.SetYZImg(123456, "1215454545", "pyj", md, tupian)
                # flag = login("91440300MA5DRRFB45", "10284784", result)
                for i in range(30):
                    result1 = client.service.GetYZCode(md)
                    if result1 is not None:
                        result1 = str(result1)
                        return result1
                    time.sleep(10)
            except Exception as e:
                self.logger.warn(e)
            self.insert_db("[dbo].[Python_Serivce_Job_Expire]", (self.batchid, self.customerid))
            break

    def tagger(self, tupian, md):
        while True:
            # formdata = {'CompanyID': 123456, 'BatchID': "1215454545", 'JobName': "pyj", 'CodeMD5': md, 'CodeData': tupian}
            # resp=requests.get(url="http://192.168.18.101:1421/SZYZService.asmx?wsdl",data=formdata)
            try:
                client = suds.client.Client(url="http://39.108.112.203:8701/SZYZService.asmx?wsdl")
                # client = suds.client.Client(url="http://192.168.18.101:1421/SZYZService.asmx?wsdl")
                auto = client.service.GetYZCodeForDll(tupian)
                if auto is not None:
                    result1 = str(auto)
                    return result1
                if auto is None:
                    return auto
                # result = client.service.SetYZImg(123456, "1215454545", "pyj", md, tupian)
                # # flag = login("91440300MA5DRRFB45", "10284784", result)
                # for i in range(30):
                #     result1 = client.service.GetYZCode(md)
                #     if result1 is not None:
                #         result1 = str(result1)
                #         return result1
                #     time.sleep(10)
            except Exception as e:
                self.logger.warn(e)
            break

    def parse_pdf(self, pdf_path):
        fp = open(pdf_path, "rb")
        # 用文件对象创建一个pdf文档分析器
        parse_pdf = PDFParser(fp)
        # 创建一个PDF文档
        doc = PDFDocument()
        parse_pdf.set_document(doc)
        doc.set_parser(parse_pdf)
        doc.initialize()
        # 检测文档是否提供txt转换，不提供就忽略
        if not doc.is_extractable:
            raise PDFTextExtractionNotAllowed
        else:
            # 创建PDf资源管理器 来管理共享资源
            rsrcmgr = PDFResourceManager()
            # 创建一个PDF参数分析器
            laparams = LAParams()
            # 创建聚合器
            device = PDFPageAggregator(rsrcmgr, laparams=laparams)
            # 创建一个PDF页面解释器对象
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            # 循环遍历列表，每次处理一页的内容
            # doc.get_pages() 获取page列表
            for page in doc.get_pages():
                # 使用页面解释器来读取
                interpreter.process_page(page)
                # 使用聚合器获取内容
                layout = device.get_result()
                results_last = ""
                # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等 想要获取文本就获得对象的text属性，
                for out in layout:
                    # 判断是否含有get_text()方法，图片之类的就没有
                    # if hasattr(out,"get_text"):
                    if isinstance(out, LTTextBoxHorizontal):
                        results = out.get_text()
                        if results_last == "税（费）种\n":
                            sz = results.strip("").split("\n")
                            print(sz)
                        if "7=5×6" in results:
                            jn = results.strip("").split("\n")
                            jn.pop(0)
                            print(jn)
                        results_last = results
        pdf_dict = {}
        for i in range(len(sz) - 3):
            pdf_dict[sz[i]] = jn[i]
        print(pdf_dict)
        return pdf_dict

    def login(self):
        try_times = 0
        while try_times <= 14:
            self.logger.info('customerid:{},开始尝试登陆'.format(self.customerid))
            try_times += 1
            if try_times>10:
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
            tag = json.dumps(tag)
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
                        return cookies
                    elif "账户和密码不匹配" in resp.json()['message'] or "不存在" in resp.json()['message'] or "已注销" in resp.json()['message']:
                        print('账号和密码不匹配')
                        self.logger.info('customerid:{}账号和密码不匹配'.format(self.customerid))
                        status="账号和密码不匹配"
                        return status
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
                    return cookies
                elif "账户和密码不匹配" in resp.json()['message'] or "不存在" in resp.json()['message'] or "已注销" in resp.json()['message']:
                    print('账号和密码不匹配')
                    self.logger.info('customerid:{}账号和密码不匹配'.format(self.customerid))
                    status = "账号和密码不匹配"
                    return status
                else:
                    time.sleep(3)
            else:
                self.logger.warn("customerid:{}登录失败,重试".format(self.customerid))
        self.logger.warn("{}登陆失败".format(self.customerid))
        return False

    def shuizhongchaxun(self, browser):
        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").clear()
        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").send_keys("增值税")
        shuiming = "增值税"
        self.parse_biaoge(browser, shuiming)

        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").clear()
        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").send_keys("财务报表")
        shuiming = "财务报表"
        self.parse_biaoge(browser, shuiming)

        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").clear()
        browser.find_element_by_css_selector("#sz .mini-buttonedit-input").send_keys("所得税")
        shuiming = "所得税"
        self.parse_biaoge(browser, shuiming)

    def parse_biaoge(self, browser, shuiming):
        self.logger.info("customerid:{}截取国税{}申报信息".format(self.customerid, shuiming))
        wait = ui.WebDriverWait(browser, 10)
        wait.until(lambda browser: browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input"))
        # 输入查询日期
        if self.wholeyear:
            for m in range(1, 13):
                year = self.batchyear
                if 0 < m < 10:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                    m = "0" + str(m)
                else:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                month = m
                qsrq = '{}{}01'.format(year, month)
                zzrq = '{}{}{}'.format(year, month, days)
                self.logger.info("customerid:{}查询{}月".format(self.customerid, m))
                browser.get(url='http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/cxdy/sbcx.html')
                wait = ui.WebDriverWait(browser, 10)
                wait.until(lambda browser: browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input"))
                browser.find_element_by_css_selector("#sz .mini-buttonedit-input").clear()
                browser.find_element_by_css_selector("#sz .mini-buttonedit-input").send_keys("{}".format(shuiming))
                browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input").clear()
                browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input").send_keys(qsrq)
                browser.find_element_by_css_selector("#sbrqz .mini-buttonedit-input").clear()
                browser.find_element_by_css_selector("#sbrqz .mini-buttonedit-input").send_keys(zzrq)
                browser.find_element_by_css_selector("#stepnext .mini-button-text").click()
                time.sleep(2)
                imgname = self.save_png(browser, 'resource/{}/国税{}{}月申报结果截图.png'.format(self.user, shuiming, month))
                # 表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="mini-grid-table-bodysbqkGrid"]/tbody/tr')
                a = 1
                for i in select[1:]:
                    shuizhong = i.xpath('.//text()')
                    a += 1
                    img_list = []
                    img_list.append(imgname)
                    img_list3 = []
                    if "查询申报表" in shuizhong:
                        self.logger.info("customerid:{}有申报表需要查询".format(self.customerid))
                        img_list3 = self.parse_shenbaobiao(browser, a, month)
                    img_list = img_list + img_list3
                    print(shuizhong)
                    self.logger.info("customerid:{}查询{}月完成".format(self.customerid, m))
                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid,
                        str(shuizhong[1]),
                        str(shuizhong[2]),
                        str(shuizhong[3]), str(shuizhong[4]), str(shuizhong[5]), str(shuizhong[6]),
                        self.img2json(img_list))
                    self.insert_db("[dbo].[Python_Serivce_GSTaxApplyShenZhen_Add]", params)
        else:
            year = self.batchyear
            month = self.batchmonth
            days = self.days
            qsrq = '{}{}01'.format(year, month)
            zzrq = '{}{}{}'.format(year, month, days)
            browser.get(url='http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/cxdy/sbcx.html')
            browser.find_element_by_css_selector("#sz .mini-buttonedit-input").clear()
            browser.find_element_by_css_selector("#sz .mini-buttonedit-input").send_keys("{}".format(shuiming))
            browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input").clear()
            browser.find_element_by_css_selector("#sbrqq .mini-buttonedit-input").send_keys(qsrq)
            browser.find_element_by_css_selector("#sbrqz .mini-buttonedit-input").clear()
            browser.find_element_by_css_selector("#sbrqz .mini-buttonedit-input").send_keys(zzrq)
            browser.find_element_by_css_selector("#stepnext .mini-button-text").click()
            time.sleep(2)
            imgname = self.save_png(browser, 'resource/{}/国税{}{}申报结果截图.png'.format(self.user, shuiming, month))
            # 表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="mini-grid-table-bodysbqkGrid"]/tbody/tr')
            a = 1
            for i in select[1:]:
                shuizhong = i.xpath('.//text()')
                a += 1
                img_list = []
                img_list.append(imgname)
                img_list3 = []
                if "查询申报表" in shuizhong:
                    self.logger.info("customerid:{}有申报表需要查询".format(self.customerid))
                    img_list3 = self.parse_shenbaobiao(browser, a, month)
                    self.logger.info("customerid:{}获取申报表完成".format(self.customerid))

                img_list = img_list + img_list3
                # logger.info("打印信息")
                print(shuizhong)
                self.logger.info(shuizhong)
                self.logger.info("customerid:{}开始插入数据库".format(self.customerid))
                params = (
                    self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid,
                    str(shuizhong[1]),
                    str(shuizhong[2]),
                    str(shuizhong[3]), str(shuizhong[4]), str(shuizhong[5]), str(shuizhong[6]),
                    self.img2json(img_list))
                self.logger.info(params)
                self.insert_db("[dbo].[Python_Serivce_GSTaxApplyShenZhen_Add]", params)
                self.logger.info("customerid:{}数据库插入完成".format(self.customerid))
        self.logger.info("customerid:{}截取国税申报信息已完成".format(self.customerid))

    # 申报表截图
    def parse_shenbaobiao(self, browser, a, month):
        browser.find_element_by_xpath('//table[@id="mini-grid-table-bodysbqkGrid"]/tbody/tr[%s]//a[1]' % (a,)).click()
        try:
            self.logger.info("customerid:{}申报表截图".format(self.customerid))
            wait = ui.WebDriverWait(browser, 5)
            wait.until(lambda browser: browser.find_element_by_css_selector(".mini-window iframe"))
            browser.find_element_by_class_name('mini-tools-max').click()
            frame_element = browser.find_element_by_css_selector('.mini-window iframe')
            browser.switch_to_frame(frame_element)
            # time.sleep(1)
            content_p = browser.page_source
            root2 = etree.HTML(content_p)
            select2 = root2.xpath('//table[@class="mini-tabs-header"]//span')
            b = 0
            img_list2 = []
            for i in select2:
                b += 1
                try:
                    browser.find_element_by_id('mini-1${}'.format(b)).click()
                    shenbaobiao = self.save_png(browser,
                                                'resource/{}/国税申报表截图{}{}{}月.png'.format(self.user, a, b, month))
                    img_list2.append(shenbaobiao)
                except Exception as e:
                    self.logger.error("出现错误:", e)
                    continue
            self.logger.info("customerid:{}申报表截图完成".format(self.customerid))
            browser.switch_to.default_content()
            self.logger.info("customerid:{}返回主页面".format(self.customerid))

            browser.find_element_by_class_name('mini-tools-close').click()
            self.logger.info("customerid:{}关闭当前申报表".format(self.customerid))

            return img_list2
        except Exception as e:
            self.logger.info(e)
            return []

    # 国税缴款
    def parse_jiaokuan(self, browser):
        self.logger.info("customerid:{}截取国税缴款信息".format(self.customerid))
        # 输入查询日期
        if self.wholeyear:
            for m in range(1, 13):
                year = self.batchyear
                if 0 < m < 10:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                    m = "0" + str(m)
                else:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                month = m
                qsrq = '{}{}01'.format(year, month)
                zzrq = '{}{}{}'.format(year, month, days)
                self.logger.info("customerid:{}缴款查询{}月".format(self.customerid, m))
                browser.find_element_by_css_selector("#sssqq .mini-buttonedit-input").clear()
                browser.find_element_by_css_selector("#sssqq .mini-buttonedit-input").send_keys('{}'.format(qsrq))
                browser.find_element_by_css_selector("#sssqz .mini-buttonedit-input").clear()
                browser.find_element_by_css_selector("#sssqz .mini-buttonedit-input").send_keys('{}'.format(zzrq))
                try:
                    browser.find_element_by_css_selector("#mini-37 .mini-button-text").click()
                except Exception as e:
                    self.logger.info("customerid:{}无弹窗".format(self.customerid))
                wait = ui.WebDriverWait(browser, 10)
                wait.until(lambda browser: browser.find_element_by_css_selector("#stepnext .mini-button-text"))
                browser.find_element_by_css_selector("#stepnext .mini-button-text").click()
                try:
                    browser.find_element_by_css_selector(".mini-tools-close ").click()
                except:
                    self.logger.info("customerid:{}处理没有缴款信息的错误".format(self.customerid))
                img = self.save_png(browser, 'resource/{}/缴税信息.png'.format(self.user))
                iml = []
                iml.append(img)
                # 表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="mini-grid-table-bodyyjscx"]/tbody/tr')
                for i in select[1:]:
                    jsxx = i.xpath('.//text()')
                    print(jsxx)
                    self.logger.warn(jsxx)
                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid,
                        str(jsxx[1]),
                        str(jsxx[2]),
                        str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]), str(jsxx[8]),
                        str(jsxx[9]),
                        self.img2json(iml))
                    self.logger.info(params)
                    self.insert_db("[dbo].[Python_Serivce_GSTaxChargeShenZhen_Add]", params)
        else:
            browser.find_element_by_css_selector("#sssqq .mini-buttonedit-input").clear()
            browser.find_element_by_css_selector("#sssqq .mini-buttonedit-input").send_keys(
                '{}{}01'.format(self.batchyear, self.batchmonth))
            browser.find_element_by_css_selector("#sssqz .mini-buttonedit-input").clear()
            browser.find_element_by_css_selector("#sssqz .mini-buttonedit-input").send_keys(
                '{}{}{}'.format(self.batchyear, self.batchmonth, self.days))
            try:
                browser.find_element_by_css_selector("#mini-37 .mini-button-text").click()
            except Exception as e:
                self.logger.info(e)
                print("没有弹窗")
            wait = ui.WebDriverWait(browser, 10)
            wait.until(lambda browser: browser.find_element_by_css_selector("#stepnext .mini-button-text"))
            browser.find_element_by_css_selector("#stepnext .mini-button-text").click()
            time.sleep(3)
            img = self.save_png(browser, 'resource/{}/缴税信息.png'.format(self.user))
            iml = []
            iml.append(img)

            # 表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="mini-grid-table-bodyyjscx"]/tbody/tr')
            for i in select[1:]:
                jsxx = i.xpath('.//text()')
                print(jsxx)
                self.logger.warn(jsxx)
                params = (
                    self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid,
                    str(jsxx[1]),
                    str(jsxx[2]),
                    str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]), str(jsxx[8]), str(jsxx[9]),
                    self.img2json(iml))
                self.logger.info(params)
                self.insert_db("[dbo].[Python_Serivce_GSTaxChargeShenZhen_Add]", params)
            self.logger.info("customerid:{}截取国税缴款信息已完成".format(self.customerid))

    # 前往地税
    def qwdishui(self, browser):
        try_times = 0
        while try_times <= 3:
            ds_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/djsxx/djsxx.html'
            browser.get(url=ds_url)
            self.logger.info("customerid:{}开始登录地税".format(self.customerid))
            wait = ui.WebDriverWait(browser, 10)
            try:
                wait.until(lambda browser: browser.find_element_by_css_selector("#mini-29 .mini-button-text"))
                browser.find_element_by_css_selector("#mini-29 .mini-button-text").click()
            except:
                print("无该弹窗")
            try:
                browser.find_element_by_css_selector("#mini-27 .mini-button-text").click()
            except:
                print("无该弹窗")
            browser.find_element_by_xpath("//a[@href='javascript:gotoDs()']").click()
            try:
                self.dishui(browser)
                return True
            except Exception as e:
                self.logger.warn(e)
                browser.get("http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html")
                try_times += 1

    def dishui(self, browser):
        self.logger.info("customerid:{}截取地税申报信息".format(self.customerid))
        time.sleep(2)
        windows = browser.window_handles
        window1 = browser.current_window_handle
        for c_window in windows:
            if c_window != window1:
                browser.close()
                browser.switch_to_window(c_window)
        if self.wholeyear:
            self.logger.info("customerid:{},地税全年数据".format(self.customerid))
            wait = ui.WebDriverWait(browser, 10)
            wait.until(
                lambda browser: browser.find_element_by_css_selector(
                    "#layui-layer1 div.layui-layer-btn a"))  # timeout
            browser.find_element_by_css_selector('#layui-layer1 div.layui-layer-btn a').click()
            browser.find_element_by_css_selector('#menu_110000_110109').click()
            time.sleep(2)
            browser.switch_to_frame('qyIndex')
            sbjkcx = browser.page_source
            wait.until(lambda browser: browser.find_element_by_css_selector("#menu2_13_110200"))  # 容易timeout
            browser.find_element_by_css_selector('#menu2_13_110200').click()
            time.sleep(2)
            browser.find_element_by_css_selector('#menu3_15_110202').click()
            browser.switch_to_frame('qymain')
            wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
            for m in range(1, 13):
                browser.switch_to_default_content()
                browser.find_element_by_css_selector('#menu_110000_110109').click()
                time.sleep(2)
                browser.switch_to_frame('qyIndex')
                sbjkcx = browser.page_source
                wait.until(lambda browser: browser.find_element_by_css_selector("#menu2_13_110200"))  # 容易timeout
                browser.find_element_by_css_selector('#menu2_13_110200').click()
                time.sleep(2)
                browser.find_element_by_css_selector('#menu3_15_110202').click()
                browser.switch_to_frame('qymain')
                wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
                year = self.batchyear
                if 0 < m < 10:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                    m = "0" + str(m)
                else:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                month = m
                qsrq = '{}-{}-01'.format(year, month)
                zzrq = '{}-{}-{}'.format(year, month, days)
                self.logger.info("customerid:{}查询{}月".format(self.customerid, m))
                # 查询个人所得税
                wait.until(lambda browser: browser.find_element_by_css_selector('#zsxmDm'))
                browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                    '//option[@value="10106"]').click()  # 选择个人所得税
                sb_startd = browser.find_element_by_css_selector('#sbqq')
                sb_startd.clear()
                sb_startd.send_keys(qsrq)
                sb_endd = browser.find_element_by_css_selector('#sbqz')
                sb_endd.clear()
                sb_endd.send_keys(zzrq)
                # time.sleep(1)
                browser.find_element_by_css_selector('#query').click()
                time.sleep(2)
                grsd = self.save_png(browser, 'resource/{}/地税个人所得税已申报查询.png'.format(self.user))
                self.logger.info("customerid:{}查询{}月个人所得税信息".format(self.customerid, m))
                # 表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
                index = 0
                pg = browser.page_source
                if "没有" not in pg:
                    for i in select:
                        pdf_list = []
                        pdf_list.append(grsd)
                        browser.find_element_by_xpath(
                            '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                                index)).click()
                        time.sleep(3)
                        browser.find_element_by_css_selector('#print').click()
                        # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                        jsxx = i.xpath('.//text()')
                        pzxh = jsxx[0]
                        print(jsxx)
                        b_ck = browser.get_cookies()
                        ck = {}
                        for x in b_ck:
                            ck[x['name']] = x['value']
                        post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                                 browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                        post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                                   'Accept-Language': 'zh-CN,zh;q=0.8',
                                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                                   'X-Requested-With': 'XMLHttpRequest'}
                        resp = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                             cookies=ck).text
                        pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                    cookies=ck).content
                        if "错误" not in resp:
                            with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                                w.write(pdf_content)
                            pdf = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                            pdf_list.append(pdf)
                        params = (
                            self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                            str(jsxx[1]),
                            None,
                            str(jsxx[2]), None,
                            str(jsxx[3]), None, None,
                            self.img2json(pdf_list))  # self.img2json("申报表详情{}.pdf".format(pzxh))
                        self.logger.info(params)
                        self.logger.info("customerid:{}查询{}月个人所得税信息插入数据库".format(self.customerid, m))
                        self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                        self.logger.info("customerid:{}查询{}月数据入库完成".format(self.customerid, m))
                        index += 1
                self.logger.info("customerid:{}查询{}月个人所得税查询完毕".format(self.customerid, m))
                # 企业所得税
                browser.switch_to_default_content()
                browser.switch_to_frame('qyIndex')
                browser.switch_to_frame('qymain')
                wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
                browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                    '//option[@value="10104"]').click()  # 选择企业所得税
                sb_startd = browser.find_element_by_css_selector('#sbqq')
                sb_startd.clear()
                sb_startd.send_keys(qsrq)
                sb_endd = browser.find_element_by_css_selector('#sbqz')
                sb_endd.clear()
                sb_endd.send_keys(zzrq)
                # time.sleep(1)
                browser.find_element_by_css_selector('#query').click()
                time.sleep(2)
                qysd = self.save_png(browser, 'resource/{}/地税企业所得税已申报查询.png'.format(self.user))
                self.logger.info("customerid:{}查询{}月地税企业所得祱所得税查询".format(self.customerid, m))
                # 表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
                index = 0
                pg = browser.page_source
                if "没有" not in pg:
                    for i in select:
                        pdf_list = []
                        pdf_list.append(qysd)
                        browser.find_element_by_xpath(
                            '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                                index)).click()
                        time.sleep(2)
                        browser.find_element_by_css_selector('#print').click()
                        # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                        jsxx = i.xpath('.//text()')
                        pzxh = jsxx[0]
                        print(jsxx)
                        b_ck = browser.get_cookies()
                        ck = {}
                        for x in b_ck:
                            ck[x['name']] = x['value']
                        post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                                 browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                        post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                                   'Accept-Language': 'zh-CN,zh;q=0.8',
                                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                                   'X-Requested-With': 'XMLHttpRequest'}
                        resp2 = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                              cookies=ck).text
                        pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                    cookies=ck).content

                        if "错误" not in resp2:
                            with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                                w.write(pdf_content)
                            pdf2 = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                            pdf_list.append(pdf2)

                        params = (
                            self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                            str(jsxx[1]),
                            str(jsxx[2]),
                            str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]),
                            self.img2json(pdf_list))  # self.img2json("申报表详情{}.pdf".format(pzxh))
                        self.logger.info(params)
                        self.logger.info("customerid:{}查询{}月数据入库".format(self.customerid, m))
                        self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                        self.logger.info("customerid:{}查询{}月数据入库完成".format(self.customerid, m))
                        index += 1
                self.logger.info("customerid:{}查询{}月地税企业所得祱所得税查询完成".format(self.customerid, m))
                # 城市建设税
                browser.switch_to_default_content()
                browser.switch_to_frame('qyIndex')
                browser.switch_to_frame('qymain')
                wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
                browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                    '//option[@value="10109"]').click()  # 选择城市建设税
                sb_startd = browser.find_element_by_css_selector('#sbqq')
                sb_startd.clear()
                sb_startd.send_keys(qsrq)
                sb_endd = browser.find_element_by_css_selector('#sbqz')
                sb_endd.clear()
                sb_endd.send_keys(zzrq)
                # time.sleep(1)
                browser.find_element_by_css_selector('#query').click()
                time.sleep(2)
                csjs = self.save_png(browser, 'resource/{}/地税城市建设税已申报查询.png'.format(self.user))
                self.logger.info("customerid:{}查询{}月城市建设税查询完成".format(self.customerid, m))
                # 表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
                index = 0
                pg = browser.page_source
                if "没有" not in pg:
                    for i in select:
                        pdf_list = []
                        pdf_list.append(csjs)
                        browser.find_element_by_xpath(
                            '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                                index)).click()
                        time.sleep(2)
                        browser.find_element_by_css_selector('#print').click()
                        # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                        jsxx = i.xpath('.//text()')
                        pzxh = jsxx[0]
                        print(jsxx)
                        b_ck = browser.get_cookies()
                        ck = {}
                        for x in b_ck:
                            ck[x['name']] = x['value']
                        post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                                 browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                        post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                        headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                                   'Accept-Language': 'zh-CN,zh;q=0.8',
                                   'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                                   'X-Requested-With': 'XMLHttpRequest'}
                        resp1 = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                              cookies=ck).text
                        pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                    cookies=ck).content

                        if "错误" not in resp1:
                            with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                                w.write(pdf_content)
                            time.sleep(0.5)
                            pdf1 = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                            pdf_list.append(pdf1)
                            pdf_dict = self.parse_pdf("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                            js = self.img2json(pdf_list)
                            js = json.loads(js)
                            js["pdf数据"] = pdf_dict
                            pdf_json = json.dumps(js, ensure_ascii=False)
                            print(pdf_json)
                        else:
                            pdf_json = self.img2json(pdf_list)
                        params = (
                            self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                            str(jsxx[1]),
                            str(jsxx[2]),
                            str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]),
                            pdf_json)  # self.img2json("申报表详情{}.pdf".format(pzxh))
                        self.logger.info(params)
                        self.logger.info("customerid:{}查询{}月地税数据入库".format(self.customerid, m))
                        self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                        self.logger.info("customerid:{}查询{}月地税数据入库完成".format(self.customerid, m))
                        index += 1
                elif "没有" in pg:
                    self.logger.info("customerid:{}查询{}月城建税已申报无数据".format(self.customerid, m))
                    browser.switch_to_default_content()
                    browser.find_element_by_css_selector('#menu_100000_102001').click()
                    browser.switch_to_frame('qyIndex')
                    wait.until(lambda browser: browser.find_element_by_css_selector("#menu3_3_102001"))
                    browser.find_element_by_css_selector('#menu3_3_102001').click()
                    browser.switch_to_frame('qymain')
                    time.sleep(2)
                    dspage = browser.page_source
                    dsroot = etree.HTML(dspage)
                    dsjudge = dsroot.xpath('//*[@id="tbody"]/tr')
                    for i in dsjudge:
                        xgm = i.xpath('.//text()')
                        if '查无数据' in xgm:
                            self.logger.info("customerid:{}查询{}月城建税三项无数据".format(self.customerid, m))
                            params = (
                                self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, "", "")
                            self.insert_db('[dbo].[Python_Serivce_DSTaxApplyShenZhen_NoDS3]', params)
                self.logger.info("customerid:{}截取地税申报信息已完成".format(self.customerid))
        if self.wholeyear:
            for m in range(1, 13):
                year = self.batchyear
                if 0 < m < 10:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                    m = "0" + str(m)
                else:
                    monthRange = calendar.monthrange(year, m)
                    days = monthRange[1]
                month = m
                qsrq = '{}-{}-01'.format(year, month)
                zzrq = '{}-{}-{}'.format(year, month, days)
                self.logger.info("customerid:{}查询{}月".format(self.customerid, m))
                # 已缴款查询
                self.logger.info("customerid:{}截取地税缴款信息".format(self.customerid))
                gbds = browser.window_handles
                dq = browser.current_window_handle
                for s in gbds:
                    if s != dq:
                        browser.switch_to_window(s)
                        browser.close()
                        browser.switch_to_window(dq)
                browser.switch_to_default_content()
                browser.find_element_by_css_selector('#menu_110000_110109').click()
                browser.switch_to_frame('qyIndex')
                wait.until(lambda browser: browser.find_element_by_css_selector("#menu2_13_110200"))  # 容易timeout
                browser.find_element_by_css_selector('#menu2_13_110200').click()
                browser.find_element_by_css_selector('#menu3_17_110204').click()
                browser.switch_to_frame('qymain')
                gbds = browser.window_handles
                dq = browser.current_window_handle
                for s in gbds:
                    if s != dq:
                        browser.switch_to_window(s)
                        browser.close()
                        browser.switch_to_window(dq)
                page = browser.page_source
                # browser.switch_to_window(window1)
                wait = ui.WebDriverWait(browser, 10)
                wait.until(lambda browser: browser.find_element_by_css_selector('#jkqq'))
                ds_start_date = browser.find_element_by_xpath('//*[@id="jkqq"]')
                ds_start_date.clear()
                ds_start_date.send_keys(qsrq)
                ds_end_date = browser.find_element_by_xpath("//*[@id='jkqz']")
                ds_end_date.clear()
                ds_end_date.send_keys(zzrq)
                # time.sleep(1)
                browser.find_element_by_css_selector('#query').click()
                time.sleep(2)
                jietu = self.save_png(browser, 'resource/{}/地税已缴款查询.png'.format(self.user))

                # 缴款表格信息爬取
                content = browser.page_source
                root = etree.HTML(content)
                select = root.xpath('//table[@id="yjkxx_table"]/tbody/tr')
                index2 = 0
                pz_l = []
                pz_t = 0
                jietulist = []
                jietulist.append(jietu)
                for i in select:
                    jkxx = i.xpath('.//text()')
                    if "没有符合条件的数据" in jkxx:
                        break
                    pz = jkxx[0]
                    print(jkxx)
                    pz_l.append(pz)
                    if pz != pz_t:
                        browser.find_element_by_xpath(
                            '//table[@id="yjkxx_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                                index2)).click()
                        time.sleep(1)
                        wait.until(lambda browser: browser.find_element_by_css_selector('#cxjkmx'))
                        browser.find_element_by_css_selector('#cxjkmx').click()
                        time.sleep(1)
                        windows = browser.window_handles
                        window2 = browser.current_window_handle
                        for c_window in windows:
                            if c_window != window2:
                                browser.switch_to_window(c_window)
                                cc = browser.page_source
                                time.sleep(0.5)
                                if "错误" not in cc:
                                    png_name = "resource/{}/缴款凭证号{}.png".format(self.user, pz)
                                    j = self.save_png(browser, png_name)
                                    jietulist.append(j)
                                    sbsj = {}
                                    bb = browser.page_source
                                    root = etree.HTML(bb)
                                    zgsb = root.xpath('//table[@id="lineTable"]/tbody/tr')
                                    for i in zgsb[1:-1]:
                                        cjb = i.xpath('./td/text()')
                                        zt = cjb[2]
                                        out = "".join(cjb[6].strip())
                                        sbsj[zt] = out
                                    cb = self.img2json(jietulist)
                                    cb = json.loads(cb)
                                    cb["缴款数据"] = sbsj
                                    jkjs = json.dumps(cb, ensure_ascii=False)
                                    print(jkjs)
                                # browser.save_screenshot(png_name)
                                browser.close()
                                browser.switch_to_window(window2)
                                time.sleep(1)
                                browser.switch_to_frame('qyIndex')
                                browser.switch_to_frame('qymain')
                    else:
                        jkjs = self.img2json(jietulist)
                    pz_t = pz
                    index2 += 1
                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(jkxx[0]),
                        str(jkxx[1]),
                        str(jkxx[2]),
                        str(jkxx[3]), str(jkxx[4]), str(jkxx[5]), str(jkxx[6]), str(jkxx[7]),
                        jkjs)
                    self.logger.info(params)
                    self.insert_db("[dbo].[Python_Serivce_DSTaxChargeShenZhen_Add]", params)
                self.logger.info("customerid:{}截取地税缴款信息已完成".format(self.customerid))
        else:
            # 查询个人所得税
            wait = ui.WebDriverWait(browser, 10)
            wait.until(
                lambda browser: browser.find_element_by_css_selector("#layui-layer1 div.layui-layer-btn a"))  # timeout
            browser.find_element_by_css_selector('#layui-layer1 div.layui-layer-btn a').click()
            browser.find_element_by_css_selector('#menu_110000_110109').click()
            time.sleep(2)
            browser.switch_to_frame('qyIndex')
            sbjkcx = browser.page_source
            wait.until(lambda browser: browser.find_element_by_css_selector("#menu2_13_110200"))  # 容易timeout
            browser.find_element_by_css_selector('#menu2_13_110200').click()
            time.sleep(2)
            browser.find_element_by_css_selector('#menu3_15_110202').click()
            browser.switch_to_frame('qymain')
            wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
            time.sleep(0.5)
            browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                '//option[@value="10106"]').click()  # 选择个人所得税
            sb_startd = browser.find_element_by_css_selector('#sbqq')
            sb_startd.clear()
            sb_startd.send_keys('{}-{}-01'.format(self.batchyear, self.batchmonth))
            sb_endd = browser.find_element_by_css_selector('#sbqz')
            sb_endd.clear()
            sb_endd.send_keys('{}-{}-{}'.format(self.batchyear, self.batchmonth, self.days))
            # time.sleep(1)
            browser.find_element_by_css_selector('#query').click()
            time.sleep(2)
            grsd = self.save_png(browser, 'resource/{}/地税个人所得税已申报查询.png'.format(self.user))
            self.logger.info("customerid:{}地税个人所得税信息查询".format(self.customerid))

            # 表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
            index = 0
            pg = browser.page_source
            if "没有" not in pg:
                for i in select:
                    pdf_list = []
                    pdf_list.append(grsd)
                    browser.find_element_by_xpath(
                        '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                            index)).click()
                    time.sleep(2)
                    browser.find_element_by_css_selector('#print').click()
                    # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                    jsxx = i.xpath('.//text()')
                    pzxh = jsxx[0]
                    print(jsxx)
                    b_ck = browser.get_cookies()
                    ck = {}
                    for x in b_ck:
                        ck[x['name']] = x['value']
                    post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                             browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                    post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                    headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                               'Accept-Language': 'zh-CN,zh;q=0.8',
                               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                               'X-Requested-With': 'XMLHttpRequest'}
                    resp = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                         cookies=ck).text
                    pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                cookies=ck).content

                    if "错误" not in resp:
                        with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                            w.write(pdf_content)
                        pdf = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                        pdf_list.append(pdf)
                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                        str(jsxx[1]),
                        None,
                        str(jsxx[2]), "",
                        str(jsxx[3]), "", "",
                        self.img2json(pdf_list))  # self.img2json("申报表详情{}.pdf".format(pzxh))
                    self.logger.info(params)
                    self.logger.info("customerid:{}地税个人所得税数据入库".format(self.customerid))
                    self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                    self.logger.info("customerid:{}地税个人所得税数据入库完成".format(self.customerid))
                    index += 1

            # 企业所得税
            browser.switch_to_default_content()
            browser.switch_to_frame('qyIndex')
            browser.switch_to_frame('qymain')
            wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
            browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                '//option[@value="10104"]').click()  # 选择企业所得税
            sb_startd = browser.find_element_by_css_selector('#sbqq')
            sb_startd.clear()
            sb_startd.send_keys('{}-{}-01'.format(self.batchyear, self.batchmonth))
            sb_endd = browser.find_element_by_css_selector('#sbqz')
            sb_endd.clear()
            sb_endd.send_keys('{}-{}-{}'.format(self.batchyear, self.batchmonth, self.days))
            # time.sleep(1)
            browser.find_element_by_css_selector('#query').click()
            time.sleep(2)
            qysd = self.save_png(browser, 'resource/{}/地税企业所得税已申报查询.png'.format(self.user))
            self.logger.info("customerid:{}地税企业所得税查询".format(self.customerid))

            # 表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
            index = 0
            pg = browser.page_source
            if "没有" not in pg:
                for i in select:
                    pdf_list = []
                    pdf_list.append(qysd)
                    browser.find_element_by_xpath(
                        '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                            index)).click()
                    time.sleep(2)
                    browser.find_element_by_css_selector('#print').click()
                    # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                    jsxx = i.xpath('.//text()')
                    pzxh = jsxx[0]
                    print(jsxx)
                    b_ck = browser.get_cookies()
                    ck = {}
                    for x in b_ck:
                        ck[x['name']] = x['value']
                    post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                             browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                    post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                    headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                               'Accept-Language': 'zh-CN,zh;q=0.8',
                               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                               'X-Requested-With': 'XMLHttpRequest'}
                    resp2 = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                          cookies=ck).text
                    pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                cookies=ck).content

                    if "错误" not in resp2:
                        with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                            w.write(pdf_content)
                        pdf2 = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                        pdf_list.append(pdf2)
                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                        str(jsxx[1]),
                        str(jsxx[2]),
                        str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]),
                        self.img2json(pdf_list))  # self.img2json("申报表详情{}.pdf".format(pzxh))
                    self.logger.info(params)
                    self.logger.info("customerid:{}地税企业所得税数据入库".format(self.customerid))
                    self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                    self.logger.info("customerid:{}地税企业所得税数据入库完成".format(self.customerid))
                    index += 1

            # 城市建设税
            browser.switch_to_default_content()
            browser.switch_to_frame('qyIndex')
            browser.switch_to_frame('qymain')
            wait.until(lambda browser: browser.find_element_by_css_selector('#sbqq'))
            browser.find_element_by_css_selector('#zsxmDm').find_element_by_xpath(
                '//option[@value="10109"]').click()  # 选择城市建设税
            sb_startd = browser.find_element_by_css_selector('#sbqq')
            sb_startd.clear()
            sb_startd.send_keys('{}-{}-01'.format(self.batchyear, self.batchmonth))
            sb_endd = browser.find_element_by_css_selector('#sbqz')
            sb_endd.clear()
            sb_endd.send_keys('{}-{}-{}'.format(self.batchyear, self.batchmonth, self.days))
            # time.sleep(1)
            browser.find_element_by_css_selector('#query').click()
            time.sleep(2)
            csjs = self.save_png(browser, 'resource/{}/地税城市建设税已申报查询.png'.format(self.user))
            self.logger.info("customerid:{}地税城建税".format(self.customerid))

            # 表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="ysbjl_table"]/tbody/tr')
            index = 0
            pg = browser.page_source
            if "没有" not in pg:
                for i in select:
                    pdf_list = []
                    pdf_list.append(csjs)
                    browser.find_element_by_xpath(
                        '//table[@id="ysbjl_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                            index)).click()
                    time.sleep(2)
                    browser.find_element_by_css_selector('#print').click()
                    # url=browser.find_element_by_name('sbbFormCj').get_attribute('action')
                    jsxx = i.xpath('.//text()')
                    pzxh = jsxx[0]
                    print(jsxx)
                    b_ck = browser.get_cookies()
                    ck = {}
                    for x in b_ck:
                        ck[x['name']] = x['value']
                    post_url = parse.urljoin("https://dzswj.szds.gov.cn",
                                             browser.find_element_by_name('sbbFormCj').get_attribute('action'))
                    post_data = {'SubmitTokenTokenId': '', 'yzpzxhArray': pzxh, 'btSelectItem': 'on'}
                    headers = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                               'Accept-Language': 'zh-CN,zh;q=0.8',
                               'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
                               'X-Requested-With': 'XMLHttpRequest'}
                    resp1 = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                          cookies=ck).text
                    pdf_content = requests.post(url=post_url, headers=headers, data=post_data, timeout=10,
                                                cookies=ck).content

                    if "错误" not in resp1:
                        with open("resource/{}/申报表详情{}.pdf".format(self.user, pzxh), 'wb') as w:
                            w.write(pdf_content)
                        time.sleep(0.5)
                        pdf1 = self.upload_img("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                        pdf_list.append(pdf1)
                        pdf_dict = self.parse_pdf("resource/{}/申报表详情{}.pdf".format(self.user, pzxh))
                        js = self.img2json(pdf_list)
                        js = json.loads(js)
                        js["pdf数据"] = pdf_dict
                        pdf_json = json.dumps(js, ensure_ascii=False)
                        print(pdf_json)
                    else:
                        pdf_json = self.img2json(pdf_list)

                    params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(pzxh),
                        str(jsxx[1]),
                        str(jsxx[2]),
                        str(jsxx[3]), str(jsxx[4]), str(jsxx[5]), str(jsxx[6]), str(jsxx[7]),
                        pdf_json)  # self.img2json("申报表详情{}.pdf".format(pzxh))
                    self.logger.info(params)
                    self.logger.info("customerid:{}地税城建税数据入库".format(self.customerid))
                    self.insert_db("[dbo].[Python_Serivce_DSTaxApplyShenZhen_Add]", params)
                    self.logger.info("customerid:{}地税城建税数据入库完成".format(self.customerid))
                    index += 1
            elif "没有" in pg:
                self.logger.info("customerid:{}地税城建无数据".format(self.customerid))
                browser.switch_to_default_content()
                browser.find_element_by_css_selector('#menu_100000_102001').click()
                browser.switch_to_frame('qyIndex')
                wait.until(lambda browser: browser.find_element_by_css_selector("#menu3_3_102001"))
                browser.find_element_by_css_selector('#menu3_3_102001').click()
                browser.switch_to_frame('qymain')
                time.sleep(2)
                dspage = browser.page_source
                dsroot = etree.HTML(dspage)
                dsjudge = dsroot.xpath('//*[@id="tbody"]/tr')
                for i in dsjudge:
                    xgm = i.xpath('.//text()')
                    if '查无数据' in xgm:
                        self.logger.info("customerid:{}地税城建三项无数据".format(self.customerid))
                        params = (
                        self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, "", "")
                        self.insert_db('[dbo].[Python_Serivce_DSTaxApplyShenZhen_NoDS3]', params)
            self.logger.info("customerid:{}截取地税申报信息已完成".format(self.customerid))
            # 已缴款查询
            gbds = browser.window_handles
            dq = browser.current_window_handle
            for s in gbds:
                if s != dq:
                    browser.switch_to_window(s)
                    browser.close()
                    browser.switch_to_window(dq)
            browser.switch_to_default_content()
            browser.find_element_by_css_selector('#menu_110000_110109').click()
            browser.switch_to_frame('qyIndex')
            browser.find_element_by_css_selector('#menu3_17_110204').click()
            browser.switch_to_frame('qymain')

            page = browser.page_source
            # browser.switch_to_window(window1)
            wait = ui.WebDriverWait(browser, 10)
            wait.until(lambda browser: browser.find_element_by_css_selector('#jkqq'))
            ds_start_date = browser.find_element_by_xpath('//*[@id="jkqq"]')
            ds_start_date.clear()
            ds_start_date.send_keys('{}-{}-01'.format(self.batchyear, self.batchmonth))
            ds_end_date = browser.find_element_by_xpath("//*[@id='jkqz']")
            ds_end_date.clear()
            ds_end_date.send_keys('{}-{}-{}'.format(self.batchyear, self.batchmonth, self.days))
            # time.sleep(1)
            browser.find_element_by_css_selector('#query').click()
            time.sleep(2)
            jietu = self.save_png(browser, 'resource/{}/地税已缴款查询.png'.format(self.user))

            # 缴款表格信息爬取
            content = browser.page_source
            root = etree.HTML(content)
            select = root.xpath('//table[@id="yjkxx_table"]/tbody/tr')
            index2 = 0
            pz_l = []
            pz_t = 0
            jietulist = []
            jietulist.append(jietu)

            for i in select:
                jkxx = i.xpath('.//text()')
                pz = jkxx[0]
                print(jkxx)
                pz_l.append(pz)
                if pz != pz_t:
                    browser.find_element_by_xpath(
                        '//table[@id="yjkxx_table"]/tbody/tr[@data-index="{}"]//input[@name="btSelectItem"]'.format(
                            index2)).click()
                    time.sleep(2)
                    wait.until(lambda browser: browser.find_element_by_css_selector('#cxjkmx'))
                    browser.find_element_by_css_selector('#cxjkmx').click()
                    time.sleep(2)
                    windows = browser.window_handles
                    window2 = browser.current_window_handle
                    for c_window in windows:
                        if c_window != window2:
                            browser.switch_to_window(c_window)
                            cc = browser.page_source
                            time.sleep(0.5)
                            print(c_window)
                            print(pz)
                            png_name = "resource/{}/缴款凭证号{}.png".format(self.user, pz)
                            j = self.save_png(browser, png_name)
                            jietulist.append(j)
                            sbsj = {}
                            bb = browser.page_source
                            root = etree.HTML(bb)
                            zgsb = root.xpath('//table[@id="lineTable"]/tbody/tr')
                            for i in zgsb[1:-1]:
                                cjb = i.xpath('./td/text()')
                                zt = cjb[2]
                                out = "".join(cjb[6].strip())
                                sbsj[zt] = out
                            cb = self.img2json(jietulist)
                            cb = json.loads(cb)
                            cb["缴款数据"] = sbsj
                            jkjs = json.dumps(cb, ensure_ascii=False)
                            print(jkjs)
                            browser.close()
                            browser.switch_to_window(window2)
                            time.sleep(1)
                            browser.switch_to_frame('qyIndex')
                            browser.switch_to_frame('qymain')
                else:
                    jkjs = self.img2json(jietulist)
                pz_t = pz
                index2 += 1
                params = (
                    self.batchid, self.batchyear, self.batchmonth, self.companyid, self.customerid, str(jkxx[0]),
                    str(jkxx[1]),
                    str(jkxx[2]),
                    str(jkxx[3]), str(jkxx[4]), str(jkxx[5]), str(jkxx[6]), str(jkxx[7]),
                    jkjs)
                self.logger.info(params)
                self.insert_db("[dbo].[Python_Serivce_DSTaxChargeShenZhen_Add]", params)
            self.logger.info("customerid:{}截取地税缴款信息已完成".format(self.customerid))

    def excute_spider(self):
        try:
            cookies = self.login()
            self.logger.info("customerid:{}获取cookies".format(self.customerid))
            jsoncookies = json.dumps(cookies,ensure_ascii=False)
            if "账号和密码不匹配" in jsoncookies:
                self.logger.warn("customerid:{}账号和密码不匹配".format(self.customerid))
                job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-2', "账号和密码不匹配")
                return
            with open('cookies/{}cookies.json'.format(self.customerid), 'w') as f:  # 将login后的cookies提取出来
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
            browser = webdriver.PhantomJS(
                executable_path='D:/BaiduNetdiskDownload/phantomjs-2.1.1-windows/bin/phantomjs.exe',
                desired_capabilities=dcap,service_args=service_args)
            # browser = webdriver.PhantomJS(
            #     executable_path='/home/tool/phantomjs-2.1.1-linux-x86_64/bin/phantomjs',
            #     desired_capabilities=dcap)
            browser.implicitly_wait(10)
            browser.viewportSize = {'width': 2200, 'height': 2200}
            browser.set_window_size(1400, 1600)  # Chrome无法使用这功能
            # browser = webdriver.Chrome(executable_path='D:/BaiduNetdiskDownload/chromedriver.exe')  # 添加driver的路径
        except Exception as e:
            self.logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "浏览器启动失败")
            return False
        try:
            index_url = "http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html"
            browser.get(url=index_url)
            browser.delete_all_cookies()
            with open('cookies/{}cookies.json'.format(self.customerid), 'r', encoding='utf8') as f:
                cookielist = json.loads(f.read())
            for (k, v) in cookielist.items():
                browser.add_cookie({
                    'domain': '.szgs.gov.cn',  # 此处xxx.com前，需要带点
                    'name': k,
                    'value': v,
                    'path': '/',
                    'expires': None})
            shenbao_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/cxdy/sbcx.html'
            browser.get(url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html")
            browser.get(url=shenbao_url)
            time.sleep(3)
            self.shuizhongchaxun(browser)
        except Exception as e:
            self.logger.info("customerid:{}GSYCX出错".format(self.customerid))
            self.logger.warn(e)
            self.logger.info("国税已申报查询失败")
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "国税已申报查询失败")
            browser.quit()
            return False
        try:
            # 国税缴款查询
            jk_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/djsxx/jk_jsxxcx.html'
            browser.get(url=jk_url)
            self.parse_jiaokuan(browser)
        except Exception as e:
            self.logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "国税已缴款查询失败")
            browser.quit()
            return False
        try:
            # 地税查询
            self.qwdishui(browser)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '1', '成功爬取')
            print("爬取完成")
            self.logger.info("customerid:{}全部爬取完成".format(self.customerid))
            browser.quit()
        except Exception as e:
            self.logger.warn(e)
            job_finish(self.host, self.port, self.db, self.batchid, self.companyid, self.customerid, '-1', "地税查询失败")
            browser.quit()
# start = time.time()
# gs = guoshui(user="440300754285743", pwd="77766683", batchid=2017, batchmonth=4, batchyear=2017, companyid=18282900,
#              customerid=13)
# cookies, session = gs.login()
# jsoncookies = json.dumps(cookies)
# with open('cookies.json', 'w') as f:  # 将login后的cookies提取出来
#     f.write(jsoncookies)
#     f.close()
# # chrome_options = Options()
# # chrome_options.add_argument("--window-size=1280,2000")
# # browser = webdriver.Chrome(executable_path='D:/BaiduNetdiskDownload/chromedriver.exe')  # 添加driver的路径
# # browser = webdriver.Chrome(executable_path='F:\web_driver_for_chrome/chromedriver.exe')  # 添加driver的路径
#
# dcap = dict(DesiredCapabilities.PHANTOMJS)
# dcap["phantomjs.page.settings.userAgent"] = (
#     'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36')
# dcap["phantomjs.page.settings.loadImages"] = False
# browser = webdriver.PhantomJS(executable_path='D:/BaiduNetdiskDownload/phantomjs-2.1.1-windows/bin/phantomjs.exe',
#                               desired_capabilities=dcap)  # 添加driver的路径
# # browser = webdriver.PhantomJS(executable_path='F:/phantomjs_driver/phantomjs-2.1.1-windows/bin/phantomjs.exe',
# #                               desired_capabilities=dcap)  # 添加driver的路径
# browser.viewportSize = {'width': 2200, 'height': 2200}
# browser.implicitly_wait(10)
# browser.set_window_size(1400, 1600)  # Chrome无法使用这功能
#
# index_url = "http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html"
# browser.get(url=index_url)
# browser.delete_all_cookies()
# with open('cookies.json', 'r', encoding='utf8') as f:
#     cookielist = json.loads(f.read())
# for (k, v) in cookielist.items():
#     browser.add_cookie({
#         'domain': '.szgs.gov.cn',  # 此处xxx.com前，需要带点
#         'name': k,
#         'value': v,
#         'path': '/',
#         'expires': None})
# shenbao_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/cxdy/sbcx.html'
# browser.get(url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/myoffice/myoffice.html")
# browser.get(url=shenbao_url)
#
# # threads=[]
# # gs.shuizhongchaxun(browser)
# # t1=threading.Thread(target=gs.parse_biaoge,args=(browser,))
# # 国税缴款查询
# # jk_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/djsxx/jk_jsxxcx.html'
# # browser.get(url=jk_url)
#
# # gs.parse_jiaokuan(browser)
#
# # zzsfp_url="http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/fp/zzszyfpdksq/fp_zzszyfpdksq.html"
# # browser.get(url=zzsfp_url)
# # browser.switch_to_frame("txsqxx")
# # cont=browser.page_source
#
#
# # 地税查询
# ds_url = 'http://dzswj.szgs.gov.cn/BsfwtWeb/apps/views/sb/djsxx/djsxx.html'
# browser.get(url=ds_url)
#
# gs.qwdishui(browser)
# # t3=threading.Thread(target=gs.dishui,args=(browser,))
# # threads.append(t1)
# # threads.append(t2)
# # threads.append(t3)
# # for t in threads:
# #     t.start()
#
# end = time.time()
# expend = end - start
# print(expend)
