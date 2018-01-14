# -*- coding: utf-8 -*-
# @Author   : Weizhou
# @Email    : 491315091@qq.com


"""
建议的设置：
itemsPerPage = 500
timeout = round(0.005*p+3.5)
sleep = 2
"""
import requests
import csv
import codecs
import time
import re
import math
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# 爬虫类
class libarySpider():

    def __init__(self):
        #设置登录状态的cookies
        self.cookies = raw_input(u'请输入cookies：'.encode('gbk'))
        # 保存已抓取的链接
        self.pages = []
        # 已经爬取过
        self.finishPages =[]
        # 初始化爬取出错数量
        self.errorPages = []
        # 连续错误
        self.repeatError = 0
        # 累计错误
        self.total_errors = 0
        # 邮件接收人
        self.to_list = ["491315091@qq.com","383951107@qq.com","gail.song@qq.com","rwwang@infor.ecnu.edu.cn"] 
        # 数据接送接口地址
        self.data_url = "http://202.120.82.182:9090/currentURL/rtlList"
        self.headers = {
                        'Cookie':self.cookies,
                        'host': "202.120.82.182:9090",
                        'Accept':'application/json, text/javascript, */*; q=0.01',
                        'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",
                        'cache-control': "no-cache",
                        'Connection':'close',
                        'Accept-Encoding':'gzip, deflate, sdch'
                        }
        
    

    # 续爬模式
    def add(self):

        self.stime = raw_input(u'请输入爬取日期(格式:yyyy-mm-dd)：'.encode('gbk'))
        # 表格每页记录数
        self.itemsPerPage = raw_input(u'请输入每页的日志记录数量：'.encode('gbk'))
        # 从日志读取当时的参数
        f = open('C:/ftp/'+self.stime+'.txt','r')
        self.numPages = []
        for line in f.readlines():
            self.numPages.append(line.rstrip().split('=>')[-1])
        f.close()


    # 生产模式
    def first(self):
        # 日期
        self.stime = raw_input(u'请输入爬取日期(格式:yyyy-mm-dd)：'.encode('gbk'))
        # 该日期总数据量
        self.numRows = raw_input(u'请输入日志记录总数：'.encode('gbk'))
        # 表格每页记录数
        self.itemsPerPage = raw_input(u'请输入每页的日志记录数量：'.encode('gbk'))
        self.numPages = self.caculatePages(self.numRows,self.itemsPerPage)

    
    # 快速的调试模式
    def debug(self):

        self.stime = '2017-10-21'
        self.numRows = '2000'
        self.itemsPerPage = '500'
        self.numPages = self.caculatePages(self.numRows,self.itemsPerPage)

    # 自定义模式
    def customize(self):
        # 日期
        self.stime = raw_input(u'请输入爬取日期(格式:yyyy-mm-dd)：'.encode('gbk'))
        self.startpage = raw_input(u'请输入起始页码：'.encode('gbk'))
        self.endpage = raw_input(u'请输入结束页码(包括该页码在内)：'.encode('gbk'))
        self.itemsPerPage = raw_input(u'请输入每页的日志记录数量：'.encode('gbk'))
        self.numPages = int(self.endpage)-int(self.startpage)+1
        


    # 计算分页总数
    def caculatePages(self,numRows,itemsPerPage):

        self.numPages = int(math.ceil(float(numRows)/float(itemsPerPage)))
        return self.numPages



    # 数据下载器和数据解析器
    # @profile # 性能测试，执行kernprof -l -v libary2.py
    def downloadPage(self):

        try:
            s = time.clock()
            while self.pages.__len__():
                page = self.pages.pop(0)
                
                items = {}
                
                # 计算爬虫运行到现在的时间
                c = time.clock()
                ctime = 1 if round((c-s)/60/60,4)==0 else round((c-s)/60/60,4)

                # 错误率计算：错误/爬取总数
                print u'当前正在抓取第%s页，总进度：%s/%s，错误数量：%s'%(page,len(self.finishPages)+1,self.numPages,self.errorPages)
                print u'已经耗时%s小时，成功抓取%s个页面，平均每分钟抓取%s个页面'%(ctime,len(self.finishPages),round(int(len(self.finishPages))/ctime/60))
                
                # 每爬取了1000页发送邮件监控
                if len(self.finishPages)%1000==0 and len(self.finishPages):
                    msg = '当前正在抓取第%s页，总进度：%s/%s，错误数量：%s'%(page,len(self.finishPages)+1,self.numPages,self.errorPages)
                    self.SendEmail(self.to_list,"1000页报告",msg)

                # url传参
                querystring = {"limit":self.itemsPerPage,"page":str(page),"stime":self.stime,"etime":self.stime,"sip":"","dip":"","platformId":"","websiteId":"","username":"","domain":"","url":"","action":"","platform":"","browser":"","line":"","dir":"","initParam":""}
                
                # 动态设置timeout
                timeout = round(0.007*int(page)+3)
                time.sleep(1.5)

                try:
                    aa =time.clock()
                    response = requests.get(self.data_url, timeout=timeout, headers=self.headers,params=querystring)
                    bb = time.clock()
                    print u'当前页面请求耗时%s'%(bb-aa)
                    data = response.json()['page']['list']
                    a = time.clock()
                    
                    # 解析和清洗数据
                    for item in data:
                        items['action']=item['action']
                        if item['dir']=="<img style='border:0px;' src='images/icons/inner.png'/>&nbsp;<img style='border:0px;' src='images/icons/forword.png'/>&nbsp;<img style='border:0px;' src='images/icons/outer.png'/>":
                            items['dir']=u'内-外'
                        else:
                            items['dir']=u'外-内'
                        items['domain']=item['domain']
                        items['domainname']=item['domainname']
                        items['ipaddr']=item['ipaddr']
                        items['plat']='|'.join(re.findall(r'(?<=title=\').*?(?=[/,\'])',item['plat']) if item['plat'] else '')
                        items['platformId']=item['platformId']
                        items['referer']=re.findall(r'(?<=href=\').*?(?=\'>)',item['referer'])[0] if item['referer'] else ''
                        items['rid']=item['rid']
                        items['sip']=item['sip']
                        items['starttime']=item['starttime']
                        items['url']=re.findall(r'(?<=href=\').*?(?=\'>)',item['url'])[0] if item['url'] else ''
                        items['websiteId']=item['websiteId']

                        # 写入csv文件
                        self.save_data(items)
                        self.repeatError = 0
       
                    self.finishPages.append(page)
                    print u'写入%s条记录到CSV文件'%len(data)
                    b = time.clock()
                    print u'当前页面解析存储耗时：%s'%(b-a)


                except Exception,e:
                    self.repeatError+=1
                    self.errorPages.append(page)
                    print e
                    # 错误写入日志文件
                    self.save_log(page,e)
                    # 连续多次报错监控
                    self.total_errors += 1
                    if self.total_errors<150:
                        if self.repeatError>30:
                            self.SendEmail(self.to_list,'严重错误通知','连续错误30次以上，累计错误%s,请检查爬虫'%self.total_errors)
                            self.repeatError = 0       
                            continue
                    else:
                        self.SendEmail(self.to_list,'！！爬虫程序休眠！！','原因：连续错误150次以上，请登录虚拟机检查')
                        print u'连续错误150次以上，程序自动停止运行'
                        time.sleep(10000)

                    

            # 爬虫结束时间
            e = time.clock()
            # 总运行时间，单位（小时）
            totaltime = round((e-s)/60/60,4)
            self.log.close()
            self.csvfile.close()
            print u'全部抓取完成'
            print u'共计耗时：%s小时'%totaltime
            # 发送邮件/
            msg = '<html><body><h3>%s全部抓取完毕:</h3><p>错误数量：%s</p><p>总计耗时：%s小时</P></body></html>'%(self.stime,self.errorPages,totaltime)
            r = self.SendEmail(self.to_list,'数据爬取报告',msg)
            print r
            print u'成功发送邮件'      
            return

        # 处理Ctrl+C事件
        except KeyboardInterrupt,e:
            self.log.close()
            self.csvfile.close()
            if self.notefile:
                self.notefile.close()
            self.errorPages.append(page)
            # self.save_log(page,e)
            print u'程序暂停，将%s加载到列表末尾'%page
            self.total_errors = 0
        # 其他错误
        except Exception,e:
            print e
        
    def run(self,mode='default'):
        # self.notefile = codecs.open('C:/ftp/'+self.stime+'.txt','a+',encoding='utf-8')
        # 模式选择
        if mode=='debug':
            self.debug()
            print u'开启调试模式'
            self.log = open('C:/ftp/'+self.stime+'.txt','w')
            self.pages = [p for p in range(1,self.numPages+1)]

        elif mode=='default':
            print u'开启批量模式'
            self.first()
            self.pages = [p for p in range(1,self.numPages+1)]
            self.notefile = codecs.open(u'C:/ftp/爬取记录.txt','ab+',encoding='utf-8')
            self.log = open('C:/ftp/'+self.stime+'.txt','w')
            # self.notefile.write('\n'+str(time.time())+'\n')
            # self.notefile.write('\t'.join([str(self.stime),str(self.numRows),str(self.numPages),str(self.itemsPerPage)])+'\n')
            self.SendEmail(self.to_list,'开始爬取%s数据'%self.stime,'爬虫开始运行')

        elif mode=='add':
            print u'开启增量模式'
            self.log = open('C:/ftp/'+self.stime+'.txt','w')
            self.add()
            self.pages = self.numPages

        elif mode=='recovery':
            self.log = open('C:/ftp/'+self.stime+'.txt','a+')
            self.total_errors=0
            print u'开启断点续爬模式'
            self.cookies = raw_input(u'请重新输入cookies：'.encode('gbk'))

        elif mode== 'customize':
            print u'开启自定义模式'
            self.customize()
            self.log = open(u'C:/ftp/'+self.stime+'自定义模式错误日志.txt','w')
            self.pages = [p for p in range(int(self.startpage),int(self.endpage)+1)]
        
        # 初始化数据和日志保存文件
        self.csvfile = codecs.open('C:/ftp/'+self.stime+'.csv', 'a+', encoding='utf-8_sig')
        self.downloadPage()




    # 数据存储
    def save_data(self,items):
        fieldnames = ['dir','action','domain','domainname','ipaddr','plat','platformId','referer','rid','sip','starttime','url','websiteId']
        writer = csv.DictWriter(self.csvfile,fieldnames=fieldnames)
        writer.writerow(items)

    # 爬虫日志存储
    def save_log(self,page,e):

        self.log.write(str(e)+' '+'on page=>'+str(page))
        self.log.write('\n')


    # 邮件发送函数
    def SendEmail(self,to_list,subject,text,debug=False):

        if not debug:
            from_addr = "491315091@qq.com"
            sslcode = "kioylteorsmzbiji"
            smtp_server = 'smtp.qq.com'

            # 如名字所示Multipart就是分多个部分
            msg = MIMEMultipart()
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg['To'] = ",".join(to_list)

            #---这是文字部分---
            part = MIMEText(text, 'html', 'utf-8')
            msg.attach(part)

            #---这是附件部分---
            # xlsx类型附件
            # part = MIMEApplication(open(u'JD显示器数据.xls', 'rb').read())
            # part.add_header('Content-Disposition', 'attachment', filename= "jd显示器排名.xls".encode('gb2312'))
            # msg.attach(part)

            try:
                s = smtplib.SMTP_SSL(smtp_server, 465)
                s.login(from_addr, sslcode)
                s.sendmail(from_addr, to_list, msg.as_string())
                s.quit()
                return u"邮件发送成功"
            except smtplib.SMTPException, e:
                return u"邮件发送失败,错误：%s" % e
        else:
            pass


# 测试用例
if __name__ == '__main__':
    spider = libarySpider()
    spider.run(mode='debug')
