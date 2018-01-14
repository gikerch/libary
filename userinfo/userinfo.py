# -*- coding: utf-8 -*-
# @Author   : Weizhou
# @Email    : 491315091@qq.com

import numpy as np
import urlparse
import os
import pandas as pd
import datetime
import time
import requests
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# from email.mime.application import MIMEApplication

# 邮件发送函数
def SendEmail(subject,text,debug=False):
    to_list = ["491315091@qq.com","383951107@qq.com","gail.song@qq.com","rwwang@infor.ecnu.edu.cn"] 

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


# 遍历目录
def gci(path, allfile, fileformat):  
    filelist =  os.listdir(path)   
    for filename in filelist:  
        filepath = os.path.join(path, filename)  
        if os.path.isdir(filepath):  
            gci(filepath, allfile)  
        else:
            if os.path.splitext(filepath)[1] == fileformat: # 后缀名为.csv
                allfile.append(filepath)  
    return allfile 


# 读取数据
def read_data(filepath):
    # 读取csv
    df = pd.read_csv(filepath,encoding='utf-8_sig',names=[u'方向',u'请求',u'域名',u'描述',u'目的IP地址',u'浏览器',u'归属平台',u'引用',u'sip',u'源IP地址',u'时间',u'URL',u'归属网站'])
    # # 删除部分列
    df = df.drop([u'目的IP地址',u'描述',u'归属网站',u'方向',u'浏览器',u'请求'],axis=1)
    # # 列索引重命名
    df = df.rename(columns={u'时间':'visit_time',u'源IP地址':'ip','URL':'url',\
        u'域名':'domain',u'归属平台':'platform',u'引用':'reference'})
    df['visit_time'] = df['visit_time'].apply(lambda x:'2017-'+x)
    df = df.sort_values(by=['ip','visit_time']) # 多列排序
    df.reset_index(drop=True) # 重置索引cf5xxv
    # df = df.set_index('visit_time')
    # df['visit_time'] = df.index
    str2timestamp = lambda x: pd.Timestamp(x).value//10**9-28800 #时区处理
    df['timestamp'] = df.visit_time.apply(str2timestamp)
    return df

# 分割URL函数
def splitUrl(df):
    ParsePath = lambda x: urlparse.urlparse(x).path
    ParseParmas = lambda x: urlparse.urlparse(x).query
    pathdeep = lambda x: x.count('/')
    df['path'] = df['url'].apply(ParsePath) # 路径
    df['path_deep'] = df['path'].apply(pathdeep) # 路径深度
    df['parmas'] = df['url'].apply(ParseParmas) # 参数
    return df  

# 获取token
def get_token():
    tokentime = time.time()
    token = None
    params = {'grant_type': 'client_credentials',
              'client_id': 'testclient', 'client_secret': 'testpass'}
    try:
        r = requests.post(
        "https://network-api.ecnu.edu.cn/oauth/token.php", data=params)
        token = r.text[17:57]
        return token, tokentime
    except Exception,e:
        print u'获取token失败',e
        SendEmail(u'获取token失败',u'获取token失败')


# 获取用户信息
def getUserInfo(df,log):
    access_token, tokentime = get_token() # 获取token
    df['apartment'] = np.nan
    df['add_time'] = np.nan
    df['drop_time'] = np.nan
    df['user_encrypt'] = np.nan
    df['gender'] = np.nan
    df['major'] = np.nan
    df['RYLX'] = np.nan
    df['year'] = np.nan
    
    preip = 0 # 上一条记录的ip
    drop_time = 0 # 下线时间
    errors = []
    
    # 统计信息
    dif_ip = 0 # 不同ip计数
    ip_timeout = 0 # 同IP超过下线时间计数
    ip_nottimeout = 0 #同IP未超过下线时间
    skip = 0 # 跳过计数
    no_userinfo = 0
    # global dif_ip,ip_timeout,ip_nottimeout,skip,no_userinfo

    for index,row in df.iterrows():
        print index
        error_msg = ''
        if index%1000==0:
            print '#'*20
            print u'当前是第{}条记录'.format(index)
            print '#'*20

         # 判断token是否过期
        if tokentime + 3000 < time.time():
            print u'token过期重新获取'
            access_token, tokentime = get_token()
        ip,ctime = row[4],row[7] # 需要更改
        print ip,ctime # IP地址,访问时间
    
        # 传递参数
        payload = {
                    'access_token': access_token,
                    'user_ip': ip,
                    'user_time': ctime
        }

        # ip变化
        if ip != preip:
            dif_ip += 1
            preip = ip # 重新设置preip
            print u'IP不同，重新获取用户信息#{}'.format(dif_ip)    
            time.sleep(0.2)
            try:
                userinfo = requests.get(
            'https://network-api.ecnu.edu.cn/oauth/userinfo.php', params=payload)\
                .text.replace("\r\n\r\n","")
                userinfo = json.loads(userinfo)
                # 获取到信息
                if userinfo.has_key(u'user_info'): #
                    # print u'成功获取用户信息'
                    print userinfo
                    df.loc[index,'add_time'] = userinfo[u'add_time']
                    df.loc[index,'drop_time'] = userinfo[u'drop_time']
                    drop_time = ctime+800 if userinfo[u'drop_time']==0 else userinfo[u'drop_time']
                    if userinfo['user_info'].has_key(u'BMMC'):
                        df.loc[index,'apartment'] = userinfo['user_info'][u'BMMC']  
                    if userinfo['user_info'].has_key(u'user_encrypt'):
                        df.loc[index,'user_encrypt'] = userinfo['user_info'][u'user_encrypt']
                    if userinfo['user_info'].has_key(u'XB'):
                        df.loc[index,'gender'] = userinfo['user_info'][u'XB']
                    if userinfo['user_info'].has_key(u'ZYMC'):
                        df.loc[index,'major'] = userinfo['user_info'][u'ZYMC']
                    if userinfo['user_info'].has_key(u'RYLX'):
                        df.loc[index,'RYLX'] = userinfo['user_info'][u'RYLX']
                    if userinfo['user_info'].has_key(u'NJ'):
                        df.loc[index,'year'] = userinfo['user_info'][u'NJ']

                # 无法获取信息
                else:
                    print u'无法获取用户信息'
                    no_userinfo+=1
                    drop_time = ctime + 120 # 由于无法获取用户信息，2分钟后重试   
                    
                    df.loc[index,'add_time'] = u'无'
                    df.loc[index,'drop_time'] = u'无'
                    df.loc[index,'apartment'] = u'无'
                    df.loc[index,'user_encrypt'] = u'无'
                    df.loc[index,'gender'] = u'无'
                    df.loc[index,'major'] = u'无'
                    df.loc[index,'RYLX'] = u'无'
                    df.loc[index,'year'] = u'无'
            except Exception,e:
                print u'错误',e
                error_msg = str(e)+'=>'+str(index)
                log.write(error_msg+'\n')
                errors.append(error_msg)
             
        # ip没变
        else:
            print u'ip没有变化'
            if ctime<=drop_time:
                ip_timeout+=1
                print u'未超过下线时间,使用上一条记录的信息填充#{}'.format(ip_nottimeout)
                skip+=1
            else:
                ip_nottimeout+=1
                print u'超过下线时间,重新获取#{}'.format(ip_timeout)
                time.sleep(0.2)
                try:
                    userinfo = requests.get(
                'https://network-api.ecnu.edu.cn/oauth/userinfo.php', params=payload)\
                    .text.replace("\r\n\r\n","")
                    userinfo = json.loads(userinfo)
                    # 可以获取信息
                    if userinfo.has_key(u'user_info'):
                        # print u'成功获取用户信息'
                        print userinfo
                        df.loc[index,'add_time'] = userinfo[u'add_time']
                        df.loc[index,'drop_time'] = userinfo[u'drop_time']
                        drop_time = ctime+800 if userinfo[u'drop_time']==0 else userinfo[u'drop_time']
                        if userinfo['user_info'].has_key(u'BMMC'):
                            df.loc[index,'apartment'] = userinfo['user_info'][u'BMMC']  
                        if userinfo['user_info'].has_key(u'user_encrypt'):
                            df.loc[index,'user_encrypt'] = userinfo['user_info'][u'user_encrypt']
                        if userinfo['user_info'].has_key(u'XB'):
                            df.loc[index,'gender'] = userinfo['user_info'][u'XB']
                        if userinfo['user_info'].has_key(u'ZYMC'):
                            df.loc[index,'major'] = userinfo['user_info'][u'ZYMC']
                        if userinfo['user_info'].has_key(u'RYLX'):
                            df.loc[index,'RYLX'] = userinfo['user_info'][u'RYLX']
                        if userinfo['user_info'].has_key(u'NJ'):
                            df.loc[index,'year'] = userinfo['user_info'][u'NJ']
                    # 没获取到
                    else:
                        print u'无法获取用户信息'
                        no_userinfo+=1
                        drop_time = ctime + 120 # 由于无法获取用户信息，2分钟后重试
                        
                        df.loc[index,'add_time'] = u'无'
                        df.loc[index,'drop_time'] = u'无'
                        df.loc[index,'apartment'] = u'无'
                        df.loc[index,'user_encrypt'] = u'无'
                        df.loc[index,'gender'] = u'无'
                        df.loc[index,'major'] = u'无'
                        df.loc[index,'RYLX'] = u'无'
                        df.loc[index,'year'] = u'无'
                except Exception,e:
                    print u'错误',e
                    error_msg = str(e)+'=>'+str(index)
                    log.write(error_msg+'\n')
                    errors.append(error_msg)
    print u'不同ip:{},\t超时:{},\t未超时:{},\t跳过:{},\t无法获取用户信息{}'.format(dif_ip,ip_timeout,ip_nottimeout,skip,no_userinfo)
    print u'错误数量{}'.format(len(errors))
    return df, errors



if __name__ == '__main__':

    start_time = time.time()
    filepath = raw_input(u'请输入原始文件夹名称:'.encode('gbk'))
    filepath = filepath+'/'
    # 遍历文件夹下的csv文件
    raw_files = gci(filepath,[],'.csv')

    for f in raw_files:
        log = open(f.split('/')[1]+'log.txt','w')
    
        print u'正在处理：'+f
        raw_df = read_data(f)# 测试
        df = splitUrl(raw_df) # 切割url
        df, errors = getUserInfo(df,log) # 获取用户信息
        log.close()
        # 填充
        print u'正在填充'
        df[['apartment','add_time','drop_time','user_encrypt','gender','major',\
        'RYLX','year']] =  df[['apartment','add_time','drop_time','user_encrypt'\
        ,'gender','major','RYLX','year']].fillna(method='ffill')
        print errors
        # 另存为csv
        # TODO：1.去掉一些没用或者重复的列；2.不要写header和index，方便合并csv和数据库存储
        df.to_csv('data/'+f.split('/')[1],encoding='utf-8_sig',header=None)
        SendEmail(u'用户信息获取完成',u'用户信息获取完成')
    end_time = time.time()
    time_dif = end_time - start_time
    print u'用时：{}'.format(datetime.timedelta(seconds=int(round(time_dif))))
