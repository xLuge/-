## 腾讯视频弹幕的爬虫思路
# 1. 先获取一页，封装成函数
# 2. 找到timestamp规律构建URL循环翻页，获取一集所有的页数，封装成函数
# 3. 找到target_id和vid的规律，获取12集的弹幕

# 导入所需库
import zlib

import requests
import json
import time
import pandas as pd
import gzip


def get_danmu_one_page(url_dm):
    """
    :param url_dm: 视频弹幕URL地址
    :return: 一页的弹幕数据
    """
    # 添加headers
    # headers = {
    #     'user-agent': 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Mobile Safari/537.36',
    #     'cookie': 'pgv_pvi=2848752640; RK=3D4EwfdWcy; tvfe_boss_uuid=dd98db066dcd3eb7; pgv_pvid=2867623160; ptcz=ab6ed0d117383835ecc375a0d3440c1947ab0bfb19a4c2ce2c1e8db43534f05f; o_cookie=458919267; LW_uid=v1A579x5K2i3D61286p1g352y6; eas_sid=81Q5p9y5j223g6W2c6r2F2Z938; pac_uid=1_458919267; LW_sid=Z1b5Q9p7F0P3k6J3v8s432e7Z3; uid=809609972; pgv_info=ssid=s8714314861; _qpsvr_localtk=0.07951954795008387; pgv_si=s3399187456',
    #     'referer': 'https://v.qq.com/',
    # }

    # 发起请求
    try:
        r = requests.get(url_dm, timeout=3)
        r.encoding = 'utf-8'
    except Exception as e:
        print(e)
        time.sleep(3)
        r = requests.get(url_dm, timeout=3)
        r.encoding = 'utf-8'

    data = json.loads(r.text)['data']['items']
    print(r.text)
    print(data)
    # 获取评论ID
    comment_id = [i.get('id') for i in data]
    # 获取用户名
    oper_name = [i.get('uname') for i in data]
    # 获取会员等级
    vip_degree = [i.get('type') for i in data]
    # 获取评论内容
    content = [i.get('content') for i in data]
    # 获取评论时间点
    time_point = [i.get('time') for i in data]
    # 获取评论点赞
    up_count = [i.get('v2_up_count') for i in data]

    # 存储数据
    df_one = pd.DataFrame({
        'comment_id': comment_id,
        'oper_name': oper_name,
        'vip_degree': vip_degree,
        'content': content,
        'time_point': time_point,
        'up_count': up_count
    })
    return df_one


def get_danmu_all_page(target_id, vid):
    """
    :param target_id: target_id
    :param vid: vid
    :return: 所有页弹幕
    """
    df_all = pd.DataFrame()
    # 记录步数
    step = 1
    for time_stamp in range(0, 3, 1):  # 右侧设置一个足够大的数
        try:  # 异常处理
            # 构建URL
            url_dm = 'https://bullet-ws.hitv.com/bullet/2020/09/2/{}/{}/{}.json'.format(target_id, vid, time_stamp)
            # 调用函数
            df = get_danmu_one_page(url_dm)
            # 终止条件
            if df.shape[0] == 0:
                print('没有数据！')
                break
            else:
                df_all = df_all.append(df, ignore_index=True)
                # 打印进度
                print(f'我正在获取第{step}页的信息')
                step += 1
                # 休眠一秒
                time.sleep(1)
        except Exception as e:
            print(e)
            continue

    return df_all

# 获取target_id和vid，此处手动抓包获取十五集

# 第一集
df_1 = get_danmu_all_page(target_id='005259', vid='8337559')
df_1.insert(0, 'episodes', 1)
# # 第二集
# df_2 = get_danmu_all_page(target_id='001634', vid='8339494/')
# df_2.insert(0, 'episodes', 2)
#
# # 第三集
# df_3 = get_danmu_all_page(target_id='000000', vid='8368098')
# df_3.insert(0, 'episodes', 3)
#
# # 第四集
# df_4 = get_danmu_all_page(target_id='010934', vid='8398205')
# df_4.insert(0, 'episodes', 4)
#
# # 第五集
# df_5 = get_danmu_all_page(target_id='003326', vid='8437644')
# df_5.insert(0, 'episodes', 5)
#
# # 第六集
# df_6 = get_danmu_all_page(target_id='005312', vid='8459286')
# df_6.insert(0, 'episodes', 6)
#
# # 第七集
# df_7 = get_danmu_all_page(target_id='095841', vid='8613018')
# df_7.insert(0, 'episodes', 7)
#
# # 第八集
# df_8 = get_danmu_all_page(target_id='003540', vid='8766480')
# df_8.insert(0, 'episodes', 8)
#
# # 第九集
# df_9 = get_danmu_all_page(target_id='012252', vid='8824382')
# df_9.insert(0, 'episodes', 9)
#
# # 第十集
# df_10 = get_danmu_all_page(target_id='181514', vid='8980904')
# df_10.insert(0, 'episodes', 10)
#
# # 第十一集
# df_11 = get_danmu_all_page(target_id='163002', vid='9156465')
# df_11.insert(0, 'episodes', 11)
#
# # 第十二集
# df_12 = get_danmu_all_page(target_id='000000', vid='9208677')
# df_12.insert(0, 'episodes', 12)
#
# # 第十三集
# df_13 = get_danmu_all_page(target_id='012252', vid='9270174')
# df_13.insert(0, 'episodes', 13)
#
# # 第十四集
# df_14 = get_danmu_all_page(target_id='000000', vid='9317248')
# df_14.insert(0, 'episodes', 14)
#
# # 第十五集
# df_15 = get_danmu_all_page(target_id='001729', vid='9333927')
# df_15.insert(0, 'episodes', 15)
#
# # 第十六集
# df_16 = get_danmu_all_page(target_id='000000', vid='9376993')
# df_16.insert(0, 'episodes', 16)
#
# # 第17集
# df_17 = get_danmu_all_page(target_id='001701', vid='9388811')
# df_17.insert(0, 'episodes', 17)
#
# # 第18集
# df_18 = get_danmu_all_page(target_id='175309', vid='9396497')
# df_18.insert(0, 'episodes', 18)
#
# # 第19集
# df_19 = get_danmu_all_page(target_id='182044', vid='9424295')
# df_19.insert(0, 'episodes', 19)
#
# # 第20集
# df_20 = get_danmu_all_page(target_id='001729', vid='9459751')
# df_20.insert(0, 'episodes', 20)
#
# # 第21集
# df_21 = get_danmu_all_page(target_id='114828', vid='9490434')
# df_21.insert(0, 'episodes', 21)
#
#
# # 第22集
# df_22 = get_danmu_all_page(target_id='174714', vid='9513202')
# df_22.insert(0, 'episodes', 22)
#
# # 第23集
# df_23 = get_danmu_all_page(target_id='174710', vid='9522795')
# df_23.insert(0, 'episodes', 23)
#
# # 第24集
# df_24 = get_danmu_all_page(target_id='001729', vid='9553671')
# df_24.insert(0, 'episodes', 24)
#
# # 第25集
# df_25 = get_danmu_all_page(target_id='003540', vid='9573392')
# df_25.insert(0, 'episodes', 25)
#
# # 第26集
# df_26 = get_danmu_all_page(target_id='010819', vid='9581915')
# df_26.insert(0, 'episodes', 26)
#
# # 第27集
# df_27 = get_danmu_all_page(target_id='174507', vid='9610731')
# df_27.insert(0, 'episodes', 27)
#
# # 第28集
# df_28 = get_danmu_all_page(target_id='003326', vid='9632748')
# df_28.insert(0, 'episodes', 28)
#
# # 第29集
# df_29 = get_danmu_all_page(target_id='174205', vid='9641253')
# df_29.insert(0, 'episodes', 29)
#
# # 第30集
# df_30 = get_danmu_all_page(target_id='060045', vid='9668010')
# df_30.insert(0, 'episodes', 30)
#
# # 第31集
# df_31 = get_danmu_all_page(target_id='001742', vid='9694217')
# df_31.insert(0, 'episodes', 31)
#
# # 第32集
# df_32 = get_danmu_all_page(target_id='162444', vid='9711953')
# df_32.insert(0, 'episodes', 32)


# 列表存储
# df_list = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15,
#            df_16, df_17, df_18, df_19, df_20, df_21, df_22, df_23, df_24, df_25, df_26, df_27, df_28, df_29, df_30,
#            df_31, df_32]






# 循环写出数据
df_list=[df_1]
for df_name in df_list:
    # 读出数据
    print(df_name.shape)
    epi_num =df_name['episodes'][0]
    print(f'正在写出第{epi_num}集的数据')
    df=df_name['content']
    print(df_name['content'].shape)
    df.to_csv(f'第11集.csv', index=True)



