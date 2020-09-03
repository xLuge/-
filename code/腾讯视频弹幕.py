## 腾讯视频弹幕的爬虫思路
# 1. 先获取一页，封装成函数
# 2. 找到timestamp规律构建URL循环翻页，获取一集所有的页数，封装成函数
# 3. 找到target_id和vid的规律，获取12集的弹幕

# 导入所需库
import requests
import json
import time
import pandas as pd


def get_danmu_one_page(url_dm):
    """
    :param url_dm: 视频弹幕URL地址
    :return: 一页的弹幕数据
    """
    # 添加headers
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
        'cookie': 'pgv_pvi=2848752640; RK=3D4EwfdWcy; tvfe_boss_uuid=dd98db066dcd3eb7; pgv_pvid=2867623160; ptcz=ab6ed0d117383835ecc375a0d3440c1947ab0bfb19a4c2ce2c1e8db43534f05f; o_cookie=458919267; LW_uid=v1A579x5K2i3D61286p1g352y6; eas_sid=81Q5p9y5j223g6W2c6r2F2Z938; pac_uid=1_458919267; LW_sid=Z1b5Q9p7F0P3k6J3v8s432e7Z3; pgv_info=ssid=s48814892; pgv_si=s6449019904; _qpsvr_localtk=0.7405608064237008; uid=809609972',
        'referer': 'https://v.qq.com/x/cover/mzc00200iseomew/b0033m9le2c.html',
    }

    # 发起请求
    try:
        r = requests.get(url_dm, headers=headers, timeout=3)
    except Exception as e:
        print(e)
        time.sleep(3)
        r = requests.get(url_dm, headers=headers, timeout=3)

    # 解析网页
    print(r.encoding)
    data = json.loads(r.text, strict=False)['comments']

    # 获取评论ID
    comment_id = [i.get('commentid') for i in data]
    # 获取用户名
    oper_name = [i.get('opername') for i in data]
    # 获取会员等级
    vip_degree = [i.get('uservip_degree') for i in data]
    # 获取评论内容
    content = [i.get('content') for i in data]
    # 获取评论时间点
    time_point = [i.get('timepoint') for i in data]
    # 获取评论点赞
    up_count = [i.get('upcount') for i in data]

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
    for time_stamp in range(15, 100000, 30):  # 右侧设置一个足够大的数
        try:  # 异常处理
            # 构建URL
            url_dm = 'https://mfm.video.qq.com/danmu?target_id={}&vid={}&timestamp={}'.format(target_id, vid, time_stamp)
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
df_1 = get_danmu_all_page(target_id='5574580425', vid='t0034201b7o')
df_1.insert(0, 'episodes', 1)

# 第二集
df_2 = get_danmu_all_page(target_id='5574581204', vid='r0034vwboan')
df_2.insert(0, 'episodes', 2)

# 第三集
df_3 = get_danmu_all_page(target_id='5574582455', vid='b0034rjba0c')
df_3.insert(0, 'episodes', 3)

# 第四集
df_4 = get_danmu_all_page(target_id='5574591213', vid='b0034h1lrxv')
df_4.insert(0, 'episodes', 4)

# 第五集
df_5 = get_danmu_all_page(target_id='5582060073', vid='w0034gahxje')
df_5.insert(0, 'episodes', 5)

# 第六集
df_6 = get_danmu_all_page(target_id='5582178005', vid='r0034cc2o6r')
df_6.insert(0, 'episodes', 6)

# 第七集
df_7 = get_danmu_all_page(target_id='5582177015', vid='v0034d2icgu')
df_7.insert(0, 'episodes', 7)

# 第八集
df_8 = get_danmu_all_page(target_id='5591083461', vid='d0034enpuju')
df_8.insert(0, 'episodes', 8)

# 第九集
df_9 = get_danmu_all_page(target_id='5591084085', vid='o003435e1af')
df_9.insert(0, 'episodes', 9)

# 第十集
df_10 = get_danmu_all_page(target_id='5594688004', vid='w0034sq9v7f')
df_10.insert(0, 'episodes', 10)

# 第十一集
df_11 = get_danmu_all_page(target_id='5594688920', vid='b0034w1ur3t')
df_11.insert(0, 'episodes', 11)

# 第十二集
df_12 = get_danmu_all_page(target_id='5601621634', vid='e0034vrbrih')
df_12.insert(0, 'episodes', 12)

# 第十三集
df_13 = get_danmu_all_page(target_id='5601620828', vid='n0034r3d2bu')
df_13.insert(0, 'episodes', 13)

# 第十四集
df_14 = get_danmu_all_page(target_id='5606418832', vid='o0034dqnewj')
df_14.insert(0, 'episodes', 14)

# 第十五集
df_15 = get_danmu_all_page(target_id='5606420667', vid='r0034m1qwtq')
df_15.insert(0, 'episodes', 15)


# 列表存储
df_list = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15]

# 循环写出数据
for df_name in df_list:
    # 读出数据
    epi_num =df_name['episodes'][0]
    print(f'正在写出第{epi_num}集的数据')
    df_name.to_csv(f'./data/第{epi_num}集.csv', index=True)



