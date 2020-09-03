import requests
import json
import pandas as pd


# 提取某一期的弹幕
def get_danmu(num1, num2, page):
    try:
        url = 'https://bullet-ws.hitv.com/bullet/2020/09/2/{}/{}/{}.json'
        danmuurl = url.format(num1, num2, page)
        res = requests.get(danmuurl)
        res.encoding = 'utf-8'

    except:
        print("无法连接")
    jd = json.loads(res.text)
    details = []
    for i in range(len(jd['data']['items'])):  # 弹幕数据在json文件'data'的'items'中
        result = {}
        result['stype'] = num2  # 通过stype可识别期数
        result['id'] = jd['data']['items'][i]['id']  # 获取id

        try:  # 尝试获取uname
            result['uname'] = jd['data']['items'][i]['uname']
        except:
            result['uname'] = ''

        result['content'] = jd['data']['items'][i]['content']  # 获取弹幕内容
        result['time'] = jd['data']['items'][i]['time']  # 获取弹幕发布时间

        try:  # 尝试获取弹幕点赞数
            result['v2_up_count'] = jd['data']['items'][i]['v2_up_count']
        except:
            result['v2_up_count'] = ''
        details.append(result)

    return details


# 输入关键信息
def count_danmu():
    danmu_total = []
    num1 = input('第一个数字')
    num2 = input('第二个数字')
    page = int(input('输入总时长'))
    for i in range(page):
        danmu_total.extend(get_danmu(num1, num2, i))

    return danmu_total


def main():
    danmu_end = []

    # 爬前六集，设置循环6次
    for j in range(1):
        danmu_end.extend(count_danmu())
    df = pd.DataFrame(danmu_end)
    print(df.shape)
    print(df['content'])
    df.to_csv(f'第集.csv', index=True)


if __name__ == '__main__':
    main()
