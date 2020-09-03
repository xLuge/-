import pandas as pd
for i in range(1,33):
    filename='乘风破浪姐姐第'+str(i)+'集.csv'
    df=pd.read_csv(filename,encoding='utf-8)')
    df.to_csv(filename,encoding='utf_8_sig')
