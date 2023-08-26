import datetime
ds = '2023-02-14'
# parse date from string
dst = datetime.datetime.strptime(ds, '%Y-%m-%d')
print(dst.weekday())