from hdfs import InsecureClient
import json

webhdfs_url = "http://127.0.0.1:50070"
username = "root"
path = "/"

client = InsecureClient(url=webhdfs_url, root="/", user=username)
x = client.list(hdfs_path=path, status=False)

print("a")

for y in x:
  print(str(y))

print("b")

fpath="/rsm/storage/SharingPrivato/DatiOnline/2021/05/20/23SharingPrivato20210520231947532429.json"
data = {}
overwrite = False
append = False

if isinstance(data, str):
    data=data.encode('utf-8')
elif isinstance(data, dict):
    data = json.dumps(data)

client.write(hdfs_path=fpath, data=data, overwrite=overwrite, append=append)
