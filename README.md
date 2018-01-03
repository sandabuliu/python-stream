#  python-stream

### 说明
数据流式框架, 可用作数据清洗, 数据预处理, 数据迁移等应用场景

更优雅的流式数据处理方式

### 安装
-----------
```shell
pip install git+https://github.com/sandabuliu/python-stream.git
```
or

```shell
git clone https://github.com/sandabuliu/python-stream.git
cd python-agent
python setup.py install
```


### QuickStart
---------------
#### Examples

##### Word Count

```python
from pystream.executor.source import Memory
from pystream.executor.executor import Map, Iterator, ReducebyKey
    
data = Memory([
    'Wikipedia is a free online encyclopedia, created and edited by volunteers around the world and hosted by the Wikimedia Foundation.',
    'Search thousands of wikis, start a free wiki, compare wiki software.',
    'The official Wikipedia Android app is designed to help you find, discover, and explore knowledge on Wikipedia.'
])
p = data | Map(lambda x: x.split(' ')) | Iterator(lambda x: (x.strip('.,'), 1)) | ReducebyKey(lambda x, y: x+y)
result = {}
for key, value in p:
    result[key] = value
print result.items()
```

执行结果

```python
[('and', 3), ('wiki', 2), ('compare', 1), ('help', 1), ('is', 2), ('Wikipedia', 3), ('discover', 1), ('hosted', 1), ('Android', 1), ('find', 1), ('Foundation', 1), ('knowledge', 1), ('to', 1), ('by', 2), ('start', 1), ('online', 1), ('you', 1), ('thousands', 1), ('app', 1), ('edited', 1), ('Search', 1), ('around', 1), ('free', 2), ('explore', 1), ('designed', 1), ('world', 1), ('The', 1), ('the', 2), ('a', 2), ('on', 1), ('created', 1), ('Wikimedia', 1), ('official', 1), ('encyclopedia', 1), ('of', 1), ('wikis', 1), ('volunteers', 1), ('software', 1)]
```

##### 计算π

```python
from random import random
from pystream.executor.source import Faker
from pystream.executor.executor import Executor, Map, Group

class Pi(Executor):
    def __init__(self, **kwargs):
        super(Pi, self).__init__(**kwargs)
        self.counter = 0
        self.result = 0

    def handle(self, item):
        self.counter += 1
        self.result += item
        return 4.0*self.result/self.counter

s = Faker(lambda: random(), 100000) | Map(lambda x: x*2-1) | Group(size=2) | Map(lambda x: 1 if x[0]**2+x[1]**2 <= 1 else 0) | Pi()

res = None
for _ in s:
    res = _
print res
```

执行结果

```python
3.14728
```

##### 排序

```python
from random import randint
from pystream.executor.source import Memory
from pystream.executor.executor import Sort
m = Memory([randint(0, 100) for i in range(10)]) | Sort()

for i in m:
    print list(i)
```

执行结果

```python
[94]
[94, 99]
[18, 94, 99]
[18, 40, 94, 99]
[18, 26, 40, 94, 99]
[18, 26, 40, 63, 94, 99]
[18, 26, 40, 63, 83, 94, 99]
[3, 18, 26, 40, 63, 83, 94, 99]
[3, 18, 26, 40, 63, 83, 83, 94, 99]
[3, 16, 18, 26, 40, 63, 83, 83, 94, 99]
```

##### 解析 NGINX 日志
```python
from pystream.config import rule
from pystream.executor.source import File
from pystream.executor.executor import Parser
s = File('/var/log/nginx/access.log') | Parser(rule('nginx'))

for item in s:
    print item

```

执行结果

```python
{'status': '400', 'body_bytes_sent': 173, 'remote_user': '-', 'http_referer': '-', 'remote_addr': '198.35.46.20', 'request': '\\x05\\x01\\x00', 'version': None, 'http_user_agent': '-', 'time_local': datetime.datetime(2017, 2, 15, 13, 11, 3), 'path': None, 'method': None}
{'status': '400', 'body_bytes_sent': 173, 'remote_user': '-', 'http_referer': '-', 'remote_addr': '198.35.46.20', 'request': '\\x05\\x01\\x00', 'version': None, 'http_user_agent': '-', 'time_local': datetime.datetime(2017, 2, 15, 13, 11, 3), 'path': None, 'method': None}
{'status': '400', 'body_bytes_sent': 173, 'remote_user': '-', 'http_referer': '-', 'remote_addr': '198.35.46.20', 'request': '\\x05\\x01\\x00', 'version': None, 'http_user_agent': '-', 'time_local': datetime.datetime(2017, 2, 15, 13, 11, 3), 'path': None, 'method': None}
{'status': '400', 'body_bytes_sent': 173, 'remote_user': '-', 'http_referer': '-', 'remote_addr': '198.35.46.20', 'request': '\\x05\\x01\\x00', 'version': None, 'http_user_agent': '-', 'time_local': datetime.datetime(2017, 2, 15, 13, 11, 3), 'path': None, 'method': None}
{'status': '400', 'body_bytes_sent': 173, 'remote_user': '-', 'http_referer': '-', 'remote_addr': '198.35.46.20', 'request': '\\x05\\x01\\x00', 'version': None, 'http_user_agent': '-', 'time_local': datetime.datetime(2017, 2, 15, 13, 11, 3), 'path': None, 'method': None}
```

##### 导出数据库数据

```python
from sqlalchemy import create_engine
from pystream.executor.source import SQL
from pystream.executor.output import Csv
from pystream.executor.wraps import Batch

engine = create_engine('mysql://root:123456@127.0.0.1:3306/test')  
conn = engine.connect()
s = SQL(conn, 'select * from faker') | Batch(Csv('/tmp/output'))

for item in s:
    print item['data']
    print item['exception']
conn.close()
```

#### 数据源
##### 读取文件数据

```python
Tail('/var/log/nginx/access.log')
File('/var/log/nginx/*.log')
Csv('/tmp/test*.csv')
```

##### 读取 TCP 流数据

```python
TCPClient('/tmp/pystream.sock')
TCPClient(('127.0.0.1', 10000))
```

##### 读取 python 数据

```python
from random import randint
Memory([1, 2, 3, 4])
Faker(randint, 1000)
Queue(queue)
```

##### 读取常用模块数据

```python
SQL(conn, 'select * from faker')   # 读取数据库数据
Kafka('topic1', '127.0.0.1:9092')  # 读取 kafka 数据
```

#### 数据输出
##### 输出到文件

```python
File('/tmp/output')
Csv('/tmp/output.csv')
```

##### 通过HTTP输出

```python
HTTPRequest('http://127.0.0.1/api/data')
```

##### 输出到kafka

```python
Kafka('topic', '127.0.0.1:9092')
```

### TodoList

* 订阅器(Subscribe)客户端超时处理
* 并行计算
* HTTP 异步输出/异步源
* 添加其他基础输出/基础源
* 添加对其他常用模块的支持, 如 redis, kafka, flume, log-stash, 各种数据库等

Copyright © 2017 [g_tongbin@foxmail.com](mailto:g_tongbin@foxmail.com)