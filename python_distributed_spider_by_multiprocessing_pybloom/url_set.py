# -*- coding: utf-8 -*-
# 从网页上解析出url提交给 url_manager.py

from multiprocessing.managers import BaseManager
import sys  # cmd命令向python程序中传递参数  # 传递 from_page 和 end_page
import time

import settings
import parse_url


# 创建类似的QueueManager:
class QueueManager(BaseManager):
    pass


# 由于这个QueueManager只从网络上获取Queue，所以注册时只提供名字:
QueueManager.register('get_all_url')

# 连接到服务器，也就是运行 url_manager.py 的机器:
print('Connect to server %s...' % settings.manager_ip)
# 端口和验证码注意保持与 url_manager.py 设置的完全一致:
m = QueueManager(address=(settings.manager_ip, settings.manager_port), authkey=settings.manager_authkey)

for i in range(10):
    try:
        # 从网络连接:
        m.connect()
        break
    except Exception as e:
        print(str(e))
        time.sleep(2)

# 获取Queue的对象:
all_url = m.get_all_url()

# 接收cmd命令传递过来的参数  from_page 和 end_page
from_page = sys.argv[1]
end_page = sys.argv[2]
# from_page = 1
# end_page = 11

# 把 url 放到 url_manager.py 下的 all_url 中
for page in range(int(from_page), int(end_page)+1):
    # 从页面提取url逻辑 开始
    page_url = 'https://www.testurl.com/?page={}'.format(page)
    parse_url.parse(page_url, all_url)  # 解析出页面上的url，并提交给 url_manager.py 中的 all_url
    # 从页面提取url逻辑 结束

# 处理结束:
print('================')
print('url_set exit.')
print(page_url)
print('from_page = {}'.format(from_page))
print('end_page = {}'.format(end_page))
