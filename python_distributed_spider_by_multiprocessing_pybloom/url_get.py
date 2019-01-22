# -*- coding: utf-8 -*-
# 从 url_manager.py 获取待爬取的url

from multiprocessing.managers import BaseManager
import os
import psutil  # 内存管理
import multiprocessing
import time

import settings
import parse_data
import save_to_db


def run_url_get_py():
    if psutil.virtual_memory().percent < settings.MAX_MEMORY_USE:
        os.system('python url_get.py')


py_id = str(os.getpid())  # str  # 该进程的id


# 创建类似的QueueManager:
class QueueManager(BaseManager):
    pass


# 由于这个QueueManager只从网络上获取Queue，所以注册时只提供名字:
QueueManager.register('get_new_url')
QueueManager.register('get_url_get_py_id')

# 连接到服务器，也就是运行url_manager.py的机器:
print('Connect to server %s...' % settings.manager_ip)
# 端口和验证码注意保持与url_manager.py设置的完全一致:
m = QueueManager(address=(settings.manager_ip, settings.manager_port), authkey=settings.manager_authkey)

for i in range(10):
    try:
        # 从网络连接:
        m.connect()
        break
    except Exception as e:
        print(str(e))

# 获取Queue的对象:
new_url = m.get_new_url()
url_get_py_id = m.get_url_get_py_id()

# 将该py的进程号放到 url_manager.py 下的 url_get_py_id 中
url_get_py_id.put(py_id)

# 从 url_manager.py 下的 new_url 队列取 url
stop_flag = 0  # 多次没取到待爬取的url，就停掉该 url_get.py
while True:
    try:
        url = new_url.get(timeout=1)  # 从待爬取的url队列中取出一个url

        if not new_url.empty() and psutil.virtual_memory().percent < settings.MAX_MEMORY_USE:
            p = multiprocessing.Process(target=run_url_get_py)
            p.start()  # 用start()方法启动

        try:
            data = parse_data.parse(url)  # 爬取逻辑
            save_to_db.save(data)
            print('爬取成功', url)
        except:
            print('!!!!爬取失败', url)
            new_url.put(url)

    except:
        print('{} 没有待爬取的url了'.format(str(py_id)))
        stop_flag += 1
        time.sleep(5)

    finally:
        if stop_flag >= 10:
            break

# 处理结束:
url_get_py_id.get()  # 退出时 url_manager.py 下的 url_get_py_id - 1
print('url_get exit.')

