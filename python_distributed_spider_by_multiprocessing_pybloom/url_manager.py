# -*- coding: utf-8 -*-
# 管理url队列。对 url_set.py 提交的url去重，为 url_get.py 提供待爬取的url队列

import queue
from multiprocessing.managers import BaseManager
from pybloom import ScalableBloomFilter
import time

import settings
import run_spider  # 动态调整 url_get.py 数量  # 有大量未爬取url时，多开 url_get.py


# 待爬取url
new_url = queue.Queue()
# 所有url
all_url = queue.Queue()
# url 去重结果  url_set.py 放入爬取队列 new_url 中的url是否成功  成功放入或者因url重复而不放入
url_set_result = queue.Queue()
# url 爬取结果  url_get.py 爬取成功与否的信息存到该队列中  爬取成功或爬取失败+失败原因+是否重新添加回new_url（是否从新爬取）
url_get_result = queue.Queue()


def return_new_url():
    global new_url
    return new_url


def return_all_url():
    global all_url
    return all_url


# 从BaseManager继承的QueueManager:
class QueueManager(BaseManager):
    pass


if __name__ == '__main__':
    sbf = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)

    num_all_url = 0  # 总共处理的url
    num_url_deduplicated = 0  # 去重后的url

    # 把两个Queue都注册到网络上, callable参数关联了Queue对象:
    QueueManager.register('get_new_url', callable=return_new_url)
    QueueManager.register('get_all_url', callable=return_all_url)
    # 绑定端口5000, 设置验证码:
    manager = QueueManager(address=(settings.manager_ip, settings.manager_port), authkey=settings.manager_authkey)  # 本机在局域网中的ip地址
    # 启动Queue:
    manager.start()
    print('url_manager start')
    # 获得通过网络访问的Queue对象:
    all_url = manager.get_all_url()
    new_url = manager.get_new_url()

    # if not all_url.empty():
    while True:
        try:
            url = all_url.get(timeout=1)
            num_all_url += 1
            add_result = sbf.add(url)  # 成功添加返回False
            if not add_result:
                new_url.put(url)
                num_url_deduplicated += 1
            print('================')
            print('等待爬取的url:', new_url.qsize())
            print('等待manager去重的url:', all_url.qsize())
            print('总共处理的url:', num_all_url)
            print('去重后的url:', num_url_deduplicated)
            print('已添加到爬取队列的url:', len(sbf))
            print(new_url.qsize())

        except:
            print('================')
            print('等待爬取的url:', new_url.qsize())
            print('等待manager去重的url:', all_url.qsize())
            print('总共处理的url:', num_all_url)
            print('去重后的url:', num_url_deduplicated)
            print('已添加到爬取队列的url:', len(sbf))
            time.sleep(2)

    # # 关闭:
    # manager.shutdown()
    # print('url_manager exit.')
