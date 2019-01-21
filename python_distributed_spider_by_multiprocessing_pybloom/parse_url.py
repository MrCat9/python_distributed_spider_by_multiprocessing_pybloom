# -*- coding: utf-8 -*-
# 从page_url中解析出url

import time


def parse(page_url, all_url):

    # 解析逻辑 开始
    for i in range(10):
        url = page_url + '&no={}'.format(i)  # 解析出的url
        # print('从 {} 中解析出 {}'.format(page_url, url))
        all_url.put(url)  # 将解析出的url提交给 url_manager.py
        print('url_set set url:', url)

    # 解析逻辑 结束

    time.sleep(2)
