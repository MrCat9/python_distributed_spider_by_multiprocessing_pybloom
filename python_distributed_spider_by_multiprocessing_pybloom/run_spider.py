# -*- coding: utf-8 -*-
# 启动分布式爬虫

import os
import multiprocessing


def gen_manager_command():
    return 'python url_manager.py'


def gen_set_command(from_page, end_page):
    return 'python url_set.py {} {}'.format(str(from_page), str(end_page))


def gen_get_command():
    return 'python url_get.py'


def run_cmd(command):
    return os.system(command)


def multi_process_task(command):
    try:
        cmd_result = run_cmd(command)
    except Exception as e:
        print(str(e))
        print('================')


if __name__ == '__main__':
    # command_list = ['python url_manager.py',
    #                 'python url_set.py', 'python url_set.py', 'python url_set.py',
    #                 'python url_get.py', 'python url_get.py', 'python url_get.py',
    #                 ]

    command_list = []

    # url_set.py
    begin_page = 1
    page_step = 10
    total_page = 111
    # url_get.py
    url_get_num = 5

    command_list.append(gen_manager_command())

    from_page = begin_page - 1
    while from_page < total_page:
        from_page = from_page + 1
        end_page = from_page + page_step - 1
        if end_page > total_page:
            end_page = total_page
        command_list.append(gen_set_command(from_page, end_page))
        from_page = end_page

    for i in range(url_get_num):
        command_list.append((gen_get_command()))

    i = 1
    pool = multiprocessing.Pool(processes=40)  # 4个进程
    # 迭代出每一个cmd命令
    for command in command_list:
        print(i, command)
        pool.apply_async(multi_process_task, (command, ))
        i += 1
    pool.close()  # 调用join()之前必须先调用close()，调用close()之后就不能继续添加新的Process了
    pool.join()
