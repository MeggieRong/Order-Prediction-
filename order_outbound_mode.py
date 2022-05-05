# 读取mysql数据
import sys
# import requests
# import json
import numpy as np
import pandas as pd
import random
import requests
import json

"""
step1: 读取数据
step2: 数据预处理
step3: 料箱匹配
step4: 计算订单相似性
step5：计算时长

"""


class OrderOutbound:

    def __init__(self, path1, path2, path3, path4):
        """

        :param path1: 订单信息路径
        :param path2: 订单详细信息
        :param path3: 库存结构信息
        :param path4: 读取客户输入信息
        """
        self.path_order = path1  # 订单信息路径
        self.path_order_detail = path2  # 订单详细信息
        self.path_stock_struct = path3  # 库存结构信息
        self.path_customer_input = path4  # 客户输入信息

        # 订单参数初始化
        self.order_no = None
        self.order_create_time = None
        self.order_detail = None
        self.order_status = None
        self.order_number = None

        # 订单行参数初始化
        self.detail_id = None
        self.qty_required = None
        self.sku_code = None
        self.order_type = None

        # 库存结构参数初始化
        self.container_id = None
        self.container_sku_code = None
        self.container_qty_actual = None

        # 硬件基础参数
        self.robot_number = None
        self.picking_station_number = None
        self.picking_shelf_number = None
        self.picking_grid_number = None
        self.picking_box_volume = None

        # 时效基础信息
        self.moving_box_time = None
        self.switching_box_time = None
        self.picking_time = None
        self.sorting_time = None
        self.packing_time = None

        # 效率基础信息
        self.moving_box_eta = None
        self.switching_box_eta = None
        self.picking_eta = None
        self.sorting_eta = None
        self.packing_eta = None

        # 返回结果
        self.container_sku_qty = None  # 库存结构统计
        self.single_station_grids = None  # 单操作台格口数量
        self.wave_number = None  # 波次
        self.order_list = None

    def read_file(self):

        read_outbound_order = pd.read_csv(self.path_order, low_memory=False)
        read_order_detail = pd.read_csv(
            self.path_order_detail, low_memory=False)
        read_stock_struct = pd.read_csv(
            self.path_stock_struct, low_memory=False)
        read_aging_info = pd.read_excel(
            self.path_customer_input, sheet_name='时效基础数据')
        read_hardware_info = pd.read_excel(
            self.path_customer_input, sheet_name='硬件基础数据')
        read_efficiency_info = pd.read_excel(
            self.path_customer_input, sheet_name='效率基础数据')

        # 读取未拣选订单
        self.order_no = read_outbound_order['customer_order_no']  # 读取订单编号
        self.order_create_time = read_outbound_order['create_time']  # 读取创建时间
        # 读取订单行
        self.order_detail = read_outbound_order['ks_outbound_order_detail_ids']
        # 读取出库订单状态
        self.order_status = read_outbound_order['outbound_order_status']
        self.order_number = len(self.order_no)
        # 制作未拣选订单（上线需删除）

        # 读取未拣选订单的订单行
        self.detail_id = read_order_detail['id']  # 读取订单header_id
        self.qty_required = read_order_detail['qty_required']  # 读取sku量
        self.sku_code = read_order_detail['sku_code']  # 读取sku编码
        self.order_type = read_order_detail['reserved_field1']  # 读取类型

        # 读取库存结构
        # 读取container_code
        self.container_id = read_stock_struct['container_code']
        # 读取料箱的sku_code
        self.container_sku_code = read_stock_struct['sku_code']
        self.container_qty_actual = read_stock_struct['qty_actual']  # 读取qty

        # 读取硬件基础信息
        self.robot_number = int(read_hardware_info['可用机器人最大数量'])
        self.picking_station_number = int(read_hardware_info['可用拣货工作站数量'])
        self.picking_shelf_number = int(read_hardware_info['可用拣选货架数量'])
        self.picking_grid_number = int(read_hardware_info['可用拣货格口数量'])
        self.picking_box_volume = int(read_hardware_info['可用拣货分播箱容纳最大数量'])

        # 读取时效基础信息
        self.moving_box_time = float(read_aging_info['单个搬箱平均时长'])
        self.switching_box_time = float(read_aging_info['箱子之间切换时间'])
        self.picking_time = float(read_aging_info['拣货员的拣货时长'])
        self.sorting_time = float(read_aging_info['二分员的二分时长'])
        self.packing_time = float(read_aging_info['打包员的打包时长'])

        # 读取效率基础信息
        self.moving_box_eta = float(read_efficiency_info['机器人搬箱效率'])
        self.switching_box_eta = float(read_efficiency_info['箱子之间切换效率'])
        self.picking_eta = float(read_efficiency_info['拣单效率'])
        self.sorting_eta = float(read_efficiency_info['二分效率'])
        self.packing_eta = float(read_efficiency_info['打包效率'])
        # 读取完毕

    # 订单预处理函数(正式上线可能会删除)
    def order_preprocessing(self):

        time_cutoff = 120  # 截选时间
        self.order_status[-time_cutoff:] = 'UNPICKED'  # 这里假设最后一百个订单未拣选
        self.order_status = list(
            self.order_status[-time_cutoff:])  # 只选取最后T_cut/一百个未拣选订单
        self.order_no = list(self.order_no[-time_cutoff:])  #
        self.order_create_time = list(self.order_create_time[-time_cutoff:])  #
        self.order_detail = list(self.order_detail[-time_cutoff:])
        self.order_number = len(self.order_no)

    def inventory_preprocessing(self):
        cutoff = 41269  # 截选
        self.container_id = self.container_id[:cutoff]  # 只选取最后T_cut/一百个未拣选订单
        # 只选取最后一百个未拣选订单
        self.container_sku_code = self.container_sku_code[:cutoff]
        # 只选取最后一百个未拣选订单
        self.container_qty_actual = self.container_qty_actual[:cutoff]

        # 打乱库存，目的为了混箱
        for i_d, code in enumerate(self.container_sku_code):
            rand_number = random.randint(int(2 * (len(self.container_sku_code) - 1) / 3),
                                         len(self.container_sku_code) - 1)
            # 把所有的sku_code装在后面箱子
            self.container_id[i_d] = self.container_id[rand_number]

    def inventory_statistics(self):
        self.container_sku_qty = dict()
        for i_d, code in enumerate(self.container_id):  # 对料箱id排序
            sku = dict()
            sku[self.container_sku_code[i_d]] = int(
                self.container_qty_actual[i_d])  # 确定料箱内的sku的code和数量
            if code in self.container_sku_qty:
                self.container_sku_qty[code] = dict(
                    self.container_sku_qty[code], **sku)  # 对相同装箱子的SKU合并
            else:
                self.container_sku_qty[code] = sku

    def wave_result(self):

        self.single_station_grids = self.picking_shelf_number * self.picking_grid_number
        self.wave_number = self.order_number / self.single_station_grids
        # 单个操作台需要处理的次数
        self.wave_number = int(self.wave_number) + 1 if (self.wave_number - int(self.wave_number)) > 0 else int(
            self.wave_number)
        self.order_list = list(range(self.order_number))  # 订单数目条目
        random.shuffle(self.order_list)  # 随机生成波次的订单数目

    def work_time(self):

        # 初始化
        # 搬箱数目
        moving_box_number = np.zeros(self.wave_number)
        # 拣单时间
        picking_order_times = np.zeros(self.order_number)  # 订单对应需要的拣单时间
        # 打包箱子数目
        packing_box = self.order_number

        # 订单相似性统计
        order_similarity = {}

        # 每个订单库存匹配统计
        order_inventory_matching = np.zeros(self.order_number)
        # 开始计算每个订单的时长
        for times in range(self.wave_number):  # 按每次操作台最大格口进行计算
            # 统计此次批次的订单
            combinations_sku_qty = {}  # 本次内所有的SKU和量

            for index in self.order_list[times * self.single_station_grids:(times + 1) * self.single_station_grids]:
                order_details = list(eval(self.order_detail[index]))  # 获取订单行
                sku_code_all = list()

                for o_l in order_details:  # 每个订单的行
                    # 寻找 SKU_code 和件数
                    index_detail = list(self.detail_id).index(o_l)  # 对应索引
                    sku_code = self.sku_code[index_detail]  # 查找订单行的sku of code
                    sku_qty = self.qty_required[index_detail]  # 查找订单行的sku的量
                    if not sku_code_all:
                        sku_code_all = [sku_code]
                    else:
                        sku_code_all.append(sku_code)
                    if sku_code in order_similarity:

                        order_similarity[sku_code] += sku_qty  # 未拣订单sku统计
                    else:
                        order_similarity[sku_code] = sku_qty

                    if sku_code in combinations_sku_qty:

                        # 单操作台单批次的SKU统计
                        combinations_sku_qty[sku_code] += sku_qty
                    else:
                        combinations_sku_qty[sku_code] = sku_qty

                    if sku_qty == 1:

                        # 拣货效率计算 (若只有一件效率为1)
                        eta_picking_order = self.picking_eta
                        picking_order_times[index] = sku_qty * \
                            self.picking_time * eta_picking_order

                    elif 1 < sku_qty <= 5:

                        eta_picking_order = self.picking_eta * \
                            (1 - sku_qty * 0.1)  #
                        picking_order_times[index] = sku_qty * \
                            self.picking_time * eta_picking_order

                    else:
                        eta_picking_order = self.picking_eta * 0.4
                        picking_order_times[index] = sku_qty * \
                            self.picking_time * eta_picking_order

                    # 订单匹配
                matching_sku_order = np.zeros(
                    len(self.container_sku_qty.keys()))
                order_sku_code = set(sku_code_all)
                for i_d, code in enumerate(self.container_sku_qty.keys()):  # 穷举每个料箱的值
                    matching_sku_order[i_d] = len(
                        set(self.container_sku_qty[code]) & order_sku_code)

                # 选取最大库存匹配值
                order_inventory_matching[index] = max(matching_sku_order)
            # 根据本操作台货架的订单结构匹配料箱，从而确定料箱数量
            if len(combinations_sku_qty) == 1:
                moving_box_number[times] = 1  # 若订单为单行，则对应需要料箱为1
            else:

                sku_kind = set(combinations_sku_qty.keys())  # 如果大于1进行料箱匹配
                matching_sku = np.zeros(len(self.container_sku_qty.keys()))
                for i_d, code in enumerate(self.container_sku_qty.keys()):  # 穷举每个料箱的值
                    matching_sku[i_d] = len(
                        set(self.container_sku_qty[code]) & sku_kind)
                if max(matching_sku) == 1:  # 订单SKU每行只由一个料箱简单匹配

                    moving_box_number[times] = len(
                        combinations_sku_qty)  # 若无匹配料箱，则对应SKU数为需要料箱数

                elif max(matching_sku) == 0:  # 没有料箱匹配订单
                    continue
                else:
                    picking_box_sku = sorted(
                        matching_sku, reverse=True)  # 订单的相关性按照大到小排序
                    for ik in picking_box_sku:

                        if ik > 1:
                            moving_box_number[times] = len(
                                combinations_sku_qty) - ik + 1  # 则对应SKU数为需要料箱数
                        else:
                            break
        # 订单相似性
        order_similarity_sort = sorted(
            order_similarity.items(), key=lambda item: item[1], reverse=True)

        # 各工作时长
        order_time_moving_box = self.moving_box_time * \
            sum(moving_box_number) * self.moving_box_eta  # 搬箱时长
        order_time_switching_box = self.switching_box_time * \
            (sum(moving_box_number) - 1) * self.switching_box_eta
        order_time_picking = sum(picking_order_times)  # 拣单时间
        order_time_packing = packing_box * self.packing_time  # by order 打包时长

        order_result = [order_time_moving_box, order_time_switching_box,
                        order_time_picking, order_time_packing]

        return order_similarity_sort, order_inventory_matching, order_result

    def calculate_outbound_time(self):

        # 文件读取函数
        self.read_file()

        # 订单处理函数(正式上线可能会删除)
        self.order_preprocessing()

        # 库存处理函数(正式上线可能会删除)
        self.inventory_preprocessing()

        # 库存统计函数
        self.inventory_statistics()

        # 波次计算函数
        self.wave_result()

        order_similarity_sort, order_inventory_matching, order_result = self.work_time()
        print(order_result, sum(order_result))

        return order_similarity_sort, order_inventory_matching, order_result


if __name__ == "__main__":

    # path_order = sys.argv[1]
    # path_order_detail = sys.argv[2]
    # path_data_stock = sys.argv[3]
    # path_customer_input = sys.argv[4]
    # path_order_outbound_time_result = sys.argv[5]
    # s_d = sys.argv[6]  # 任务sd
    # 输入地址
    path_order = '出库方式预测/测试1/data_outbound_order.csv'
    path_order_detail = '出库方式预测/测试1/data_outbound_order_detail.csv'
    path_data_stock = '出库方式预测/测试1/data_stock.csv'
    path_customer_input = '出库方式预测/测试1/出库模式预测导入模版.xlsx'

    # 输出地址
    path_order_outbound_time_result = '出库方式预测/测试1/输出/order_result.xlsx'

    Post_url = "http://172.18.34.103:10001/outboundforecast/notice"  # 自己想办法弄到key
    se = requests.session()
    try:
        my_order_outbound = OrderOutbound(
            path_order, path_order_detail, path_data_stock, path_customer_input)
        my_order_similarity_sort, my_order_inventory_matching, my_order_result = my_order_outbound.calculate_outbound_time()
        df_result = pd.DataFrame(my_order_result)
        df_result1 = pd.DataFrame(my_order_similarity_sort)
        df_result2 = pd.DataFrame(my_order_inventory_matching)

        writer = pd.ExcelWriter(path_order_outbound_time_result)
        df_result.to_excel(writer, sheet_name='order_time_res')
        df_result1.to_excel(writer, sheet_name='order_similarity_result')
        df_result2.to_excel(writer, sheet_name='order_inventory_matching')
        writer.save()
        writer.close()

        data = json.dumps({'s_d': 0, 'result': 'success ',
                          'fileUrl': path_order_outbound_time_result, 'scriptType': 1})
        r = requests.post(Post_url, data, auth=('user', '*****'))

    except TypeError as e:
        data = json.dumps({'s_d': 1, 'result': 'fail ',
                          'fileUrl': '', 'scriptType': 1})
        r = requests.post(Post_url, data, auth=('user', '*****'))
