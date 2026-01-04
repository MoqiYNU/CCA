from collections import Counter
import copy
from graphviz import Digraph


# 1.1.定义编排--------------------------------------------
class CHOR(object):

    def __init__(self, source, sinks, places, trans, flows, interaction_map):
        self.source = source
        self.sinks = sinks
        # 库所集
        self.places = places
        # 变迁集
        self.trans = trans
        # 流集
        self.flows = flows
        # 变迁上关联的交互
        self.interaction_map = interaction_map

    # 重命名编排(e.g. p1 -> p1(1)), flag形如'(1),(2),...'
    def rename_chor(self, flag):

        # 1. 创建库所和变迁的映射关系
        place_old_to_new = {}
        trans_old_to_new = {}

        # 更新库所集合
        for place in self.places[:]:
            new_place = place + flag
            place_old_to_new[place] = new_place
            self.places[self.places.index(place)] = new_place

        # 更新变迁集合
        for tran in self.trans[:]:
            new_tran = tran + flag
            trans_old_to_new[tran] = new_tran
            self.trans[self.trans.index(tran)] = new_tran

        # 2. 更新源标识 (source)
        old_source_places = self.source.get_infor()
        new_source_places = [place_old_to_new[p] for p in old_source_places]
        self.source = Marking(new_source_places)

        # 3. 更新汇标识 (sinks)
        new_sinks = []
        for sink in self.sinks:
            old_sink_places = sink.get_infor()
            new_sink_places = [place_old_to_new[p] for p in old_sink_places]
            new_sinks.append(Marking(new_sink_places))
        self.sinks = new_sinks

        # 4. 更新流 (flows) 中涉及的库所和变迁
        new_flows = []
        for flow in self.flows:
            flow_from, flow_to = flow.get_infor()
            new_from = flow_from + flag
            new_to = flow_to + flag
            new_flows.append(Flow(new_from, new_to))
        self.flows = new_flows

        # 5. 更新interaction_map中的交互
        # 注意：interaction_map中的交互形如[0, msg, {a,b,c}]
        # 其中msg是消息名，{a,b,c}是参与角色的集合
        new_interaction_map = {}
        for tran, interaction in self.interaction_map.items():
            new_tran = trans_old_to_new[tran]

            if interaction == 'tau':  #  若关联空交互则保持不变
                new_interaction_map[new_tran] = interaction
            else:
                # 创建新的交互列表
                new_interaction = []

                # 处理交互列表的每个元素
                for i, element in enumerate(interaction):
                    if i == 0:
                        # 第0个元素（交互类型标识）保持不变
                        new_interaction.append(element)
                    else:
                        # 其他元素直接添加后缀
                        new_interaction.append(element + flag)

                # 替换交互列表
                new_interaction_map[new_tran] = new_interaction
        # 更新interaction_map
        self.interaction_map = new_interaction_map


# 1.2.定义开放网------------------------------------------
class OpenNet(object):

    def __init__(self, source, sinks, places, trans, label_map, flows):
        self.source = source
        self.sinks = sinks
        # places=inner_places+msg_places
        self.places = places
        self.inner_places = []
        self.msg_places = []
        # 标识库所(ps:建模中不涉及,用于标识执行路径)
        self.idf_places = []
        # 资源库所
        self.init_res = []  # 初始资源向量
        self.res_places = []
        # 资源属性映射:0为可重复使用资源,1为消耗资源
        self.res_property = {}
        # 变迁请求资源映射:<变迁,请求资源>
        self.req_res_map = {}
        # 变迁释放资源映射:<变迁,释放资源>
        self.rel_res_map = {}
        # 变迁及路由变迁
        self.trans = trans
        self.rout_trans = []
        self.label_map = label_map
        # 变迁Delay映射
        self.tran_delay_map = {}
        # 流和抑制弧(ps:建模中不考虑抑制弧)
        self.flows = flows
        self.inhibitor_arcs = []
        # 沿用流和删除流(ps:建模中不考虑,用于实现面向执行路径策略)
        self.follow_arcs = []
        self.delete_arcs = []
        # 消息和资源id到name映射
        self.msg_place_map = {}
        self.res_place_map = {}
        # 网对应的角色(用于编排分析)
        self.role = 'NULL'

    # 1)Utils方法~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def get_start_ends(self):
        return self.source, self.sinks

    def get_places(self):
        return self.places, self.inner_places, self.msg_places

    def get_res_places(self):
        return self.res_places, self.res_property, self.req_res_map, self.rel_res_map

    # 获取某个资源的初始数量
    def get_res_init_num(self, res):
        number = 0
        for temp_res in self.init_res:
            if temp_res == res:
                number += 1
        return number

    def get_init_res(self):
        return self.init_res

    def get_trans(self):
        return self.trans, self.rout_trans, self.label_map

    def get_flows(self):
        return self.flows

    # Note:添加资源到源中(Note:可以重复)
    def add_res_to_source(self, rps):
        self.source = Marking(self.source.get_infor() + rps)

    def add_places(self, pls):
        self.places = list(set(self.places + pls))

    def add_inner_places(self, pls):
        self.inner_places = list(set(self.inner_places + pls))

    def add_msg_places(self, pls):
        self.msg_places = list(set(self.msg_places + pls))

    def set_res_places(self, pls):
        self.res_places = list(set(self.res_places + pls))

    def set_res_property(self, rp):
        self.res_property = rp

    def set_req_res_map(self, rrm):
        self.req_res_map = rrm

    def set_init_res(self, ir):
        self.init_res = ir

    def add_trans(self, trs):
        self.trans = list(set(self.trans + trs))

    def add_rout_trans(self, trs):
        self.rout_trans = list(set(self.rout_trans + trs))

    def add_flows(self, fls):  #ps:不重复添加
        for fl in fls:
            flow_from, flow_to = fl.get_infor()
            # 先判断添加流是否存在
            if not self.flow_is_exist(flow_from, flow_to):
                self.flows.append(Flow(flow_from, flow_to))

    def add_flow(self, flow_from, flow_to):  #ps:不重复添加
        # 先判断添加流是否存在
        if not self.flow_is_exist(flow_from, flow_to):
            self.flows.append(Flow(flow_from, flow_to))

    # 判断某条流是否存在
    def flow_is_exist(self, flow_from, flow_to):
        for temp_flow in self.flows:
            temp_flow_from, temp_flow_to = temp_flow.get_infor()
            if temp_flow_from == flow_from and temp_flow_to == flow_to:
                return True
        return False

    # 获取异步交互活动集
    def get_asyn_inter_trans(self):
        asyn_inter_trans = []
        for flow in self.flows:
            flow_from, flow_to = flow.get_infor()
            if flow_from in self.msg_places:
                asyn_inter_trans.append(flow_to)
            if flow_to in self.msg_places:
                asyn_inter_trans.append(flow_from)
        return asyn_inter_trans

    # 移除库收
    def rov_place(self, pl):
        self.places.remove(pl)

    # 移除多个库收
    def rov_places(self, pls):
        for pl in pls:
            for place in self.places[::-1]:
                if pl == place:
                    self.places.remove(place)
                    break

    # 移除多个内部库收
    def rov_internal_places(self, pls):
        for pl in pls:
            for place in self.inner_places[::-1]:
                if pl == place:
                    self.inner_places.remove(place)
                    break

    # 移除多条变迁
    def rov_trans(self, trs):
        for tr in trs:
            for tran in self.trans[::-1]:
                if tr == tran:
                    self.trans.remove(tran)
                    break

    # 移除库所/变迁(用于生成执行路径)~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def rov_objs(self, objs):
        for obj in objs:
            # a)移除库所对应部分
            if obj in self.places:
                self.places.remove(obj)
                if obj in self.inner_places:
                    self.inner_places.remove(obj)
                if obj in self.msg_places:
                    self.msg_places.remove(obj)
                if obj in self.res_places:
                    self.res_places.remove(obj)
                    if obj in self.res_property.keys():
                        self.res_property.pop(obj)
                    if obj in self.req_res_map.keys():
                        self.req_res_map.pop(obj)
                    # 更新初始资源
                    self.init_res = [
                        res for res in self.init_res if res != obj
                    ]
            # b)移除变迁对应部分
            if obj in self.trans:
                self.trans.remove(obj)
                if obj in self.label_map.keys():
                    self.label_map.pop(obj)
                # Note:更新请求资源映射
                for res, req_trans in self.req_res_map.items():
                    if obj in req_trans:
                        req_trans.remove(obj)

    # 移除库所/变迁所关联的流集
    def rov_flows_by_obj(self, obj):
        # Note:********避免跳过for循环中相邻的待删除元素****************
        for flow in self.flows[::-1]:
            fl_from, fl_to = flow.get_infor()
            if fl_from == obj or fl_to == obj:
                self.flows.remove(flow)

    # 移除流
    def rov_flow(self, flow_from, flow_to):
        for flow in self.flows:
            fl_from, fl_to = flow.get_infor()
            if flow_from == fl_from and flow_to == fl_to:
                self.flows.remove(flow)
                break

    # 移除多条流
    def rov_flows(self, fls):
        for fl in fls[::-1]:
            fl_from, fl_to = fl.get_infor()
            for flow in self.flows[::-1]:
                flow_from, flow_to = flow.get_infor()
                if flow_from == fl_from and flow_to == fl_to:
                    self.flows.remove(flow)
                    break

    # 利用前/后集计算网对应的有向图~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def to_graph(self):
        graph = {}
        for place in self.places:
            succ_nodes = '#'.join(get_postset(self.flows, place))
            graph[place] = succ_nodes
        for tran in self.trans:
            succ_nodes = '#'.join(get_postset(self.flows, tran))
            graph[tran] = succ_nodes
        return graph

    # 重命名变迁~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def rename_trans(self, trans, renamed_trans):

        updated_trans = []
        for tran in self.trans:
            if tran in trans:
                index = trans.index(tran)
                updated_trans.append(renamed_trans[index])
            else:
                updated_trans.append(tran)

        updated_req_res_map = {}
        for key, val in self.req_res_map.items():
            if key in trans:
                index = trans.index(key)
                updated_req_res_map[renamed_trans[index]] = val
            else:
                updated_req_res_map[key] = val

        updated_rel_res_map = {}
        for key, val in self.rel_res_map.items():
            if key in trans:
                index = trans.index(key)
                updated_rel_res_map[renamed_trans[index]] = val
            else:
                updated_rel_res_map[key] = val

        updated_label_map = {}
        for tran, label in self.label_map.items():
            if tran in trans:
                index = trans.index(tran)
                updated_label_map[renamed_trans[index]] = renamed_trans[index]
            else:
                updated_label_map[tran] = label

        updated_tran_delay_map = {}
        for tran, delay in self.tran_delay_map.items():
            if tran in trans:
                index = trans.index(tran)
                updated_tran_delay_map[renamed_trans[index]] = delay
            else:
                updated_tran_delay_map[tran] = delay

        updated_flows = []
        for flow in self.flows:
            fl_from, fl_to = flow.get_infor()
            if fl_from in self.places:
                if fl_to in trans:
                    index = trans.index(fl_to)
                    updated_flow = Flow(fl_from, renamed_trans[index])
                    updated_flows.append(updated_flow)
                else:
                    updated_flows.append(flow)
            elif fl_to in self.places:
                if fl_from in trans:
                    index = trans.index(fl_from)
                    updated_flow = Flow(renamed_trans[index], fl_to)
                    updated_flows.append(updated_flow)
                else:
                    updated_flows.append(flow)

        self.req_res_map = updated_req_res_map
        self.rel_res_map = updated_rel_res_map
        self.trans = updated_trans
        # self.rout_trans = []  #设置路由变迁为空
        self.label_map = updated_label_map
        self.tran_delay_map = updated_tran_delay_map
        self.flows = updated_flows

    # 重命名网(e.g. p1 -> p1(1)), flag形如'(1),(2),...'
    def rename_net(self, flag):

        # 1. 创建库所和变迁的映射关系
        place_old_to_new = {}
        trans_old_to_new = {}

        # 更新库所集合
        for place in self.places[:]:
            new_place = place + flag
            place_old_to_new[place] = new_place
            self.places[self.places.index(place)] = new_place

        # 更新变迁集合('/'出现在生产的公共视图中,需特殊处理)
        for tran in self.trans[:]:
            if '/' not in tran:
                new_tran = tran + flag
            else:
                parts = tran.split('/')
                # 类似a1/2----->a1(1)/2,flag = '(1)'
                new_tran = parts[0] + flag + '/' + parts[1]
            trans_old_to_new[tran] = new_tran
            self.trans[self.trans.index(tran)] = new_tran

        # 2. 更新角色 (role)
        if self.role != 'NULL':
            self.role = self.role + flag

        # 3. 更新内部库所集合
        for i, place in enumerate(self.inner_places):
            self.inner_places[i] = place_old_to_new[place]

        # 4. 更新消息库所集合
        for i, place in enumerate(self.msg_places):
            self.msg_places[i] = place_old_to_new[place]

        # 5. 更新标签映射 (label_map) - 同时更新键和值
        new_label_map = {}
        for tran, label in self.label_map.items():
            new_tran = trans_old_to_new[tran]
            new_label_map[new_tran] = label + flag
        self.label_map = new_label_map

        # 6. 更新源标识 (source)
        old_source_places = self.source.get_infor()
        new_source_places = [place_old_to_new[p] for p in old_source_places]
        self.source = Marking(new_source_places)

        # 7. 更新汇标识 (sinks)
        new_sinks = []
        for sink in self.sinks:
            old_sink_places = sink.get_infor()
            new_sink_places = [place_old_to_new[p] for p in old_sink_places]
            new_sinks.append(Marking(new_sink_places))
        self.sinks = new_sinks

        # 8. 更新流 (flows) 中涉及的库所和变迁
        new_flows = []
        for flow in self.flows:
            flow_from, flow_to = flow.get_infor()
            # 检查是否是库所
            if flow_from in place_old_to_new:
                new_from = place_old_to_new[flow_from]
            else:
                new_from = trans_old_to_new.get(flow_from, flow_from)

            if flow_to in place_old_to_new:
                new_to = place_old_to_new[flow_to]
            else:
                new_to = trans_old_to_new.get(flow_to, flow_to)

            new_flows.append(Flow(new_from, new_to))
        self.flows = new_flows

    # 2)打印网信息~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def print_infor(self):

        print(
            '\n=============================================================================='
        )
        print(
            '                                   Net Infor                                     '
        )
        print(
            '------------------------------------------------------------------------------'
        )

        # a)将网中元素转换为字符串=================================================
        infor = []
        str_source = '[' + ', '.join(self.source.get_infor()) + ']'
        infor.append(['Source', str_source])

        slist = []
        for sink in self.sinks:
            slist.append('[' + ', '.join(sink.get_infor()) + ']')
        str_sinks = '{' + ', '.join(slist) + '}'
        infor.append(['Sinks', str_sinks])

        str_pls = '[' + ', '.join(self.places) + ']'
        infor.append(['Places', str_pls])

        str_mpls = '[' + ', '.join(self.msg_places) + ']'
        infor.append(['Msg Places', str_mpls])

        str_rpls = '[' + ', '.join(self.res_places) + ']'
        infor.append(['Res Places', str_rpls])

        str_ir = '[' + ', '.join(self.init_res) + ']'
        infor.append(['Initial Res', str_ir])

        rp_list = []
        for res, pro in self.res_property.items():
            rp_list.append(res + ': ' + str(pro))
        str_rp = '{' + ', '.join(rp_list) + '}'
        infor.append(['Res Property', str_rp])

        rrm_list = []
        for tran, res in self.req_res_map.items():
            rrm_list.append(tran + ': ' + '[' + ', '.join(res) + ']')
        str_rrm = '{' + ', '.join(rrm_list) + '}'
        infor.append(['Req Res Map', str_rrm])

        rerm_list = []
        for tran, res in self.rel_res_map.items():
            rerm_list.append(tran + ': ' + '[' + ', '.join(res) + ']')
        str_rerm = '{' + ', '.join(rerm_list) + '}'
        infor.append(['Rel Res Map', str_rerm])

        str_trans = '[' + ', '.join(self.trans) + ']'
        infor.append(['Trans', str_trans])

        str_rts = '[' + ', '.join(self.rout_trans) + ']'
        infor.append(['Rout Trans', str_rts])

        dy_list = []
        for tran, delay in self.tran_delay_map.items():
            dy_list.append(tran + ': [' + str(delay[0]) + ", " +
                           str(delay[1]) + ']')
        str_dy = '{' + ', '.join(dy_list) + '}'
        infor.append(['Tran Delay', str_dy])

        lm_list = []
        for tran, label in self.label_map.items():
            lm_list.append(tran + ': ' + label)
        str_lm = '{' + ', '.join(lm_list) + '}'
        infor.append(['Label Map', str_lm])

        flow_list = []
        for flow in self.flows:
            fl_from, fl_to = flow.get_infor()
            flow_list.append('(' + fl_from + ', ' + fl_to + ')')
        str_fls = '{' + ', '.join(flow_list) + '}'
        infor.append(['Flows', str_fls])

        inhib_arcs_list = []
        for flow in self.inhibitor_arcs:
            pl, tran = flow.get_infor()
            inhib_arcs_list.append('(' + pl + ', ' + tran + ')')
        inhib_arc_fls = '{' + ', '.join(inhib_arcs_list) + '}'
        infor.append(['Inhib Arcs', inhib_arc_fls])

        # Following Arcs
        follow_arcs_list = []
        for [pl, tran] in self.follow_arcs:
            follow_arcs_list.append('(' + pl + ', ' + tran + ')')
        str_fol_arcs = '{' + ', '.join(follow_arcs_list) + '}'
        infor.append(['Follow Arcs', str_fol_arcs])

        # Deleting Arcs
        delete_arcs_list = []
        for [pl, tran] in self.delete_arcs:
            delete_arcs_list.append('(' + pl + ', ' + tran + ')')
        str_del_arcs = '{' + ', '.join(delete_arcs_list) + '}'
        infor.append(['Delete Arcs', str_del_arcs])

        # Role
        infor.append(['Role', self.role])

        # b)格式化字符串=================================================
        len_list = [len(x[0]) for x in infor]
        offset = max(len_list) + 5
        for item in infor:
            lines = int(len(item[1]) / 52)
            if lines < 1:
                print("%s: %s" % (item[0].ljust(offset), item[1]))
            else:
                line_str = []
                for i in range(lines + 1):
                    val = item[1][i * 52:(i + 1) * 51 + i + 1]
                    if i == 0:
                        line_str.append(val)
                    else:
                        # Note:第一行之后的每行前面添加20个空格
                        line_str.append(' ' * 20 + val)
                print("%s: %s" % (item[0].ljust(offset), '\n'.join(line_str)))

        print(
            '=============================================================================='
        )

    # 3)利用dot显示~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def net_to_dot(self, name, delay_flag):

        # Note:保存为png格式
        dot = Digraph(filename=name, format='jpg')
        dot.graph_attr['rankdir'] = 'LR'
        # 设置dpi=600
        dot.graph_attr['dpi'] = '300'

        for place in self.places:
            dot.node(
                name=place,
                label="",  #ps:库所不显示标签
                # xlabel=place,
                fixedsize='true',
                shape='circle',
                # Note:数字也要加''
                # height='0.3'
                height='.3',
                width='.3')
            # 设置初始库所内有一个黑点'&bull;'
            if place in self.source.get_infor():
                dot.node(name=place, label='&bull;')
            # 内部库所
            if place in self.inner_places:
                print('inner place: ', place)
                dot.node(name=place, xlabel=place)
            # 消息库所为暗红色填充
            if place in self.msg_places:
                dot.node(name=place,
                         xlabel=place,
                         shape='circle',
                         style='filled',
                         color='chartreuse4')
            # 汇库所为绿色加粗边框
            # print('test:', self.sinks)
            if place in self.sinks[0].get_infor():
                dot.node(name=place,
                         shape='circle',
                         style='bold',
                         color='chartreuse3')

        # 资源库所为蓝色填充
        for place in self.res_places:
            dot.node(name=place,
                     label=str(1 if place in self.source.get_infor() else 0),
                     xlabel=place,
                     shape='doublecircle',
                     fixedsize='true',
                     color='blue',
                     height='.2',
                     width='.2')

        # 设置标记库所内开始有一个托肯
        for place in self.idf_places:
            dot.node(name=place,
                     label=str(1),
                     xlabel=place,
                     shape='doublecircle',
                     fixedsize='true',
                     color='chocolate',
                     height='.2',
                     width='.2')

        for tran in self.trans:
            # dot.node(name=tran, shape='rect', height='0.1', fontsize='10')
            if delay_flag:
                min_delay = self.tran_delay_map[tran][0]
                max_delay = self.tran_delay_map[tran][1]
                delay = '[' + str(min_delay) + ', ' + str(max_delay) + ']'
                dot.node(name=tran,
                         label=self.label_map[tran] + ' ' + delay,
                         shape='rect',
                         height='.3',
                         width='.3')
            else:
                dot.node(
                    name=tran,
                    shape='rect',
                    height='.3',
                    width='.3',
                )

        for flow in self.flows:
            fl_from, fl_to = flow.get_infor()
            dot.edge(fl_from, fl_to, arrowhead='normal', arrowsize='0.8')

        for [pl, tran] in self.follow_arcs:
            dot.edge(
                pl,
                tran,
                style='dashed',  #虚线
                arrowhead='odot',
                color='chocolate',
                arrowsize='0.8')

        for [pl, tran] in self.delete_arcs:
            dot.edge(
                pl,
                tran,
                style='dashed',  #虚线
                arrowhead='onormal',
                color='chocolate',
                arrowsize='0.8')

        # 请求和释放资源边
        for tran, res in self.req_res_map.items():
            if not res:
                continue
            counter = Counter(res)
            # print(res[0])
            # dot.edge(res[0],
            #          tran,
            #          label=str(len(res)),
            #          arrowhead='normal',
            #          color='blue',
            #          arrowsize='0.8')
            for re in res:
                dot.edge(re,
                         tran,
                         label=str(counter[re]),
                         arrowhead='normal',
                         color='blue',
                         arrowsize='0.8')
        for tran, res in self.rel_res_map.items():
            if not res:
                continue
            counter = Counter(res)
            # print(res[0])
            # dot.edge(tran,
            #          res[0],
            #          label=str(len(res)),
            #          arrowhead='normal',
            #          color='blue',
            #          arrowsize='0.8')
            for re in res:
                dot.edge(tran,
                         re,
                         label=str(counter[re]),
                         arrowhead='normal',
                         color='blue',
                         arrowsize='0.8')

        # 抑制弧
        for ih_arc in self.inhibitor_arcs:
            dot.edge(
                ih_arc[0],
                ih_arc[1],
                style='dashed',  #虚线
                color='#c00000',
                arrowhead='odot',
                arrowsize='0.8')

        # 实时看画的图
        dot.view()


# 2.定义时间标记网------------------------------------------
class TSN(object):

    def __init__(self, places, trans, flows, enable_time_map, fire_time_map):
        self.places = places
        self.trans = trans
        self.flows = flows
        # 变迁关联使能/点火时间映射
        self.eanble_time_map = enable_time_map
        self.fire_time_map = fire_time_map


# 3.定义流关系-----------------------------------------------
class Flow(object):

    def __init__(self, flow_from, flow_to):
        self.flow_from = flow_from
        self.flow_to = flow_to

    def get_infor(self):
        return self.flow_from, self.flow_to


# 4.定义标识------------------------------------------------
class Marking(object):

    def __init__(self, places):
        self.places = places

    # Note:深度拷贝,防止动态更新
    def get_infor(self):
        return copy.deepcopy(self.places)


# 5.判断标识的函数-------------------------------------------
def marking_is_exist(marking, marking_list):
    for mk in marking_list:
        if equal_markings(marking, mk):
            return True
        else:
            continue
    return False


# 判断两个标识是否相同
def equal_markings(marking1, marking2):
    places1 = marking1.get_infor()
    places2 = marking2.get_infor()
    if Counter(places1) == Counter(places2):
        return True
    else:
        return False


# 判断两个标识集是否相同
def equal_marking_sets(markings1, markings2):
    if len(markings1) != len(markings2):
        return False
    card_map1 = get_cardinality(markings1)
    card_map2 = get_cardinality(markings2)
    if len(card_map1) != len(card_map2):
        return False
    for key, val in card_map1.items():
        if get_count(key, card_map1) != get_count(key, card_map2):
            return False
    return True


# 统计markings中标识数量
def get_cardinality(markings):
    card_map = {}
    for marking in markings:
        count = get_count(marking, card_map)
        if count == -1:
            card_map[marking] = 1
        else:
            card_map[marking] = count + 1
    return card_map


def get_count(marking, card_map):
    for key, val in card_map.items():
        if equal_markings(key, marking):
            return val
    return -1


# 6.获取后继标识----------------------------------------------
def succ_marking(places, preset, postset):
    # 移除前集
    for place in preset:
        if place in places:
            places.remove(place)
    places = places + postset
    return Marking(places)


# 7.获得places下所有使能变迁(P/T网)------------------------------
def get_enable_trans(net, marking):
    enable_trans = []
    trans = net.trans
    for tran in trans:
        if is_enable(net, tran, marking):
            enable_trans.append(tran)
    return enable_trans


# 8.判断标识是否使能-------------------------------------------
def is_enable(net, tran, marking):
    places = marking.get_infor()
    preset = get_preset(net.flows, tran)
    cou = Counter(places)
    cou.subtract(Counter(preset))
    vals = cou.values()
    for val in vals:
        if val < 0:
            return False
    return True


# 9.获取元素elem(库所或变迁)的前集------------------------------
def get_preset(flows, elem):
    preset = set()
    for flow in flows:
        flow_from, flow_to = flow.get_infor()
        if flow_to == elem:
            preset.add(flow_from)
    return list(preset)


# 10.获取元素elem(库所或变迁)的后集-------------------------------
def get_postset(flows, elem):
    postset = set()
    for flow in flows:
        flow_from, flow_to = flow.get_infor()
        if flow_from == elem:
            postset.add(flow_to)
    return list(postset)
