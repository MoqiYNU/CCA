# coding=gbk

from collections import Counter
import copy
from lts import LTS, Tran
import net as nt
import net_gen as ng
import comp_utils as cu
import lts_utils as lu


# 1.1产生可达图(资源用资源库所标识,每个资源对应一个资源库所)------------------------------
def gen_rg(net: nt.OpenNet):

    source, sinks = net.get_start_ends()

    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    visiting_queue = [source]
    visited_queue = [source]

    # 迭代计算
    while visiting_queue:
        marking = visiting_queue.pop(0)
        enable_trans = nt.get_enable_trans(net, marking)
        for enable_tran in enable_trans:
            # 1)产生后继标识
            preset = nt.get_preset(net.get_flows(), enable_tran)
            postset = nt.get_postset(net.get_flows(), enable_tran)
            succ_makring = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            # 2)产生后继变迁(Note:迁移动作以标号进行标识)
            # tran = Tran(marking, label_map[enable_tran], succ_makring)
            # 迁移动作以变迁Id进行标识
            tran = Tran(marking, enable_tran, succ_makring)
            gen_trans.append(tran)
            # 添加未访问的状态
            if not nt.marking_is_exist(succ_makring, visited_queue):
                visiting_queue.append(succ_makring)
                visited_queue.append(succ_makring)

    # 终止标识(ps:允许消息库所和资源库所不为空)
    end_markings = []
    msg_places = net.msg_places
    res_places = net.res_places
    for marking in visited_queue:
        places = marking.get_infor()
        inter_places = [
            place for place in places
            if place not in msg_places and place not in res_places
        ]
        new_marking = nt.Marking(inter_places)
        if nt.marking_is_exist(new_marking, sinks):
            # print('end marking:', places)
            end_markings.append(marking)

    return LTS(source, sinks, visited_queue, gen_trans)


# 确定网的正确性
def check_net_correctness(net: nt.OpenNet):
    rg = gen_rg(net)
    print('rg size:', len(rg.states))
    lts, marking_map = rg.rg_to_lts()
    for index, marking in enumerate(lts.states):
        print(index, marking_map[marking].get_infor())
    # ps:先将lts转换为邻接表实现快速检测
    adj_list = lu.lts_to_adjacency_list(lts)
    lts.lts_to_dot_name('rg', net.label_map)
    valid_states = []
    states = lts.states
    ends = lts.ends
    # 计算每个状态的迁移闭包
    for state in states:
        tran_closure = lu.gen_tran_closure(state, adj_list)
        # print('tran closure:', state, tran_closure)
        if set(tran_closure) & set(ends):
            valid_states.append(state)
    if len(valid_states) == len(states):
        return 'Correct'
    if len(valid_states) == 0:
        return 'Fully Incorrect'
    else:
        return 'Partially Correct'


# 1.2产生可达图(资源用list表示)----------------------------------------------------
def gen_rg_with_res(net):

    source, sinks = net.get_start_ends()

    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    print('net.init_res', net.init_res)
    visiting_queue = [[source, net.init_res]]
    visited_queue = [[source, net.init_res]]

    # 迭代计算
    while visiting_queue:
        [marking, res] = visiting_queue.pop(0)
        enable_trans = nt.get_enable_trans(net, marking)
        for enable_tran in enable_trans:
            # Note:避免分支中提前将资源消耗掉(每个分支获得资源相同)~~~~~~~~~~~~~~~~~~
            res_copy = copy.deepcopy(res)
            req_res = net.req_res_map[enable_tran]
            # 若当前资源不充足,则跳过当前使能变迁
            if not res_is_suff(res_copy, req_res):
                continue
            # 1)产生后继状态
            preset = nt.get_preset(net.get_flows(), enable_tran)
            postset = nt.get_postset(net.get_flows(), enable_tran)
            succ_makring = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            succ_res = get_succ_res(res_copy, net.req_res_map[enable_tran],
                                    net.rel_res_map[enable_tran])
            # 2)产生后继变迁(Note:迁移动作以标号进行标识)
            tran = Tran([marking, res], enable_tran, [succ_makring, succ_res])
            gen_trans.append(tran)
            # 添加未访问的状态
            if not state_is_exist([succ_makring, succ_res], visited_queue):
                visiting_queue.append([succ_makring, succ_res])
                visited_queue.append([succ_makring, succ_res])

    # 添加终止状态集(ps:允许消息库所和资源库所不为空)
    end_states = []
    for state in visited_queue:
        [marking, res] = state
        if nt.marking_is_exist(marking, sinks):
            end_states.append(state)

    return LTS([source, net.init_res], end_states, visited_queue, gen_trans)


# 判断当前资源是否充足
def res_is_suff(res, req_res):
    cou = Counter(res)
    cou.subtract(Counter(req_res))
    vals = cou.values()
    for val in vals:
        if val < 0:
            return False
    return True


# 获取变迁迁移后的资源集合(ps:资源是list类型)
def get_succ_res(res, req_res, rel_res):
    # 移除前集
    for rr in req_res:
        if rr in res:
            res.remove(rr)
    succ_res = res + rel_res
    return succ_res


# 判断后继状态是否已存在
def state_is_exist(succ_state, visited_queue):
    [succ_makring, succ_res] = succ_state
    for temp_state in visited_queue:
        [temp_makring, temp_res] = temp_state
        if nt.equal_markings(
                succ_makring,
                temp_makring) and Counter(succ_res) == Counter(temp_res):
            return True
    return False


# 2.1利用稳固集产生可达图(每个资源对应一个资源库所)-----------------------------------------
def gen_rg_with_subset(net: nt.OpenNet):

    source, sinks = net.get_start_ends()
    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    visiting_queue = [source]
    visited_queue = [source]

    # 迭代计算
    while visiting_queue:
        marking = visiting_queue.pop(0)
        # print(marking.get_infor())
        # Note:只迁移稳固集中使能活动(未考虑消除忽视问题)
        enable_trans = nt.get_enable_trans(net, marking)
        S = get_stubset(net, marking, enable_trans)
        enable_trans = list(set(enable_trans).intersection(set(S)))
        for enable_tran in enable_trans:
            # 1)产生后继标识
            preset = nt.get_preset(net.get_flows(), enable_tran)
            postset = nt.get_postset(net.get_flows(), enable_tran)
            succ_makring = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            # 2)产生后继变迁(Note:迁移动作以标号进行标识)
            tran = Tran(marking, enable_tran, succ_makring)
            gen_trans.append(tran)
            # 添加未访问的状态
            if not nt.marking_is_exist(succ_makring, visited_queue):
                visiting_queue.append(succ_makring)
                visited_queue.append(succ_makring)

    # 终止标识(ps:允许消息库所和资源库所不为空)
    end_markings = []
    msg_places = net.msg_places
    res_places = net.res_places
    for marking in visited_queue:
        places = marking.get_infor()
        inter_places = [
            place for place in places
            if place not in msg_places and place not in res_places
        ]
        new_marking = nt.Marking(inter_places)
        if nt.marking_is_exist(new_marking, sinks):
            print('end marking:', places)
            end_markings.append(marking)

    return LTS(source, end_markings, visited_queue, gen_trans)


# 2.2利用稳固集产生中间约简可达图(资源用list表示)--------------------------------------
def gen_rrg(net: nt.OpenNet):
    '''
    Note:由于建模采用工作流网,故不存在忽视问题
    '''
    source, sinks = net.get_start_ends()
    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    visiting_queue = [[source, net.init_res]]
    visited_queue = [[source, net.init_res]]

    # 迭代计算
    while visiting_queue:
        [marking, res] = visiting_queue.pop(0)
        # Note:只迁移稳固集中使能活动
        enable_trans = []
        # 求受到资源约束的使能迁移集
        for tran in nt.get_enable_trans(net, marking):
            # Note:避免分支中提前将资源消耗掉(每个分支获得资源相同)
            res_copy = copy.deepcopy(res)
            req_res = net.req_res_map[tran]
            # 若当前资源充足,则添加使能变迁
            if res_is_suff(res_copy, req_res):
                enable_trans.append(tran)
        S = get_stubset(net, marking, enable_trans)
        enable_trans = list(set(enable_trans).intersection(set(S)))
        for enable_tran in enable_trans:
            # Note:避免分支中提前将资源消耗掉(每个分支获得资源相同)
            res_copy = copy.deepcopy(res)
            # 1)产生后继状态
            preset = nt.get_preset(net.get_flows(), enable_tran)
            postset = nt.get_postset(net.get_flows(), enable_tran)
            succ_makring = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            succ_res = get_succ_res(res_copy, net.req_res_map[enable_tran],
                                    net.rel_res_map[enable_tran])
            # 2)产生后继变迁(Note:迁移动作以标号进行标识)
            tran = Tran([marking, res], enable_tran, [succ_makring, succ_res])
            gen_trans.append(tran)
            # 添加未访问的状态
            if not state_is_exist([succ_makring, succ_res], visited_queue):
                visiting_queue.append([succ_makring, succ_res])
                visited_queue.append([succ_makring, succ_res])

    # 添加终止状态集(ps:允许消息库所和资源库所不为空)
    end_states = []
    for state in visited_queue:
        [marking, res] = state
        if nt.marking_is_exist(marking, sinks):
            end_states.append(state)

    return LTS([source, net.init_res], end_states, visited_queue, gen_trans)


# 3.计算特定标识的稳固集----------------------------------------------------
def get_stubset(net, marking, enable_trans):
    S = []  # 返回稳固集
    U = []  # 未处理迁移集
    if not enable_trans:  # 没有使能迁移,直接返回空稳固集S
        return S
    else:  # 有使能迁移,随机选择一个使能迁移
        first_act = enable_trans[0]
        # print('first_act', first_act)
        S.append(first_act)
        U.append(first_act)
        while U:
            act = U.pop(0)
            if act in enable_trans:
                N = get_disenabling_trans(net, act)
            else:
                N = get_enabling_trans(net, act, marking)
            # 避免重复添加
            subset = set(N) - set(S)
            U = list(set(U).union(subset))
            # 添加稳固集到S
            S = list(set(S).union(N))
        return S


# 获取导致变迁使能的变迁集(以Id标识)
def get_enabling_trans(net, tran, marking):
    enabling_trans = set()
    places = marking.get_infor()
    preset = nt.get_preset(net.get_flows(), tran)
    for place in preset:
        # 跳过已经含有托肯的库所
        if place in places:
            continue
        # 不重复
        enabling_trans = enabling_trans.union(
            set(nt.get_preset(net.get_flows(), place)))
    return list(enabling_trans)


# 获取变迁的冲突变迁集(以Id标识)
def get_disenabling_trans(net: nt.OpenNet, tran):
    disenabling_trans = []
    trans = net.trans
    preset = nt.get_preset(net.get_flows(), tran)
    for temp_tran in trans:
        # 不包括自己
        if temp_tran == tran:
            continue
        temp_preset = nt.get_preset(net.get_flows(), temp_tran)
        # 前集相交(冲突)
        if set(preset).intersection(set(temp_preset)):
            disenabling_trans.append(temp_tran)
    return disenabling_trans


if __name__ == '__main__':

    # path = '/Users/moqi/Desktop/原始模型/过程挖掘/IMPL_副本2.xml'
    # path = '/Users/moqi/Desktop/原始模型/过程挖掘/稳固集测试.xml'
    path = '/Users/moqi/Desktop/报奖/投稿2/IMPL.xml'
    # path = '/Users/moqi/Desktop/原始模型/过程挖掘/模型_6.22/实验模型/Log-01.xml'

    nets = ng.gen_nets(path)
    comp_net = cu.get_compose_net(nets)
    # comp_net.net_to_dot('comp_net', False)
    # comp_net = cu.res_to_places(comp_net)
    result = check_net_correctness(comp_net)
    print('result:', result)
    # back_trans = cu.get_back_trans(path)

    # new_comp_net = cu.consume_res_to_places(comp_net)
    # new_comp_net.print_infor()

    # # res_rg = gen_rg_with_res(comp_net)
    # # rrg = gen_rg_with_subset(new_comp_net)
    # rrg = su.remove_ignore(new_comp_net)
    # lts = rrg.rg_to_lts()[0]
    # lts.lts_to_dot_name('rrg')
