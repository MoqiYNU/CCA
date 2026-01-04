from collections import Counter
import copy
import itertools
from lts import LTS, Tran


# 1.定义最小化过程中合并状态------------------------------------------
class MinState(object):

    def __init__(self, id, states):
        self.id = id
        self.states = states

    def get_infor(self):
        return self.id, self.states


# 2.1获得state的tau闭包---------------------------------------------
def gen_tau_closure(state, lts):
    # 运行队列和已访问队列
    ing_queue = [state]
    ed_queue = [state]  # 包括state
    # 迭代计算
    while ing_queue:
        from_state = ing_queue.pop(0)
        tau_states = gen_tau_states(from_state, lts)
        for tau_state in tau_states:
            if tau_state not in ed_queue:
                ing_queue.append(tau_state)
                ed_queue.append(tau_state)
    return ed_queue


# 获得state的tau迁移状态
def gen_tau_states(state, lts):
    tau_states = set()
    start, ends, states, trans = lts.get_infor()
    for tran in trans:
        state_from, label, state_to = tran.get_infor()
        if state_from == state and label == 'tau':
            tau_states.add(state_to)
    return list(tau_states)


def gen_tau_closure_adv(state, adj_list):
    # 运行队列和已访问队列
    ing_queue = [state]
    ed_queue = [state]  # 包括state
    # 迭代计算
    while ing_queue:
        from_state = ing_queue.pop(0)
        tau_states = gen_tau_states_adv(from_state, adj_list)
        for tau_state in tau_states:
            if tau_state not in ed_queue:
                ing_queue.append(tau_state)
                ed_queue.append(tau_state)
    return ed_queue


# 获得state的tau迁移状态
def gen_tau_states_adv(state, adj_list):
    tau_states = set()
    # print('adj_list: ', adj_list[state])
    for val in adj_list[state]:
        (state_to, label) = val
        if label == 'tau':
            tau_states.add(state_to)
    return list(tau_states)


# 2.2获得state的传递闭包---------------------------------------------
def gen_tran_closure(state, adj_list):
    # 运行队列和已访问队列
    ing_queue = [state]
    ed_queue = [state]  # 包括state
    # 迭代计算
    while ing_queue:
        from_state = ing_queue.pop(0)
        tran_states = gen_tran_states(from_state, adj_list)
        for tran_state in tran_states:
            if tran_state not in ed_queue:
                ing_queue.append(tran_state)
                ed_queue.append(tran_state)
    return ed_queue


# 获得state的一次迁移状态
def gen_tran_states(state, adj_list):
    tran_states = set()
    for val in adj_list[state]:
        (state_to, label) = val
        tran_states.add(state_to)
    return list(tran_states)


# 获得state的一次迁移的label集
def one_tran_labels(state, adj_list):
    one_trans = set()
    for val in adj_list[state]:
        (state_to, label) = val
        one_trans.add(label)
    return list(one_trans)


# 获取没有back的传递闭包(Note:计算投影网是否是有效的)
def gen_tran_closure_no_back(state, back_trans, lts):
    # 运行队列和已访问队列
    ing_queue = [state]
    ed_queue = [state]  # 包括state
    # 迭代计算
    while ing_queue:
        from_state = ing_queue.pop(0)
        tran_states = gen_tran_states_no_back(from_state, back_trans, lts)
        for tran_state in tran_states:
            if tran_state not in ed_queue:
                ing_queue.append(tran_state)
                ed_queue.append(tran_state)
    return ed_queue


# 获得state的一次迁移状态(Note:排除back变迁)
def gen_tran_states_no_back(state, back_trans, lts):
    tran_states = set()
    start, ends, states, trans = lts.get_infor()
    for tran in trans:
        state_from, label, state_to = tran.get_infor()
        # 排除back变迁
        if label in back_trans:
            continue
        if state_from == state:
            tran_states.add(state_to)
    return list(tran_states)


# 3.最小化lts----------------------------------------------------
def min_lts(lts, flag):

    # ps:先将lts转换为邻接表
    adj_list = lts_to_adjacency_list(lts)

    min_states = []
    min_trans = []

    start, ends, states, trans = lts.get_infor()

    index = 0
    init_min_state_id = '{}{}'.format(flag, index)
    init_closure = gen_tau_closure_adv(start, adj_list)
    init_min_state = MinState(init_min_state_id, init_closure)
    min_states.append(init_min_state)

    names = get_lts_names(lts)

    # 运行队列和已访问队列
    visiting_queue = [init_min_state]
    visited_queue = [init_min_state]
    # 迭代计算
    while visiting_queue:

        from_min_state = visiting_queue.pop(0)
        from_id, from_closure = from_min_state.get_infor()
        print('ing id', from_id, from_closure)

        for name in names:

            reach_states = move(from_closure, name, adj_list)
            if not reach_states:
                continue
            to_closure = set()
            # 获取迁移名字name后的所有状态的tau闭包
            for reach_state in reach_states:
                to_closure = to_closure.union(
                    set(gen_tau_closure_adv(reach_state, adj_list)))
            print('name: ', name, 'from_closure: ', from_closure,
                  'to_closure: ', to_closure)

            # 确定是否生成过
            idf = is_gen_closure(to_closure, visited_queue)
            if idf is None:  # a)to_closure未生成过
                index += 1
                to_id = '{}{}'.format(flag, index)
                to_min_state = MinState(to_id, list(to_closure))
                min_states.append(to_min_state)
                min_tran = Tran(from_id, name, to_id)
                min_trans.append(min_tran)
                visiting_queue.append(to_min_state)
                visited_queue.append(to_min_state)
            else:  # b)to_closure未生成过
                min_tran = Tran(from_id, name, idf)
                min_trans.append(min_tran)

    return LTS(init_min_state, get_min_ends(ends, visited_queue), min_states,
               min_trans)


# 将一个lts转换为邻接表
def lts_to_adjacency_list(lts: LTS):
    adjacency_list = {}
    # ps:先初始化每个节点
    for state in lts.states:
        adjacency_list[state] = []
    for tran in lts.trans:
        state_from, label, state_to = tran.get_infor()
        adjacency_list[state_from].append((state_to, label))
    return adjacency_list


# 获得最小化后的终止标识
def get_min_ends(ends, ed_queue):
    min_ends = []
    # min_state是终止标识,当且仅当其状态中含有终止状态
    for min_state in ed_queue:
        id, states = min_state.get_infor()
        if set(states).intersection(set(ends)):
            min_ends.append(min_state)
    return min_ends


# 判断新状态是否生产过
def is_gen_closure(to_closure, ed_queue):
    for min_state in ed_queue:
        id, states = min_state.get_infor()
        if Counter(states) == Counter(list(to_closure)):
            return id
        else:
            continue
    return None


# 获得得states中每个状态迁移name后的状态集
def move(states, name, adj_list):
    reach_states = set()
    for state in states:
        tran_states = get_tran_states(state, name, adj_list)
        reach_states = reach_states.union(tran_states)
    return list(reach_states)


# 获得state的以name迁移后的状态集
def get_tran_states(state, name, adj_list):
    tran_states = set()
    for val in adj_list[state]:
        (state_to, label) = val
        if label == name:
            tran_states.add(state_to)
    return tran_states


# 获得lst中所有名字集(排除tau)
def get_lts_names(lts):
    names = set()
    start, ends, states, trans = lts.get_infor()
    for tran in trans:
        state_from, label, state_to = tran.get_infor()
        if label != 'tau':
            names.add(label)
    return list(names)


# 4.同步组合lts----------------------------------------------------
def lts_compose(lts_list):

    # 初始组合状态
    comp_start = []
    for lts in lts_list:
        start, ends, states, trans = lts.get_infor()
        comp_start.append(start)

    comp_trans = []  # 产生的组合变迁集
    inner_name_map, inter_name_map = divide_names(lts_list)

    # 运行队列和已访问队列
    visiting_queue = [comp_start]
    visited_queue = [comp_start]

    # 迭代计算
    while visiting_queue:

        comp_state = visiting_queue.pop(0)
        sync_tran_set = []
        for i, state in enumerate(comp_state):
            succ_trans = get_succ_trans(state, lts_list[i])
            # for succ_tran in succ_trans:
            #     print('succ', succ_tran.get_infor())
            sync_trans_list = []
            for succ_tran in succ_trans:
                state_from, label, state_to = succ_tran.get_infor()
                print('succ_tran:', state_from, label, state_to)
                # succ_tran为内部变迁
                if label in inner_name_map.keys():
                    # 深度拷贝生成后继组合状态
                    succ_comp_state = copy.deepcopy(comp_state)
                    succ_comp_state[i] = state_to
                    comp_trans.append(
                        Tran(str(comp_state), label, str(succ_comp_state)))
                    # 添加未访问的状态
                    if succ_comp_state not in visited_queue:
                        # print('async succ_comp_state: ', succ_comp_state)
                        visiting_queue.append(succ_comp_state)
                        visited_queue.append(succ_comp_state)
                else:  # succ_tran为同步交互变迁
                    sync_trans_list.append(succ_tran)

            sync_tran_set.append(sync_trans_list)

        null_size = 0
        for i, succ_tran in enumerate(sync_tran_set):
            if not succ_tran:
                # 若为空,则添加-1占位用于使用笛卡尔积计算同步组合
                sync_tran_set[i].append(-1)
                null_size += 1

        # ps: 跳过sync_tran_set=[list1,list2,...]中每个同步变迁集均为空的情况
        if null_size == len(sync_tran_set):
            continue

        # 计算每个组织剩余活动(排除内部活动)的笛卡尔积
        for tran_list in itertools.product(*sync_tran_set):
            if is_sync_trans(tran_list, inter_name_map):
                # 深度拷贝生成后继组合状态
                succ_comp_state = copy.deepcopy(comp_state)
                for i, tran in enumerate(tran_list):
                    # 跳过当前没有同步迁移的组织
                    if tran == -1:
                        continue
                    # 同时迁移每个组织的同步活动
                    state_from, label, state_to = tran.get_infor()
                    succ_comp_state[i] = state_to
                # 添加未访问的状态
                if succ_comp_state not in visited_queue:
                    # print('sync succ_comp_state: ', succ_comp_state)
                    visiting_queue.append(succ_comp_state)
                    visited_queue.append(succ_comp_state)
                comp_trans.append(
                    Tran(str(comp_state), label, str(succ_comp_state)))

    # 组合终止状态
    comp_ends = []
    for comp_state in visited_queue:
        if is_comp_ends(comp_state, lts_list):
            comp_ends.append(comp_state)

    # ps:状态转为字符串形式
    return LTS(str(comp_start), [str(comp_end) for comp_end in comp_ends],
               [str(state) for state in visited_queue], comp_trans)


# 判断组合状态是否为终止状态
def is_comp_ends(comp_state, lts_list):
    for i, state in enumerate(comp_state):
        start, ends, states, trans = lts_list[i].get_infor()
        if state not in ends:
            return False
        else:
            continue
    return True


# 判断来自多个组织的变迁集是否能够同步
def is_sync_trans(tran_list, inter_name_map):
    names = set()
    index_list = []
    for i, tran in enumerate(tran_list):
        if tran == -1:
            continue
        state_from, label, state_to = tran.get_infor()
        names.add(label)
        index_list.append(i)
    # 1)同步变迁集中每个变迁的名字相同
    if len(names) == 1:
        # 2)每个具有同步名字的组织参与同步
        name = list(names)[0]
        if inter_name_map[name] == index_list:
            return True
    return False


# 将名字划分为内部名字映射和交互名字映射
def divide_names(lts_list):
    names = get_all_names(lts_list)
    # 内部名字映射
    inner_name_map = {}
    # 交互名字映射
    inter_name_map = {}
    for name in names:
        name_index = get_name_index(name, lts_list)
        if len(name_index) < 2:  # 内部名字
            inner_name_map[name] = name_index
        else:  # 交互名字
            inter_name_map[name] = name_index
    return inner_name_map, inter_name_map


# 获取名字的位置
def get_name_index(name, lts_list):
    name_index = []
    for i, lts in enumerate(lts_list):
        labels = lts.get_labels()
        if name in labels:
            name_index.append(i)
    return name_index


# 获取组合lts中所有的名字集(不重复)
def get_all_names(lts_list):
    names = set()
    for lts in lts_list:
        labels = lts.get_labels()
        names = names.union(set(labels))
    return list(names)


# 获取state后继变迁集
def get_succ_trans(state, lts):
    succ_trans = []
    start, ends, states, trans = lts.get_infor()
    for tran in trans:
        state_from, label, state_to = tran.get_infor()
        if state_from == state:
            succ_trans.append(tran)
    return succ_trans


# 5.获取同步组合lts的一次迁移集-----------------------------------------------
def succ_trans(comp_state, lts_list):

    comp_trans = []  # 产生的组合变迁集
    inner_name_map, inter_name_map = divide_names(lts_list)

    sync_tran_set = []
    for i, state in enumerate(comp_state):
        succ_trans = get_succ_trans(state, lts_list[i])
        # for succ_tran in succ_trans:
        #     print('succ', succ_tran.get_infor())
        sync_tran_list = []
        for succ_tran in succ_trans:
            state_from, label, state_to = succ_tran.get_infor()
            # print('succ_tran:', state_from, label, state_to)
            # succ_tran为内部变迁
            if label in inner_name_map.keys():
                # 深度拷贝生成后继组合状态
                succ_comp_state = copy.deepcopy(comp_state)
                succ_comp_state[i] = state_to
                comp_trans.append(Tran(comp_state, label, succ_comp_state))
            else:  # succ_tran为交互变迁
                sync_tran_list.append(succ_tran)

        sync_tran_set.append(sync_tran_list)

    null_size = 0
    for i, succ_tran in enumerate(sync_tran_set):
        if not succ_tran:
            # 若为空,则添加-1占位用于使用笛卡尔积计算同步组合
            sync_tran_set[i].append(-1)
            null_size += 1

    # ps: 需要跳过sync_tran_set中每个同步变迁全部为空的情况
    if null_size != len(sync_tran_set):
        # 计算每个组织剩余活动(排除内部活动)的笛卡尔积
        for tran_list in itertools.product(*sync_tran_set):
            if is_sync_trans(tran_list, inter_name_map):
                # 深度拷贝生成后继组合状态
                succ_comp_state = copy.deepcopy(comp_state)
                for i, tran in enumerate(tran_list):
                    # 跳过当前没有同步迁移的组织
                    if tran == -1:
                        continue
                    # 同时迁移每个组织的同步活动
                    state_from, label, state_to = tran.get_infor()
                    succ_comp_state[i] = state_to
                comp_trans.append(Tran(comp_state, label, succ_comp_state))

    # for comp_tran in comp_trans:
    #     print('comp_tran:', comp_tran.state_from, '---', comp_tran.label,
    #           '--->', comp_tran.state_to)

    return comp_trans


# 6.重命名函数-------------------------------------------------------------
def rename_tau(input_lts: LTS):
    lts = copy.deepcopy(input_lts)
    for tran in lts.trans:
        from1, label, to = tran.get_infor()
        # label的格式:[0/1, msg, {org1, org2}]: t 或者 tau: t
        inter_tran = [elem.strip() for elem in label.split(':')]
        if inter_tran[0] == 'tau':
            tran.label = 'tau'
        else:
            tran.label = inter_tran[0]
    return lts


# 7.基于给定的状态集从lts中构建一个新的lts---------------------------------
def build_lts_from_states(states, lts):
    new_trans = []
    for tran in lts.trans:
        from1, label, to = tran.get_infor()
        if from1 in states and to in states:
            new_trans.append(tran)
    return LTS(lts.start, lts.ends, states, new_trans)


# 忽视迁移上的交互而只保留变迁
def build_lts_from_states_without_interactions(states, lts):
    new_trans = []
    for tran in lts.trans:
        from1, label, to = tran.get_infor()
        if from1 in states and to in states:
            new_label = label.split(':')[1].strip()
            # 移除变迁后面的'/',变迁可能为T1/2等
            new_label = new_label.split('/')[0]
            tran.label = new_label
            new_trans.append(tran)
    return LTS(lts.start, lts.ends, states, new_trans)


# 8.基于传递闭包确定一个lts中所有合法状态(即能够到达终止状态)------------------
def get_legal_states(lts: LTS):
    legal_states = []
    adj_list = lts_to_adjacency_list(lts)
    for state in lts.states:
        tran_closure = gen_tran_closure(state, adj_list)
        if set(tran_closure) & set(lts.ends):
            legal_states.append(state)
    return legal_states


def get_legal_states_by_ends(ends, states, lts: LTS):
    legal_states = []
    adj_list = lts_to_adjacency_list(lts)
    for state in states:
        tran_closure = gen_tran_closure(state, adj_list)
        if set(tran_closure) & set(ends):
            legal_states.append(state)
    return legal_states


# -------------------------------测试---------------------------------#

if __name__ == '__main__':

    # tran1 = Tran('0', 'tau', '1')
    # tran2 = Tran('0', 'tau', '7')
    # tran3 = Tran('1', 'tau', '2')
    # tran4 = Tran('1', 'tau', '4')
    # tran5 = Tran('2', 'a', '3')
    # tran6 = Tran('4', 'b', '5')
    # tran7 = Tran('3', 'tau', '6')
    # tran8 = Tran('5', 'tau', '6')
    # tran9 = Tran('6', 'tau', '1')
    # tran10 = Tran('6', 'tau', '7')
    # tran11 = Tran('7', 'a', '8')
    # tran12 = Tran('8', 'b', '9')

    # lts = LTS('0', ['9'], ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], [
    #     tran1, tran2, tran3, tran4, tran5, tran6, tran7, tran8, tran9, tran10,
    #     tran11, tran12
    # ])

    # tran1 = Tran('S0', 'tau', 'S1')
    # tran2 = Tran('S0', 'tau', 'S2')
    # tran3 = Tran('S1', 'a', 'S3')
    # tran4 = Tran('S2', 'b', 'S3')
    # tran5 = Tran('S1', 'c', 'S1')

    # lts = LTS('S0', ['S3'], ['S0', 'S1', 'S2', 'S3'],
    #           [tran1, tran2, tran3, tran4, tran5])

    # min_lts = min_lts(lts, 1)
    # start, ends, states, trans = min_lts.get_infor()
    # for tran in trans:
    #     state_from, label, state_to = tran.get_infor()
    #     print(state_from, label, state_to)

    tran1 = Tran('S0', 'a', 'S1')
    lts1 = LTS('S0', ['S1'], ['S0', 'S1'], [tran1])

    tran2 = Tran('S2', 'a', 'S3')
    tran3 = Tran('S2', 'a', 'S4')
    tran4 = Tran('S4', 'b', 'S3')
    tran5 = Tran('S3', 'c', 'S5')
    lts2 = LTS('S2', ['S5'], ['S2', 'S3', 'S4', 'S5'],
               [tran2, tran3, tran4, tran5])
    lts2.lts_to_dot()

    tran6 = Tran('S6', 'd', 'S7')
    tran7 = Tran('S7', 'a', 'S8')
    lts3 = LTS('S6', ['S8'], ['S6', 'S7', 'S8'], [tran6, tran7])

    lts = lts_compose([lts1, lts2, lts3])
    start, ends, states, trans = lts.get_infor()
    print(start, ends, states)
    for tran in trans:
        print(tran.get_infor())

    comp_trans = succ_trans(['S0', 'S2'], [lts1, lts2])

    # trans = [Tran('S1', 'a', 'S2'), Tran('S1', 'b', 'S2')]
    # for tran in trans:
    #     print(tran.get_infor())

# -------------------------------------------------------------------#
