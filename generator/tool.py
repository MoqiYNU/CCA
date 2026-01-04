# coding=gbk

import lts_utils as lu
import chor_utils as chu
import cbp_utils as cbpu
import net as nt
from lts import LTS, Tran
import pandas as pd


# 1.产生抽象核-------------------------------------------------
def gen_abstract_kernel_adv(CHOR_lts, nets, views, hide_trans):

    view_comp = cbpu.get_compose_net(views)
    msg_places = view_comp.msg_places

    # 获取view_comp中每个发送消息变迁/同步变迁对应的交互
    interaction_map = cbpu.get_tran_interaction_map(views, view_comp)

    source, sinks = view_comp.get_start_ends()

    # 查找产生交互的变迁集
    send_trans, sync_trans = cbpu.find_send_and_sync_trans(view_comp)

    # 匹配中产生的状态集和变迁集
    gen_markings = []
    gen_trans = []

    adjacency_list_chor = lu.lts_to_adjacency_list(CHOR_lts)
    # 计算source的传递闭包
    marking_in_closure, trans_in_closure = cbpu.gen_tau_closure(
        source, view_comp, send_trans, sync_trans)
    # 添加source的传递闭包中状态和迁移到gen_markings和gen_trans
    for marking in marking_in_closure:
        if not nt.marking_is_exist(marking, gen_markings):
            gen_markings.append(marking)
    gen_trans.extend(trans_in_closure)

    visiting_queue = [(CHOR_lts.start, marking_in_closure)]
    visited_queue = [(CHOR_lts.start, marking_in_closure)]

    while visiting_queue:

        chor_state, impl_markings = visiting_queue.pop(0)

        for chor_elem in adjacency_list_chor[chor_state]:
            chor_state_to, chor_label = chor_elem
            match_trans, to_markings = match_an_interaction(
                impl_markings, chor_label, views, view_comp, send_trans,
                sync_trans, interaction_map)

            print('match_trans: ', chor_label, match_trans)

            if not match_trans:  # 1)不存在匹配的交互

                # 抛出一个异常
                raise Exception(
                    'impl cannot realize chor as no matching interactions, exit!'
                )

            else:  # 2)存在匹配的交互

                # 添加新产生的匹配变迁
                gen_trans.extend(match_trans)

                # 计算to_markings的传递闭包
                to_markings_closure = []
                for marking in to_markings:
                    closure_markings, closure_trans = cbpu.gen_tau_closure(
                        marking, view_comp, send_trans, sync_trans)
                    for marking in closure_markings:
                        if not nt.marking_is_exist(marking,
                                                   to_markings_closure):
                            to_markings_closure.append(marking)
                    gen_trans.extend(closure_trans)
                # 添加新产生的标识
                for marking in to_markings_closure:
                    if not nt.marking_is_exist(marking, gen_markings):
                        gen_markings.append(marking)

                # 特别注意~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~(START)

                # 1)chor到达终止状态但是impl没有到达则抛出异常
                if chor_state_to in CHOR_lts.ends and not in_sinks(
                        to_markings_closure, sinks, msg_places):
                    for to_marking in to_markings_closure:
                        print('to_markings: ', chor_state_to,
                              to_marking.get_infor())
                    raise Exception(
                        'impl cannot realize chor as no matching ends, exit!')

                # 2)impl到达终止状态但是chor没有到达则移除闭包中的终止状态
                if chor_state_to not in CHOR_lts.ends and in_sinks(
                        to_markings_closure, sinks, msg_places):
                    to_markings_closure_new = []
                    for marking in to_markings_closure:
                        # ps:终止标识中可以含有消息
                        inter_places = [
                            place for place in marking.get_infor()
                            if place not in msg_places
                        ]
                        new_marking = nt.Marking(inter_places)
                        if not nt.marking_is_exist(new_marking, sinks):
                            to_markings_closure_new.append(marking)
                    to_markings_closure = to_markings_closure_new

                    # 特别注意~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~(END)

                    # 测试打印
                    for to_marking in to_markings_closure_new:
                        print('to_markings: ', chor_state_to,
                              to_marking.get_infor())

                if not visited(
                    (chor_state_to, to_markings_closure), visited_queue):
                    visited_queue.append((chor_state_to, to_markings_closure))
                    visiting_queue.append((chor_state_to, to_markings_closure))

    impl_rg = LTS(source, sinks, gen_markings, gen_trans)
    impl_lts, marking_map = impl_rg.rg_to_lts()

    # ps:先移除变迁后面的'/',变迁可能为T1/2等
    lst = []
    for tran in impl_lts.trans:
        from1, label, to = tran.get_infor()
        lst.append([from1, label.split('/')[0], to])
    # 再移除重复的迁移
    df = pd.DataFrame(lst)
    df = df.drop_duplicates()
    impl_trans = [
        Tran(from1, label, to) for from1, label, to in df.values.tolist()
    ]
    impl_lts.trans = impl_trans

    # 显示impl_lts
    impl_lts.lts_to_dot_name('impl_lts')

    # 获取impl_lts中那些能够到达ends的状态
    legal_states = lu.get_legal_states_by_ends(impl_lts.ends, impl_lts.states,
                                               impl_lts)
    # 移除impl_lts中不合法状态及其形成的边
    abstract_kernel = lu.build_lts_from_states(legal_states, impl_lts)

    abstract_kernel.lts_to_dot_name('abstract_kernel')

    # print('gen_markings: ', len(gen_markings))

    return nets, hide_trans, view_comp, marking_map, abstract_kernel


# 匹配一个交互
def match_an_interaction(from_markings, interaction, views, view_comp,
                         send_trans, sync_trans, interaction_map):
    to_markings = []
    match_trans = []
    for marking in from_markings:
        enable_trans = nt.get_enable_trans(view_comp, marking)
        for enable_tran in enable_trans:
            # 跳过非发送变迁和同步变迁
            if enable_tran not in set(send_trans + sync_trans):
                continue
            if enable_tran in send_trans:  #异步变迁不需要排序
                inter_str = '[' + interaction_map[enable_tran][
                    0] + ', ' + interaction_map[enable_tran][
                        1] + ', ' + '{' + ', '.join(
                            interaction_map[enable_tran][2]) + '}' + ']'
            else:  # ps:同步变迁需要排序
                inter_str = '[' + interaction_map[enable_tran][
                    0] + ', ' + interaction_map[enable_tran][
                        1] + ', ' + '{' + ', '.join(
                            sorted(
                                interaction_map[enable_tran][2])) + '}' + ']'
            print('inter_str: ', inter_str)
            # 匹配交互
            if inter_str == interaction:
                preset = nt.get_preset(view_comp.get_flows(), enable_tran)
                postset = nt.get_postset(view_comp.get_flows(), enable_tran)
                succ_marking = nt.succ_marking(marking.get_infor(), preset,
                                               postset)
                if not nt.marking_is_exist(succ_marking, to_markings):
                    to_markings.append(succ_marking)
                tran = Tran(marking, enable_tran, succ_marking)
                match_trans.append(tran)
    return match_trans, to_markings


# markings中某个标识在sinks中
def in_sinks(markings, sinks, msg_places):
    for marking in markings:
        # ps:终止标识中可以含有消息
        inter_places = [
            place for place in marking.get_infor() if place not in msg_places
        ]
        new_marking = nt.Marking(inter_places)
        if nt.marking_is_exist(new_marking, sinks):
            return True
    return False


# 判断elem是否在queue中
def visited(elem, queue):
    for item in queue:
        if item[0] == elem[0] and nt.equal_marking_sets(item[1], elem[1]):
            return True
    return False


# 2.获取核中不稳定任务集-----------------------------------------------------
def get_unstable_tasks(comp_net: nt.OpenNet, marking_map,
                       abstract_kernel: LTS):
    adjacency_list = lu.lts_to_adjacency_list(abstract_kernel)
    unstable_tasks = set()
    for state in abstract_kernel.states:
        # 获取state对应的标识
        marking = marking_map[state]
        enable_trans = nt.get_enable_trans(comp_net, marking)
        # 基于adjacency_list获取从state出发的迁移标号
        driver_trans = [label for (state_to, label) in adjacency_list[state]]
        unstable_tasks = unstable_tasks.union(
            set(enable_trans).difference(driver_trans))
    return list(unstable_tasks)


# 3.产生每个参与组织对应的协调者-----------------------------------------------
def gen_CDs(nets, abstract_kernel: LTS, unstable_tasks):
    hide_kernels = get_hide_kernels(nets, abstract_kernel, unstable_tasks)
    CDs = []
    for i, net in enumerate(nets):
        CD_trans = []
        hide_kernel = hide_kernels[i]
        hide_kernel.lts_to_dot_name('hide_core{}'.format(i))
        min_hide_kernel = lu.min_lts(hide_kernel, i)
        # 显示min_hide_kernel
        min_hide_kernel_show = LTS(
            min_hide_kernel.start.id, [end.id for end in min_hide_kernel.ends],
            [state.id
             for state in min_hide_kernel.states], min_hide_kernel.trans)
        min_hide_kernel_show.lts_to_dot_name('min_hide_core{}'.format(i))
        index = 0
        for tran in min_hide_kernel.trans:
            state_from, label, state_to = tran.get_infor()
            if label not in net.trans and label in unstable_tasks:
                cood_state = 'CS{}{}'.format(i, index)
                index += 1
                temp_tran1 = Tran(state_from, 'SYNC_1_{}'.format(label),
                                  cood_state)
                temp_tran2 = Tran(cood_state, 'SYNC_2_{}'.format(label),
                                  state_to)
                CD_trans.append(temp_tran1)
                CD_trans.append(temp_tran2)
            elif label in net.trans and label in unstable_tasks:
                cood_state1 = 'CS{}{}'.format(i, index)
                index += 1
                temp_tran1 = Tran(state_from, 'SYNC_1_{}'.format(label),
                                  cood_state1)
                cood_state2 = 'CS{}{}'.format(i, index)
                index += 1
                temp_tran2 = Tran(cood_state1, label, cood_state2)
                temp_tran3 = Tran(cood_state2, 'SYNC_2_{}'.format(label),
                                  state_to)
                CD_trans.append(temp_tran1)
                CD_trans.append(temp_tran2)
                CD_trans.append(temp_tran3)
            else:
                CD_trans.append(tran)
        CD_states = []
        for CD_tran in CD_trans:
            state_from, label, state_to = CD_tran.get_infor()
            if state_from not in CD_states:
                CD_states.append(state_from)
            if state_to not in CD_states:
                CD_states.append(state_to)
        CD = LTS(min_hide_kernel.start, min_hide_kernel.ends, CD_states,
                 CD_trans)
        # 将CD状态都以id标识
        CD.start = CD.start.id
        CD.ends = [t.get_infor()[0] for t in CD.ends]
        # print('CD.ends:', CD.ends)
        CDs.append(CD)
    return CDs


# 获取隐藏核(即将每个网中除自身变迁集及不稳定任务外的变迁隐藏)
# ps:每个状态都是字符串而不是marking
def get_hide_kernels(nets, kernel: LTS, unstable_tasks):
    hide_kernels = []
    start = kernel.start
    ends = kernel.ends
    states = kernel.states
    trans = kernel.trans
    for net in nets:
        # 不稳定任务和自身变迁不能隐藏
        visual_names = unstable_tasks + net.trans
        print('visual_names:', visual_names)
        hide_core_trans = []
        for tran in trans:
            state_from, label, state_to = tran.get_infor()
            if label in visual_names:
                hide_core_trans.append(tran)
            else:
                # 需要考虑同步变迁情形
                ls = label.split('_')
                if len(ls) > 1:
                    if set(ls) & set(net.trans):
                        hide_core_trans.append(tran)
                    else:
                        hide_core_trans.append(
                            Tran(state_from, 'tau', state_to))
                else:
                    hide_core_trans.append(Tran(state_from, 'tau', state_to))
        hide_kernel = LTS(start, ends, states, hide_core_trans)
        hide_kernels.append(hide_kernel)
    return hide_kernels


# 4.产生组合行为---------------------------------------------
def gen_compose_behavior(comp_net, CDs, hide_trans):

    gen_markings = []
    gen_trans = []
    comp_trans = []

    # 构建初始组合状态
    init_marking = comp_net.source
    gen_markings.append(init_marking)
    init_CD_state = []
    for lts in CDs:
        start, ends, states, trans = lts.get_infor()
        init_CD_state.append(start)
    init_comp_state = [init_marking, init_CD_state]

    # 运行队列和已访问队列
    visiting_queue = [init_comp_state]
    visited_queue = [init_comp_state]

    # 迭代计算
    while visiting_queue:

        [marking, CD_state] = visiting_queue.pop(0)
        # 组合网使能变迁集和CD中使能的变迁集
        enable_trans = nt.get_enable_trans(comp_net, marking)
        succ_trans = lu.succ_trans(CD_state, CDs)

        # 1)CD独立执行协调变迁
        for succ_tran in succ_trans:
            state_from, label, state_to = succ_tran.get_infor()
            if label.startswith('SYNC_'):
                to_comp_state = [marking, state_to]
                comp_trans.append(
                    Tran([marking, CD_state], label, to_comp_state))
                if not is_visited_comp_state(to_comp_state, visited_queue):
                    visiting_queue.append(to_comp_state)
                    visited_queue.append(to_comp_state)

        # 2)业务过程独立执行,分如下两种情况:
        for enable_tran in enable_trans:
            preset = nt.get_preset(comp_net.get_flows(), enable_tran)
            postset = nt.get_postset(comp_net.get_flows(), enable_tran)
            to_marking = nt.succ_marking(marking.get_infor(), preset, postset)
            # 2.1)业务过程独立执行隐藏变迁
            if enable_tran in hide_trans:
                print('enable_tran:', enable_tran)
                to_comp_state = [to_marking, CD_state]
                print('to_comp_state: ', to_marking.get_infor(), CD_state)
                # 组合网迫使后生成的标识和变迁
                if not nt.marking_is_exist(to_marking, gen_markings):
                    gen_markings.append(to_marking)
                # 该迁移没有生成过则生成
                if not tran_is_exist(marking, enable_tran, to_marking,
                                     gen_trans):
                    gen_trans.append(Tran(marking, enable_tran, to_marking))
                # 生成的组合行为的标识和变迁
                comp_trans.append(
                    Tran([marking, CD_state], enable_tran, to_comp_state))

                if not is_visited_comp_state(to_comp_state, visited_queue):
                    visiting_queue.append(to_comp_state)
                    visited_queue.append(to_comp_state)
            else:
                # 2.2)业务过程中除隐藏变迁外的其他变迁需要协调执行
                for succ_tran in succ_trans:
                    state_from, label, state_to = succ_tran.get_infor()
                    if label == enable_tran:
                        to_comp_state = [to_marking, state_to]
                        # 组合网迫使后生成的标识和变迁
                        if not nt.marking_is_exist(to_marking, gen_markings):
                            gen_markings.append(to_marking)
                        # 该迁移没有生成过则生成
                        if not tran_is_exist(marking, label, to_marking,
                                             gen_trans):
                            gen_trans.append(Tran(marking, label, to_marking))
                        # 生成的组合行为的标识和变迁
                        comp_trans.append(
                            Tran([marking, CD_state], label, to_comp_state))
                        if not is_visited_comp_state(to_comp_state,
                                                     visited_queue):
                            visiting_queue.append(to_comp_state)
                            visited_queue.append(to_comp_state)

    # 终止标识(ps:允许消息库所和资源库所不为空)
    end_markings = []
    for marking in gen_markings:
        if is_end_marking(marking, comp_net):
            end_markings.append(marking)
    gen_behavior = LTS(init_marking, end_markings, gen_markings, gen_trans)

    # 组合终止状态
    comp_ends = []
    for comp_state in visited_queue:
        if is_end_marking(comp_state[0], comp_net) and lu.is_comp_ends(
                comp_state[1], CDs):
            print('ends:', comp_state[0].get_infor(), comp_state[1])
            comp_ends.append(comp_state)
    comp_behavior = LTS(init_comp_state, comp_ends, visited_queue, comp_trans)

    return gen_behavior, comp_behavior


def is_visited_comp_state(comp_state, visited_queue):
    for temp_comp_state in visited_queue:
        if nt.equal_markings(
                temp_comp_state[0],
                comp_state[0]) and temp_comp_state[1] == comp_state[1]:
            return True
    return False


# 判断组合标识是否为终止标识(容许消息和资源存在)
def is_end_marking(marking, comp_net):
    msg_places = comp_net.msg_places
    res_places = comp_net.res_places
    places = marking.get_infor()
    inter_places = [
        place for place in places
        if place not in msg_places and place not in res_places
    ]
    new_marking = nt.Marking(inter_places)
    if nt.marking_is_exist(new_marking, comp_net.sinks):
        return True
    return False


# 判断迁移是否存在
def tran_is_exist(marking, label, to_marking, gen_trans):
    for tran in gen_trans:
        if nt.equal_markings(
                tran.state_from,
                marking) and tran.label == label and nt.equal_markings(
                    tran.state_to, to_marking):
            return True
    return False


# -------------------------------测试---------------------------------#

if __name__ == '__main__':

    chor_path = '/Users/moqi/Desktop/CHOR.xml'
    impl_path = '/Users/moqi/Desktop/IMPL.xml'

    CHOR_lts = chu.gen_CHOR_ig_lts(chor_path)
    nets, views, hide_trans = cbpu.gen_views(impl_path)

    print('hide_trans: ', hide_trans)

    nets, hide_trans, view_comp, marking_map, abstract_kernel = gen_abstract_kernel_adv(
        CHOR_lts, nets, views, hide_trans)
    unstable_tasks = get_unstable_tasks(view_comp, marking_map,
                                        abstract_kernel)
    print('unstable_tasks: ', unstable_tasks)
    CDs = gen_CDs(nets, abstract_kernel, unstable_tasks)
    for i, CD in enumerate(CDs):
        CD.lts_to_dot_name('CD{}'.format(i))

    # ps:针对的是业务过程组合的协调
    comp_net = cbpu.get_compose_net(nets)
    gen_behavior, comp_behavior = gen_compose_behavior(comp_net, CDs,
                                                       hide_trans)

    gen_behavior_lts, marking_map = gen_behavior.rg_to_lts()
    gen_behavior_lts.lts_to_dot_name('gen_behavior')

    comp_behavior_lts, comp_state_map = comp_behavior.comp_to_lts()
    comp_behavior_lts.lts_to_dot_name('comp_behavior')

# -------------------------------------------------------------------#
