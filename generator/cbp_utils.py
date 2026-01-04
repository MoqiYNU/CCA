# coding=gbk

from copy import deepcopy
import copy
import net as nt
import net_gen as ng
from lts import LTS, Tran
import net_utils as nu
from net import Flow, Marking, OpenNet
import branch_bisim as bb


# 1.获取组织业务过程的内网---------------------------------------------
def get_inner_net(net: nt.OpenNet):

    source, sinks = net.get_start_ends()
    for sink in sinks:
        print('sinks: ', sink.get_infor())
    places, inner_places, msg_places = net.get_places()
    trans, rout_trans, label_map = net.get_trans()
    flows = net.get_flows()

    # 确定流集
    inner_net_flows = []
    for flow in flows:
        flow_from, flow_to = flow.get_infor()
        # 移除消息流
        if flow_from in msg_places or flow_to in msg_places:
            continue
        inner_net_flows.append(flow)

    # 确定库所
    inner_net_places = set()
    for flow in inner_net_flows:
        flow_from, flow_to = flow.get_infor()
        if flow_from in places:
            inner_net_places.add(flow_from)
        if flow_to in places:
            inner_net_places.add(flow_to)

    inner_net = nt.OpenNet(
        source,
        sinks,
        list(inner_net_places),
        trans,
        {},  # 变迁标号映射设为空
        inner_net_flows,
    )
    # 打印内网
    # inner_net.net_to_dot('inner_net', False)
    return inner_net


# 2.获取任意业务过程的公共视图-------------------------------------------


# 获取每个组织的视图
def gen_views(impl_path):
    nets = ng.gen_nets(impl_path)
    # 生成每个net的视图(ps:每个视图中库所为a1, a2, ...)
    hide_trans = set()
    str = 'abcdefghijklmnopqrstuvwxyz'
    views = []
    for i, net in enumerate(nets):
        view, ignore_trans = gen_view(net, str[i])
        hide_trans.update(ignore_trans)
        views.append(view)
        view.net_to_dot('view_{}'.format(i), False)
    return nets, views, hide_trans


# 从多个实现中获取视图(每个实现中包含若干业务过程,每个业务过程对应一个视图)
def gen_views_by_joining_IMPLs(impl_paths):
    nets = []
    views = []
    hide_trans = set()
    for i, impl_path in enumerate(impl_paths):
        nets_i = ng.gen_nets(impl_path)
        nets_i_copy = deepcopy(nets_i)
        # ps:重命名网(e.g. p1 -> p1(1)), flag形如'(1),(2),...'
        index = '({})'.format(i)
        for net in nets_i:
            # ps:重命名编排
            net.rename_net(index)
            nets.append(net)
        # 生成每个net的视图(ps:每个视图中库所为a1, a2, ...)
        str = 'abcdefghijklmnopqrstuvwxyz'
        for j, net in enumerate(nets_i_copy):
            view, ignore_trans = gen_view(net, str[j])
            for tran in ignore_trans:
                new_tran = tran + index
                hide_trans.add(new_tran)
            view.rename_net(index)
            views.append(view)
            # view.net_to_dot('view_{}{}'.format(i, j), False)
    return nets, views, hide_trans


def gen_views_by_joining_IMPLs_adv(impl_paths):
    nets = []
    views = []
    hide_trans = set()
    for i, impl_path in enumerate(impl_paths):
        if i == 0:  # 第一个实现中的网不需要处理
            nets_i = ng.gen_nets(impl_path)
            nets_i_copy = deepcopy(nets_i)
        else:  # 后续实现中的网需要处理,即它的终止标识中需包括初始标识
            nets_i = ng.gen_nets(impl_path)
            for net in nets_i:
                net.sinks = net.sinks + [net.source]
            nets_i_copy = deepcopy(nets_i)
        # ps:重命名网(e.g. p1 -> p1(1)), flag形如'(1),(2),...'
        index = '({})'.format(i)
        for net in nets_i:
            # ps:重命名编排
            net.rename_net(index)
            nets.append(net)
        # 生成每个net的视图(ps:每个视图中库所为a1, a2, ...)
        str = 'abcdefghijklmnopqrstuvwxyz'
        for j, net in enumerate(nets_i_copy):
            view, ignore_trans = gen_view(net, str[j])
            for tran in ignore_trans:
                new_tran = tran + index
                hide_trans.add(new_tran)
            view.rename_net(index)
            views.append(view)
            # view.net_to_dot('view_{}{}'.format(i, j), False)
    return nets, views, hide_trans


def gen_view(net: nt.OpenNet, flag):

    # a)获取内网,并生成其可达图对应lts
    inner_net = get_inner_net(net)
    rg = nu.gen_rg(inner_net)
    lts, map = rg.rg_to_lts()
    # 打印lts
    lts.lts_to_dot()

    # b)利用分支互模拟最小化lts
    msg_flows = []  # 消息流
    comm_trans = set()  # 通信变迁(同步+异步)
    for flow in net.flows:
        flow_from, flow_to = flow.get_infor()
        # 异步情况
        if flow_from in net.msg_places:
            msg_flows.append(flow)
            comm_trans.add(flow_to)
        if flow_to in net.msg_places:
            msg_flows.append(flow)
            comm_trans.add(flow_from)
        # 同步情况(标号以'SYNC_'开头)
        if flow_from in net.places:
            if net.label_map[flow_to].startswith('SYNC_'):
                comm_trans.add(flow_to)
        if flow_to in net.places:
            if net.label_map[flow_from].startswith('SYNC_'):
                comm_trans.add(flow_from)

    print("通信变迁:", comm_trans)

    # ps:状态是用数字标识且迁移中状态也是数字标识
    states = [int(s[1:]) for s in lts.states]
    temp_transitions = []
    for tran in lts.trans:
        s, a, t = tran.get_infor()
        temp_transitions.append((int(s[1:]), a, int(t[1:])))
    # 这里将非通信变迁设为tau
    transitions = []
    for tran in lts.trans:
        s, a, t = tran.get_infor()
        if a in comm_trans:
            transitions.append((int(s[1:]), a, int(t[1:])))
        else:
            transitions.append((int(s[1:]), 'tau', int(t[1:])))

    for s, a, t in transitions:
        print('trans in ts: ', s, a, t)

    # 运行分支互模拟最小化算法

    ts = bb.build_lts_from_data(states, transitions)
    minimized_partition = bb.compute_branching_bisimilarity(ts)

    # 打印结果
    print("最小化后的划分:")
    for i, block in enumerate(minimized_partition):
        print(f"等价类 {i+1}: {sorted(block)}")

    # 构建最小化后lts中每个块对应的状态
    min_states = [
        'bl{}'.format(i) for i, block in enumerate(minimized_partition)
    ]
    # 构建最小化lts的迁移
    min_trans = []
    ignore_trans = set()  # net中隐藏的变迁集
    rest_trans = set()  # net中剩余的变迁集
    for tran in temp_transitions:
        s, a, t = tran
        # 获取s在minimized_partition中的等价类对应块索引
        s_block = get_block_index(s, minimized_partition)
        t_block = get_block_index(t, minimized_partition)
        if s_block == t_block:  # ps:在同一个块中则忽略
            ignore_trans.add(a)
            continue
        min_trans.append(
            Tran('bl{}'.format(s_block), a, 'bl{}'.format(t_block)))
        rest_trans.add(a)
    rest_trans = list(rest_trans)
    print("剩余变迁:", ignore_trans, rest_trans)
    # lts中初始状态的索引为0
    min_start = 'bl{}'.format(get_block_index(0, minimized_partition))
    end_nums = []
    for end in lts.ends:
        end_num = int(end[1:])
        end_nums.append(end_num)
    min_ends = []
    for i, block in enumerate(minimized_partition):
        # 检查块是否包含任何终止状态
        if any(end in block for end in end_nums):
            min_ends.append('bl{}'.format(i))

    min_lts = LTS(min_start, min_ends, min_states, min_trans)
    min_lts.lts_to_dot_name('min_lts')

    # c)根据最小化的lts生成开放网

    # [2:]表示取块状态编号部分
    source = nt.Marking(['{}{}'.format(flag, min_start[2:])])
    sinks = [nt.Marking(['{}{}'.format(flag, end[2:])]) for end in min_ends]
    places = ['{}{}'.format(flag, state[2:]) for state in min_states]
    inner_places = copy.deepcopy(places)
    places = places + net.msg_places  # 需要包含消息库所
    # ps:变迁id不能重复但是lts中可以重复出现
    rest_trans_counter = [0 for i in range(len(rest_trans))]
    flows = []
    trans = []
    for tran in min_trans:
        s, a, t = tran.get_infor()
        index = rest_trans.index(a)
        val = rest_trans_counter[index]
        # 变迁id不能重复但是lts中可以重复出现
        a = a if val == 0 else '{}/{}'.format(a, val)
        rest_trans_counter[index] += 1
        trans.append(a)
        flows.append(nt.Flow('{}{}'.format(flag, s[2:]), a))
        flows.append(nt.Flow(a, '{}{}'.format(flag, t[2:])))
    # 增加消息流
    add_msg_flows = []
    for flow in msg_flows:
        flow_from, flow_to = flow.get_infor()
        if flow_from in net.msg_places:
            # 类似T1,T1/1,T1/2,...
            index = rest_trans.index(flow_to)
            counter = rest_trans_counter[index]
            numbers = list(range(0, counter))
            for i in numbers:
                if i == 0:  # 0表示变迁没有加后缀
                    add_msg_flows.append(nt.Flow(flow_from, flow_to))
                else:
                    temp_flow_to = '{}/{}'.format(flow_to, i)
                    add_msg_flows.append(nt.Flow(flow_from, temp_flow_to))
        if flow_to in net.msg_places:
            index = rest_trans.index(flow_from)
            counter = rest_trans_counter[index]
            numbers = list(range(0, counter))
            for i in numbers:
                if i == 0:  # 0表示变迁没有加后缀
                    add_msg_flows.append(nt.Flow(flow_from, flow_to))
                else:
                    temp_flow_from = '{}/{}'.format(flow_from, i)
                    add_msg_flows.append(nt.Flow(temp_flow_from, flow_to))
    flows = flows + add_msg_flows
    label_map = {}
    for tran in trans:
        label_map[tran] = net.label_map[tran.split('/')[0]]

    view = nt.OpenNet(source, sinks, places, trans, label_map, flows)
    view.inner_places = inner_places
    view.msg_places = net.msg_places
    view.role = net.role
    # 打印相关信息
    # view.print_infor()
    # view.net_to_dot('view{}'.format(flag), False)

    return view, ignore_trans


# 获取state在minimized_partition中的等价类对应块索引
def get_block_index(state, minimized_partition):
    for i, block in enumerate(minimized_partition):
        if state in block:
            return i
    return -1


# 3.组合业务过程(同步+异步)-------------------------------------------------------
def get_compose_net(nets):
    # gen_sync_trans为合并过程中的中间同步变迁集
    gen_sync_trans = []
    net = compose_nets(nets, gen_sync_trans)
    print('gen_sync_trans: ', gen_sync_trans)
    net.print_infor()
    return net


# 1.1a.组合bag构建组合网---------------------------------------------
def compose_nets(nets, gen_sync_trans):
    if len(nets) == 0:
        print('no bag_nets exist, exit...')
        return
    if len(nets) == 1:
        return nets[0]
    else:
        net = compose_two_nets(nets[0], nets[1], gen_sync_trans)
        for i in range(2, len(nets)):
            net = compose_two_nets(net, nets[i], gen_sync_trans)
        return net


# 组合两个开放网
def compose_two_nets(net1: OpenNet, net2: OpenNet, gen_sync_trans):

    # 1)产生源和终止标识~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    source1, sinks1 = net1.get_start_ends()
    source2, sinks2 = net2.get_start_ends()
    source = Marking(source1.get_infor() + source2.get_infor())
    sinks = []
    for sink1 in sinks1:
        for sink2 in sinks2:
            sink = Marking(sink1.get_infor() + sink2.get_infor())
            sinks.append(sink)

    # 2)产生库所(不能重复)~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    places1, inner_places1, msg_places1 = net1.get_places()
    places2, inner_places2, msg_places2 = net2.get_places()
    places = list(set(places1 + places2))
    inner_places = list(set(inner_places1 + inner_places2))
    msg_places = list(set(msg_places1 + msg_places2))

    # 3)产生变迁~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    trans1, rout_trans1, tran_label_map1 = net1.get_trans()
    trans2, rout_trans2, tran_label_map2 = net2.get_trans()
    sync_trans1, sync_trans2 = get_sync_trans(net1, net2)
    trans = []
    tran_label_map = {}

    # a)net1和net2中非同步变迁
    for tran1 in trans1:
        if tran1 not in sync_trans1:
            trans.append(tran1)
            tran_label_map[tran1] = tran_label_map1[tran1]
    for tran2 in trans2:
        if tran2 not in sync_trans2:
            trans.append(tran2)
            tran_label_map[tran2] = tran_label_map2[tran2]

    # b)net1和net2中同步变迁(Note:用于生成同步合并变迁,其中一个变迁可以参加多个同步活动)
    syncMap1 = []
    syncMap2 = []

    print('sync_trans: ', sync_trans1, sync_trans2)

    for sync_tran1 in sync_trans1:

        # sync_trans存储net2中与sync_tran1同步(标号相同)的迁移集
        sync_trans_in_net2 = []

        for sync_tran2 in sync_trans2:
            if tran_label_map1[sync_tran1] == tran_label_map2[sync_tran2]:
                sync_trans_in_net2.append(sync_tran2)

        if sync_tran1 in gen_sync_trans:
            gen_sync_trans.remove(sync_tran1)

        for sync_tran in sync_trans_in_net2:

            if sync_tran in gen_sync_trans:
                gen_sync_trans.remove(sync_tran)

            # 同步变迁Id合并:a_b
            gen_sync_tran = sync_tran1 + '_' + sync_tran
            trans.append(gen_sync_tran)
            tran_label_map[gen_sync_tran] = tran_label_map1[sync_tran1]

            gen_sync_trans.append(gen_sync_tran)

            syncMap1.append([sync_tran1, gen_sync_tran])
            syncMap2.append([sync_tran, gen_sync_tran])

    print('gen_sync_trans: ', gen_sync_trans)
    rout_trans = list(set(rout_trans1 + rout_trans2))

    # 5)产生流关系~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    flows = []
    flows1 = net1.get_flows()
    flows2 = net2.get_flows()

    for flow in flows1:

        flow_from, flow_to = flow.get_infor()

        if flow_from in sync_trans1:
            merge_trans = get_merge_trans(flow_from, syncMap1)
            for merge_tran in merge_trans:
                flows.append(Flow(merge_tran, flow_to))
        elif flow_to in sync_trans1:
            merge_trans = get_merge_trans(flow_to, syncMap1)
            for merge_tran in merge_trans:
                flows.append(Flow(flow_from, merge_tran))
        else:
            flows.append(flow)

    for flow in flows2:

        flow_from, flow_to = flow.get_infor()

        if flow_from in sync_trans2:
            merge_trans = get_merge_trans(flow_from, syncMap2)
            for merge_tran in merge_trans:
                flows.append(Flow(merge_tran, flow_to))
        elif flow_to in sync_trans2:
            merge_trans = get_merge_trans(flow_to, syncMap2)
            for merge_tran in merge_trans:
                flows.append(Flow(flow_from, merge_tran))
        else:
            flows.append(flow)

    openNet = OpenNet(source, sinks, places, trans, tran_label_map, flows)
    openNet.inner_places = inner_places
    openNet.msg_places = msg_places
    openNet.rout_trans = rout_trans
    return openNet


# 获取同步中合并迁移集
def get_merge_trans(tran, syncMap):
    merge_trans = []
    for item in syncMap:
        if item[0] == tran:
            merge_trans.append(item[1])
    return merge_trans


# 分别获取net1和net2中同步迁移集
def get_sync_trans(net1, net2):
    sync_trans1 = []
    sync_trans2 = []
    trans1, rout_trans1, label_map1 = net1.get_trans()
    trans2, rout_trans2, label_map2 = net2.get_trans()
    for tran1 in trans1:
        # 排除控制变迁
        if tran1 in rout_trans1:
            continue
        if is_sync_tran(tran1, net1, net2):
            sync_trans1.append(tran1)
    for tran2 in trans2:
        # 排除控制变迁
        if tran2 in rout_trans2:
            continue
        if is_sync_tran(tran2, net2, net1):
            sync_trans2.append(tran2)
    return sync_trans1, sync_trans2


# 判断tran1是不是同步变迁
def is_sync_tran(tran1, net1, net2):
    trans1, rout_trans1, tran_label_map1 = net1.get_trans()
    trans2, rout_trans2, tran_label_map2 = net2.get_trans()
    label1 = tran_label_map1[tran1]
    for tran2 in trans2:
        # 排除控制变迁
        if tran2 in rout_trans2:
            continue
        label2 = tran_label_map2[tran2]
        if label1 == label2:
            return True
    return False


# 4.产生实现交互图-------------------------------------------------
def gen_impl_ig(views):

    view_comp = get_compose_net(views)

    # comp_net.net_to_dot('CN', False)

    # 获取comp_net中每个发送消息变迁/同步变迁对应的交互
    interaction_map = get_tran_interaction_map(views, view_comp)

    source, sinks = view_comp.get_start_ends()
    # 查找产生交互的变迁集
    send_trans, sync_trans = find_send_and_sync_trans(view_comp)

    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    visiting_queue = [source]
    visited_queue = [source]

    while visiting_queue:
        marking = visiting_queue.pop(0)
        enable_trans = nt.get_enable_trans(view_comp, marking)
        for enable_tran in enable_trans:
            # 1)产生后继标识
            preset = nt.get_preset(view_comp.get_flows(), enable_tran)
            postset = nt.get_postset(view_comp.get_flows(), enable_tran)
            succ_marking = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            if enable_tran not in set(send_trans + sync_trans):
                tran = Tran(marking, f'tau: {enable_tran}', succ_marking)
            else:
                if enable_tran in send_trans:  #异步变迁不需要排序
                    inter_str = '[' + interaction_map[enable_tran][
                        0] + ', ' + interaction_map[enable_tran][
                            1] + ', ' + '{' + ', '.join(
                                interaction_map[enable_tran]
                                [2]) + '}' + ']: ' + enable_tran
                else:  # ps:同步变迁需要排序
                    inter_str = '[' + interaction_map[enable_tran][
                        0] + ', ' + interaction_map[enable_tran][
                            1] + ', ' + '{' + ', '.join(
                                sorted(interaction_map[enable_tran]
                                       [2])) + '}' + ']: ' + enable_tran
                tran = Tran(marking, inter_str, succ_marking)
            gen_trans.append(tran)
            if not nt.marking_is_exist(succ_marking, visited_queue):
                visiting_queue.append(succ_marking)
                visited_queue.append(succ_marking)

    return view_comp, LTS(source, sinks, visited_queue, gen_trans)


# 获取每个同步/或异步变迁的交互
def get_tran_interaction_map(views, comp_net):

    #  获取每条变迁对应的组织
    tran_org = {}
    for tran in comp_net.trans:
        print('tran:', tran)
        if '_' in tran:  # 同步变迁需要分解
            sync_trans = tran.split('_')
            for sync_tran in sync_trans:
                for view in views:
                    if sync_tran in view.trans:
                        tran_org[sync_tran] = view.role
                        break
        else:
            for view in views:
                if tran in view.trans:
                    tran_org[tran] = view.role
                    break

    print('tran_org', tran_org)

    tran_send = {}  # tran发送的消息
    for flow in comp_net.flows:
        source, target = flow.get_infor()
        if target in comp_net.msg_places:
            tran_send[source] = target

    interaction_map = {}

    # 设置异步发生变迁对应的交互
    for tran in comp_net.trans:
        if tran not in tran_send.keys() and '_' not in tran:
            continue
        else:
            if '_' in tran:  # 同步交互
                inter = comp_net.label_map[tran]
                orgs = [tran_org[sync_tran] for sync_tran in tran.split('_')]
                interaction_map[tran] = ('1', inter, orgs)
            else:  # 异步交互
                print('tran:', tran)
                # ps:约定某个变迁只能每次最多发送一条消息
                msg = tran_send[tran]
                source_org = tran_org[tran]
                # ps:异步交互中,消息接收者可能是其他组织也可能是自己组织(例如退货情况)
                postset = nt.get_postset(comp_net.flows, msg)
                for target in postset:
                    target_org = tran_org[target]
                    # 异步交互中,消息发送者和接收者组织需不同
                    if target_org != source_org:
                        break
                orgs = [source_org, target_org]
                interaction_map[tran] = ('0', msg, orgs)
                print('interaction_map[tran]:', tran, interaction_map[tran])

    print('interaction_map:', interaction_map)

    return interaction_map


# 找到一个组合网中所有的发送变迁和同步变迁(这两类变迁产生交互)
def find_send_and_sync_trans(comp_net):
    send_trans = []
    sync_trans = []
    for flow in comp_net.flows:
        source, target = flow.get_infor()
        if target in comp_net.msg_places:  #即发送消息
            send_trans.append(source)
    for tran in comp_net.trans:
        if '_' in tran:
            sync_trans.append(tran)
    return send_trans, sync_trans


# 5.获得marking的tau闭包---------------------------------------------
def gen_tau_closure(marking, view_comp, send_trans, sync_trans):
    trans_in_closure = []
    # 运行队列和已访问队列
    ing_queue = [marking]
    ed_queue = [marking]  # 包括marking
    # 迭代计算
    while ing_queue:
        from_marking = ing_queue.pop(0)
        tau_markings, tau_trans = gen_tau_markings(from_marking, view_comp,
                                                   send_trans, sync_trans)
        for marking in tau_markings:
            if not nt.marking_is_exist(marking, ed_queue):
                ing_queue.append(marking)
                ed_queue.append(marking)
        trans_in_closure.extend(tau_trans)
    return ed_queue, trans_in_closure


# 获得marking的tau(不生成交互,即发送消息变迁和同步变迁)迁移标识
def gen_tau_markings(marking, view_comp, send_trans, sync_trans):
    tau_markings = []
    tau_trans = []
    enable_trans = nt.get_enable_trans(view_comp, marking)
    for enable_tran in enable_trans:
        if enable_tran not in set(send_trans + sync_trans):  # 不产生交互
            preset = nt.get_preset(view_comp.get_flows(), enable_tran)
            postset = nt.get_postset(view_comp.get_flows(), enable_tran)
            succ_marking = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            tran = Tran(marking, enable_tran, succ_marking)
            tau_trans.append(tran)
            if not nt.marking_is_exist(succ_marking, tau_markings):
                tau_markings.append(succ_marking)
    return tau_markings, tau_trans


# -------------------------------测试---------------------------------#

if __name__ == '__main__':

    # nets = ng.gen_nets('/Users/moqi/Desktop/原始模型/编排测试/BP.xml')
    # net = nets[0]
    # gen_view(net, 'a')

    path = '/Users/moqi/Desktop/原始模型/编排测试/IMPL_2.xml'
    nets = ng.gen_nets(path)
    for i, net in enumerate(nets):
        net.rename_net('({})'.format(i))
        net.print_infor()
    # comp_net = get_compose_net(nets)
    # comp_net.rename_net('(1)')
    # comp_net.print_infor()
    # comp_net, impl_ig = gen_impl_ig(nets)
    # impl_lts, impl_map = impl_ig.rg_to_lts()
    # impl_lts.lts_to_dot_name('impl')

# -------------------------------------------------------------------#
