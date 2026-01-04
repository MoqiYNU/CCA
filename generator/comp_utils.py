# coding=gbk

from net import Flow, Marking, OpenNet


# 1a.组合业务过程(同步+异步)-------------------------------------------------------
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
