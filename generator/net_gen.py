from collections import Counter
import xml.dom.minidom
from net import Flow, Marking, OpenNet
import net as nt


# 0.解析pnml(编排)---------------------------------------------
def parse_CHOR_pnml(path):

    # 使用minidom解析器打开pnml文档
    DOMTree = xml.dom.minidom.parse(path)
    collection = DOMTree.documentElement

    # 1)解析流关系(排除数据流)~~~~~~~~~~~~~~~~~~~~~~~~
    flows = []
    flow_elems = collection.getElementsByTagName('arc')
    for flow_elem in flow_elems:
        fl_from = flow_elem.getAttribute('source').strip()
        fl_to = flow_elem.getAttribute('target').strip()
        flows.append(Flow(fl_from, fl_to))

    # 2)解析库所~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    sour_places = []
    places = []

    place_elems = collection.getElementsByTagName('place')
    for place_elem in place_elems:

        id = place_elem.getAttribute('id').strip()

        # 若为内部,消息及资源库所,则进行如下处理~~~~~~~~~~~~~~~~~~~·
        places.append(id)

        # 库所托肯
        init_mark_elem = place_elem.getElementsByTagName('initialMarking')[0]
        val = init_mark_elem.getElementsByTagName(
            'value')[0].childNodes[0].data
        token_num = int(val)
        if token_num > 0:
            for i in range(token_num):
                sour_places.append(id)

    # 3)解析变迁~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    trans = []
    interaction_map = {}

    tran_elems = collection.getElementsByTagName('transition')
    for tran_elem in tran_elems:
        id = tran_elem.getAttribute('id').strip()
        trans.append(id)
        name_elem = tran_elem.getElementsByTagName('name')[0]
        name = name_elem.getElementsByTagName("value")[0].childNodes[0].data
        # 移除前后空格
        name = name.strip()
        interaction = ''
        if '[' not in name:
            interaction = 'tau'
        else:
            # 每个变迁上关联的交互形如:[0(异步)/1(同步), msg, {org1, org2, org3}]
            # 移除'[',']','{'和'}'
            name = name.replace('[', '')
            name = name.replace(']', '')
            name = name.replace('{', '')
            name = name.replace('}', '')
            elems = name.split(',')
            # 格式为[0/1, msg, org1, org2, org3]
            interaction = [elem.strip() for elem in elems]
        interaction_map[id] = interaction

    # 4)解析终止标识~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    sinks = []
    label_elems = collection.getElementsByTagName('labels')
    for label_elem in label_elems:
        is_final = label_elem.getElementsByTagName(
            'finalMarings')[0].childNodes[0].data
        if is_final == 'true':
            text = label_elem.getElementsByTagName(
                'text')[0].childNodes[0].data
            sinks = sinks + gen_markings(text)

    # 5)返回解析后编排~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    source = nt.Marking(sour_places)
    CHOR = nt.CHOR(source, sinks, places, trans, flows, interaction_map)

    return CHOR


# 1.解析pnml(过程)---------------------------------------------
def parse_pnml(path):

    # 使用minidom解析器打开pnml文档
    DOMTree = xml.dom.minidom.parse(path)
    collection = DOMTree.documentElement

    # 1)解析流关系(排除数据流)~~~~~~~~~~~~~~~~~~~~~~~~
    flows = []
    # inhibitor_arcs = []
    flow_elems = collection.getElementsByTagName('arc')
    for flow_elem in flow_elems:
        fl_from = flow_elem.getAttribute('source').strip()
        fl_to = flow_elem.getAttribute('target').strip()
        # type = flow_elem.getElementsByTagName('type')[0].getAttribute('value')
        # 普通流,即控制流,消息流和资源流(ps:建立模型中不涉及抑制弧)
        flows.append(Flow(fl_from, fl_to))
        # # 抑制弧
        # if type == 'inhibitor':
        #     inhibitor_arcs.append([fl_from, fl_to])
        #     flows.append(Flow(fl_from, fl_to))
        # else:  # 普通流(即控制流,消息流和资源流)
        #     flows.append(Flow(fl_from, fl_to))

    # 2)解析库所~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    sour_places = []
    places = []
    inner_places = []
    msg_places = []
    msg_place_map = {}  # 消息库所到名字的映射
    res_places = []
    res_place_map = {}  # 资源库所到名字的映射
    res_property = {}
    init_res = []  # 初始资源
    visited_res_places = []  # 避免初始资源重复

    place_elems = collection.getElementsByTagName('place')
    for place_elem in place_elems:

        id = place_elem.getAttribute('id').strip()

        # 若为内部,消息及资源库所,则进行如下处理~~~~~~~~~~~~~~~~~~~·
        places.append(id)

        # 消息库所(生成网中是名字而非Id)
        is_msg = place_elem.getElementsByTagName(
            'msgPlace')[0].childNodes[0].data
        if is_msg == 'true':
            name_elem = place_elem.getElementsByTagName('name')[0]
            name = name_elem.getElementsByTagName(
                'value')[0].childNodes[0].data
            msg_places.append(id)
            # Note:消息库所保存的是name
            msg_place_map[id] = name.strip()  # 移除前后空格

        # 资源库所(生成网中是名字而非Id)
        is_res = place_elem.getElementsByTagName(
            'resPlace')[0].childNodes[0].data
        if is_res == 'true':
            name_elem = place_elem.getElementsByTagName('name')[0]
            name = name_elem.getElementsByTagName(
                'value')[0].childNodes[0].data
            res_places.append(id)
            # Note:资源库所保存的是name
            res_place_map[id] = name.strip()  # 移除前后空格
            # 前后集均不为空,则为重复利用资源
            if nt.get_preset(flows, id) and nt.get_postset(flows, id):
                # 重复利用资源
                res_property[id] = 0
            else:
                res_property[id] = 1

        # 内部库所
        if is_msg == 'false' and is_res == 'false':
            inner_places.append(id)

        # 库所托肯
        init_mark_elem = place_elem.getElementsByTagName('initialMarking')[0]
        val = init_mark_elem.getElementsByTagName(
            'value')[0].childNodes[0].data
        token_num = int(val)
        if token_num > 0:
            if is_res == 'false':  # 非资源库所
                for i in range(token_num):
                    sour_places.append(id)
            else:  # 构建初始资源(用名字表示资源)
                name_elem = place_elem.getElementsByTagName('name')[0]
                name = name_elem.getElementsByTagName(
                    'value')[0].childNodes[0].data
                # 初始资源向量(避免重复添加初始资源)
                name = name.strip()  # 移除前后空格
                if name in visited_res_places:
                    continue
                visited_res_places.append(name)
                for i in range(token_num):
                    init_res.append(name)

    # 3)解析变迁~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    trans = []
    rout_trans = []
    label_map = {}
    req_res_map = {}
    rel_res_map = {}
    # 变迁Delay映射
    tran_delay_map = {}

    tran_elems = collection.getElementsByTagName('transition')
    for tran_elem in tran_elems:
        id = tran_elem.getAttribute('id').strip()
        trans.append(id)
        is_rout = tran_elem.getElementsByTagName(
            'routTran')[0].childNodes[0].data
        if is_rout == 'true':
            rout_trans.append(id)
        name_elem = tran_elem.getElementsByTagName('name')[0]
        name = name_elem.getElementsByTagName("value")[0].childNodes[0].data
        # 移除前后空格
        label_map[id] = name.strip()
        # 变迁请求资源(Note:每类请求资源可能有多个,以名字标识)
        req_res = []
        for flow_elem in flow_elems:
            fl_from = flow_elem.getAttribute('source').strip()
            fl_to = flow_elem.getAttribute('target').strip()
            if fl_to == id and fl_from in res_places:
                insc_elem = flow_elem.getElementsByTagName('inscription')[0]
                wight = insc_elem.getElementsByTagName(
                    'value')[0].childNodes[0].data
                for i in range(int(wight)):
                    req_res.append(res_place_map[fl_from])
        req_res_map[id] = req_res
        # 变迁释放资源(ps:每类释放资源可能有多个,以名字标识)
        rel_res = []
        for flow_elem in flow_elems:
            fl_from = flow_elem.getAttribute('source').strip()
            fl_to = flow_elem.getAttribute('target').strip()
            if fl_from == id and fl_to in res_places:
                insc_elem = flow_elem.getElementsByTagName('inscription')[0]
                wight = insc_elem.getElementsByTagName(
                    'value')[0].childNodes[0].data
                for i in range(int(wight)):
                    rel_res.append(res_place_map[fl_to])
        rel_res_map[id] = rel_res
        # 变迁最小和最大点火时间
        min_delay = tran_elem.getElementsByTagName(
            'minDelay')[0].childNodes[0].data
        max_delay = tran_elem.getElementsByTagName(
            'maxDelay')[0].childNodes[0].data
        # print(min_delay, max_delay)
        tran_delay_map[id] = [
            float(min_delay.strip()),
            float(max_delay.strip())
        ]

    # 4)解析终止标识~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    sinks = []
    label_elems = collection.getElementsByTagName('labels')
    for label_elem in label_elems:
        is_final = label_elem.getElementsByTagName(
            'finalMarings')[0].childNodes[0].data
        if is_final == 'true':
            text = label_elem.getElementsByTagName(
                'text')[0].childNodes[0].data
            sinks = sinks + gen_markings(text)

    # 5)返回解析开放网~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    openNet = OpenNet(Marking(sour_places), sinks, list(set(places)), trans,
                      label_map, flows)

    openNet.inner_places = inner_places
    openNet.msg_places = list(set(msg_places))

    openNet.rout_trans = rout_trans

    # 资源库所和数据库所设置
    openNet.init_res = init_res
    openNet.res_places = list(set(res_places))
    openNet.res_property = res_property
    openNet.req_res_map = req_res_map
    openNet.rel_res_map = rel_res_map

    # openNet.inhibitor_arcs = inhibitor_arcs
    openNet.tran_delay_map = tran_delay_map

    return openNet, msg_place_map, res_place_map


# 解析多个终止标识的字符串,这些标识件用";"分隔,如1*P1+2*P2;2*P3+3*P4
def gen_markings(text):
    markings = []
    str_markings = text.split(';')
    for i in range(len(str_markings)):
        markings.append(gen_marking(str_markings[i].strip()))
    return markings


# 解析一个终止标识,如1*P1+2*P3+3*P4
def gen_marking(str_marking):
    places = []
    maps = str_marking.split('+')
    for map in maps:
        map_arr = map.split('*')
        number = int(map_arr[0].strip())
        for i in range(number):
            places.append(map_arr[1].strip())
    return Marking(places)


# 2.利用变迁扩展形成迁移包---------------------------------------------
def gen_bags(trans, net: OpenNet):

    bags = []
    visited_trans = []

    # print(net.get_res_places())

    for tran in trans:
        if tran in visited_trans:
            continue
        # 运行队列和已访问队列
        visiting_queue = [tran]
        visited_queue = [tran]
        # 迭代计算
        while visiting_queue:
            to_places = []
            from_tran = visiting_queue.pop(0)
            to_places = list(
                set(to_places + nt.get_preset(net.get_flows(), from_tran)))
            to_places = list(
                set(to_places + nt.get_postset(net.get_flows(), from_tran)))
            to_trans = []
            for to_place in to_places:
                to_trans = list(
                    set(to_trans + nt.get_preset(net.get_flows(), to_place)))
                to_trans = list(
                    set(to_trans + nt.get_postset(net.get_flows(), to_place)))
            # 计算tran可达的前后迁移集(避免前后集重复)
            for to_tran in to_trans:
                if to_tran not in visited_queue:
                    visiting_queue.append(to_tran)
                    visited_queue.append(to_tran)
        # print('visited_queue:', tran, visited_queue)
        bags.append(visited_queue)
        # print(bags)
        visited_trans = visited_trans + visited_queue
    return bags


# 判断当前bag是否产生过
def is_gen_bag(ed_queue, bags):
    for bag in bags:
        if Counter(ed_queue) == Counter(bag):
            return True
        else:
            continue
    return False


# 3.由bag中变迁产生参与组织的网---------------------------------------------
def gen_nets(net_path):

    bag_nets = []

    # 1)解析产生包~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    net, msg_place_map, res_place_map = parse_pnml(net_path)
    trans, rout_trans, label_map = net.get_trans()
    tran_delay_map = net.tran_delay_map
    # print('trans: ', trans)
    bags = gen_bags(trans, net)

    # 使用minidom解析器打开pnml文档并获取每个组织对应的角色
    role_map = {}
    DOMTree = xml.dom.minidom.parse(net_path)
    collection = DOMTree.documentElement
    label_elems = collection.getElementsByTagName('labels')
    for label_elem in label_elems:
        is_final = label_elem.getElementsByTagName(
            'finalMarings')[0].childNodes[0].data
        if is_final == 'false':
            text = label_elem.getElementsByTagName(
                'text')[0].childNodes[0].data
            text = text.strip()
            print(text)
            # 如Org-P0
            role_source = text.split('-')
            if '-' not in text:  # 如没有设置角色则每个角色关联的源库所设为'SOURCE'
                role_map[role_source[0].strip()] = 'SOURCE'
            else:
                role_map[role_source[0].strip()] = role_source[1].strip()

    # 2)针对每个包,计算其对应网~~~~~~~~~~~~~~~~~~~~~~~
    for bag in bags:

        # print('net infor.................................................\n')

        # a)确定变迁
        bag_rout_trans = list(set(bag).intersection(set(rout_trans)))
        bag_label_map = {}
        for id, label in label_map.items():
            if id in bag:
                bag_label_map[id] = label
        # ps:确定bag中变迁时间间隔
        bag_tran_delay_map = {}
        for id, delay in tran_delay_map.items():
            if id in bag:
                bag_tran_delay_map[id] = delay
        bag_req_res_map = {}
        bag_rel_res_map = {}
        for tran in bag:
            # 请求和释放资源以name标识
            bag_req_res_map[tran] = net.req_res_map[tran]
            bag_rel_res_map[tran] = net.rel_res_map[tran]

        # b)确定流和库所(排除资源库所)
        bag_places = set()
        bag_msg_place_ids = set()  #记录bag中消息库所的id
        res_place_ids = set()
        bag_flows = []
        flows = net.get_flows()
        for flow in flows:
            fl_from, fl_to = flow.get_infor()
            if fl_from in bag:  #fl_from为变迁
                # 跳过资源库所形成的流~~~~~~~~~~~~~~~~~~~
                if fl_to in net.res_places:
                    res_place_ids.add(fl_to)
                    continue
                # 消息库所用name表示
                if fl_to in msg_place_map.keys():
                    bag_msg_place_ids.add(fl_to)
                    bag_places.add(msg_place_map[fl_to])
                    bag_flows.append(Flow(fl_from, msg_place_map[fl_to]))
                else:
                    bag_places.add(fl_to)
                    bag_flows.append(flow)
            elif fl_to in bag:  #fl_to为变迁
                # 跳过资源库所形成的流~~~~~~~~~~~~~~~~~~~
                if fl_from in net.res_places:
                    res_place_ids.add(fl_from)
                    continue
                # 消息库所用name表示
                if fl_from in msg_place_map.keys():
                    bag_msg_place_ids.add(fl_from)
                    bag_places.add(msg_place_map[fl_from])
                    bag_flows.append(Flow(msg_place_map[fl_from], fl_to))
                else:
                    bag_places.add(fl_from)
                    bag_flows.append(flow)

        # c)确定资源库所
        inner_places = net.inner_places
        bag_inner_places = list(bag_places.intersection(set(inner_places)))

        bag_msg_places = list(
            bag_places.intersection(set(msg_place_map.values())))

        # 资源库所设置(名字标识)
        bag_res_places = [res_place_map[id] for id in res_place_ids]
        bag_res_property = {}
        for id, pro in net.res_property.items():
            if id in res_place_ids:
                bag_res_property[res_place_map[id]] = pro
        bag_init_res = [
            res for res in net.get_init_res() if res in bag_res_places
        ]

        # e)确定source和sink(Note:有重复元素)
        source, sinks = net.get_start_ends()
        sour_places = []
        for place in source.get_infor():
            if place in bag_places:
                sour_places.append(place)
            # ps:消息库所也可能有初始值
            if place in bag_msg_place_ids:
                sour_places.append(msg_place_map[place])
        bag_source = Marking(sour_places)
        # print('sour_places', sour_places)
        bag_sinks = []
        for sink in sinks:
            sink_places = [i for i in sink.get_infor() if i in bag_places]
            if sink_places:
                bag_sink = Marking(sink_places)
                bag_sinks.append(bag_sink)

        # 构建由包生成的网
        bag_net = OpenNet(bag_source, bag_sinks, list(bag_places), bag,
                          bag_label_map, bag_flows)

        bag_net.inner_places = bag_inner_places
        bag_net.msg_places = bag_msg_places

        bag_net.rout_trans = bag_rout_trans

        # 资源库所和数据库所设置
        bag_net.init_res = bag_init_res
        bag_net.res_places = list(set(bag_res_places))
        bag_net.res_property = bag_res_property
        bag_net.req_res_map = bag_req_res_map
        bag_net.rel_res_map = bag_rel_res_map

        bag_net.tran_delay_map = bag_tran_delay_map

        # 设置网对应的角色
        for key, value in role_map.items():
            if value in bag_net.places:
                bag_net.role = key
                break

        bag_net.print_infor()

        bag_nets.append(bag_net)

    return bag_nets


# -------------------------------测试---------------------------------#

if __name__ == '__main__':

    # nets = gen_nets('/Users/moqi/Desktop/临时文件/2023.xml')

    # nets = gen_nets('/Users/moqi/Desktop/原始模型/过程挖掘/Example 1.xml')

    CHOR = parse_CHOR_pnml('/Users/moqi/Desktop/原始模型/编排测试/CHOR_2.xml')
    print(CHOR.source.places, CHOR.sinks[0].places, CHOR.places, CHOR.trans,
          CHOR.interaction_map)
    CHOR.rename_net('(1)')
    print(CHOR.source.places, CHOR.sinks[0].places, CHOR.places, CHOR.trans,
          CHOR.interaction_map)

    # for flow in CHOR.flows:
    #     print(flow.flow_from, flow.flow_to)

    # net = nets[1]
    # net.net_to_dot('abc', True)
    # for net in nets:
    #     net.print_infor()
    #     net.net_to_dot()
    # comp_net = get_compose_net(nets)
    # comp_net.net_to_dot('comp_net', True)
    # print('partner, places, trans:', len(nets), len(comp_net.places),
    #       len(comp_net.trans))

# -------------------------------------------------------------------#
