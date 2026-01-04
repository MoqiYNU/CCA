# coding=gbk

from lts import LTS, Tran
import net as nt
import net_gen as ng
import lts_utils as lu


# 1.1生成编排交互图的lts(需要最小化)-------------------------------------------------
def gen_CHOR_ig_lts(chor_path):
    CHOR = ng.parse_CHOR_pnml(chor_path)
    CHOR_ig = gen_CHOR_ig(CHOR)
    CHOR_lts, chor_map = CHOR_ig.rg_to_lts()
    # 先重命名再最小化
    CHOR_lts = lu.rename_tau(CHOR_lts)
    CHOR_lts = lu.min_lts(CHOR_lts, 'CH')
    # 将CHOR_lts中的start，ends，states类型从Minstate转换为id
    CHOR_lts.start = CHOR_lts.start.id
    CHOR_lts.ends = [t.get_infor()[0] for t in CHOR_lts.ends]
    CHOR_lts.states = [t.get_infor()[0] for t in CHOR_lts.states]
    # 显示CHOR_lts
    CHOR_lts.lts_to_dot_name('chor')
    return CHOR_lts


# 1.2组合多个编排构建其交互图的lts(需要最小化)-----------------------------------------
def gen_ig_lts_by_joining_CHORs(chor_paths):
    chors = []
    for i, chor_path in enumerate(chor_paths):
        CHOR = ng.parse_CHOR_pnml(chor_path)
        # ps:重命名编排(e.g. p1 -> p1(1)), flag形如'(1),(2),...'
        index = '({})'.format(i)
        CHOR.rename_chor(index)
        chors.append(CHOR)
    comp_chor = get_compose_chor(chors)
    CHOR_ig = gen_CHOR_ig(comp_chor)
    CHOR_lts, chor_map = CHOR_ig.rg_to_lts()
    # 先重命名再最小化
    CHOR_lts = lu.rename_tau(CHOR_lts)
    CHOR_lts = lu.min_lts(CHOR_lts, 'CH')
    # 将CHOR_lts中的start，ends，states类型从Minstate转换为id
    CHOR_lts.start = CHOR_lts.start.id
    CHOR_lts.ends = [t.get_infor()[0] for t in CHOR_lts.ends]
    CHOR_lts.states = [t.get_infor()[0] for t in CHOR_lts.states]
    # 显示CHOR_lts
    # CHOR_lts.lts_to_dot_name('chor')
    return CHOR_lts


# 2.产生编排交互图---------------------------------------------------------------
def gen_CHOR_ig(CHOR: nt.CHOR):

    source = CHOR.source
    sinks = CHOR.sinks
    interaction_map = CHOR.interaction_map

    gen_trans = []  # 产生的变迁集

    # 运行队列和已访问队列
    visiting_queue = [source]
    visited_queue = [source]

    # 迭代计算
    while visiting_queue:
        marking = visiting_queue.pop(0)
        enable_trans = nt.get_enable_trans(CHOR, marking)
        for enable_tran in enable_trans:
            # 1)产生后继标识
            preset = nt.get_preset(CHOR.flows, enable_tran)
            postset = nt.get_postset(CHOR.flows, enable_tran)
            succ_marking = nt.succ_marking(marking.get_infor(), preset,
                                           postset)
            # 2)产生后继变迁(Note:迁移动作以'交互/变迁id'进行标识)
            # tran = Tran(marking, label_map[enable_tran], succ_marking)
            # 迁移动作以变迁进行标识
            interaction = interaction_map[enable_tran]
            inter_str = ''
            if interaction == 'tau':
                inter_str = f'tau: {enable_tran}'
            else:  # ps:同步交互需将list中的同步变迁排序
                if interaction[0] == '0':  # 异步交互(不排序)
                    inter_str = '[' + interaction[0] + ', ' + interaction[
                        1] + ', ' + '{' + ', '.join(
                            interaction[2:]) + '}' + ']: ' + enable_tran
                else:  # 同步交互(需排序)
                    inter_str = '[' + interaction[0] + ', ' + interaction[
                        1] + ', ' + '{' + ', '.join(sorted(
                            interaction[2:])) + '}' + ']: ' + enable_tran
            tran = Tran(marking, inter_str, succ_marking)
            gen_trans.append(tran)
            # 添加未访问的状态
            if not nt.marking_is_exist(succ_marking, visited_queue):
                visiting_queue.append(succ_marking)
                visited_queue.append(succ_marking)

    return LTS(source, sinks, visited_queue, gen_trans)


# 3.组合编排网(ps:异步组合)----------------------------------------------
def get_compose_chor(chors):
    if len(chors) == 0:
        print('no nets exist, exit...')
        return
    if len(chors) == 1:
        return chors[0]
    else:
        chor = compose_two_chors(chors[0], chors[1])
        for i in range(2, len(chors)):
            chor = compose_two_chors(chor, chors[i])
        return chor


# 组合两个开放网
def compose_two_chors(chor1: nt.CHOR, chor2: nt.CHOR):

    # 1)产生源和终止标识~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    source1 = chor1.source
    sinks1 = chor1.sinks
    source2 = chor2.source
    sinks2 = chor2.sinks
    # ps:避免重复添加消息(消息可以初始存在)
    source = nt.Marking(list(set(source1.get_infor() + source2.get_infor())))
    sinks = []
    for sink1 in sinks1:
        for sink2 in sinks2:
            sink = nt.Marking(sink1.get_infor() + sink2.get_infor())
            sinks.append(sink)

    # 2)产生库所(不能重复)~~~~~~~~~~~~~~~~~~~~
    places1 = chor1.places
    places2 = chor2.places
    places = list(set(places1 + places2))

    # 3)产生变迁(ps:不涉及同步变迁)~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    trans1 = chor1.trans
    trans2 = chor2.trans
    trans = []
    interaction_map = {}

    for tran1 in trans1:
        trans.append(tran1)
        interaction_map[tran1] = chor1.interaction_map[tran1]
    for tran2 in trans2:
        trans.append(tran2)
        interaction_map[tran2] = chor2.interaction_map[tran2]

    # 4)产生流关系~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    flows1 = chor1.flows
    flows2 = chor2.flows
    flows = flows1 + flows2

    chor = nt.CHOR(source, sinks, places, trans, flows, interaction_map)

    return chor


# -------------------------------测试---------------------------------#

if __name__ == '__main__':

    chor_path = '/Users/moqi/Desktop/原始模型/编排测试/CHOR.xml'
    CHOR = ng.parse_CHOR_pnml(chor_path)
    CHOR_ig = gen_CHOR_ig(CHOR)
    CHOR_ig_lts, chor_map = CHOR_ig.rg_to_lts()
    CHOR_ig_lts.lts_to_dot_name('chor')

# -------------------------------------------------------------------#
