from collections import Counter
from graphviz import Digraph
import net as nt


# 1.定义LTS(可达图)---------------------------------------------
class LTS(object):

    def __init__(self, start, ends, states, trans):
        self.start = start
        self.ends = ends
        self.opt_ends = []
        self.states = states
        self.trans = trans

    def get_infor(self):
        return self.start, self.ends, self.states, self.trans

    # 获取lts中所有名字集(不重复)
    def get_labels(self):
        labels = set()
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            labels.add(label)
        return list(labels)

    # a.1)将一般可达图转换为lts(正确性验证)~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def rg_to_lts(self):
        index_marking_map = {}
        lts_start = 'S{}'.format(self.get_marking_index(self.start))
        lts_ends = []
        for end in self.ends:
            lts_ends.append('S{}'.format(self.get_marking_index(end)))
        lts_states = []
        for i, state in enumerate(self.states):
            index = 'S{}'.format(i)
            index_marking_map[index] = state
            # print(self.get_marking_index(state), '-------->',
            #       state.get_infor())
            lts_states.append(index)
        lts_trans = []
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            lts_state_from = 'S{}'.format(self.get_marking_index(state_from))
            lts_state_to = 'S{}'.format(self.get_marking_index(state_to))
            lts_trans.append(Tran(lts_state_from, label, lts_state_to))
        # ps:用于挖掘
        opt_ends = [
            'S{}'.format(self.get_marking_index(opt_end))
            for opt_end in self.opt_ends
        ]
        rg_lts = LTS(lts_start, lts_ends, lts_states, lts_trans)
        rg_lts.opt_ends = opt_ends
        # print(lts_start)
        return rg_lts, index_marking_map

    # a.2)将组合图(如[标识,资源]和[标识,lts组合状态]等)转换为lts~~~~~~~~~~
    def comp_to_lts(self):
        index_state_map = {}
        lts_start = 'S{}'.format(self.get_state_index(self.start))
        lts_ends = []
        for end in self.ends:
            lts_ends.append('S{}'.format(self.get_state_index(end)))
        lts_states = []
        for state in self.states:
            index = 'S{}'.format(self.get_state_index(state))
            index_state_map[index] = state  # 将index与state对应
            print(index, '-------->', state[0].get_infor(), state[1])
            lts_states.append(index)
        lts_trans = []
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            lts_state_from = 'S{}'.format(self.get_state_index(state_from))
            lts_state_to = 'S{}'.format(self.get_state_index(state_to))
            lts_trans.append(Tran(lts_state_from, label, lts_state_to))
        print('lts_ends:', lts_ends)
        return LTS(lts_start, lts_ends, lts_states, lts_trans), index_state_map

    # b.1)将lts转换为dot文件~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def lts_to_dot(self):
        dot = Digraph('lts', format='jpg')
        dot.graph_attr['rankdir'] = 'LR'
        dot.graph_attr['dpi'] = '300'
        for state in self.states:
            # print('state', state)
            dot.node(name=state, shape='circle')
            # 如果该节点在终止状态集,变将shape写成doublecircle
            if state in self.ends:
                dot.node(name=state, shape='doublecircle')
            if state == self.start:
                dot.node(name=state, shape='circle')
                # 添加一个空节点，并将其颜色设置为白色
                dot.node(name='', color='white')
                dot.edge('', state, label='', arrowhead='normal')
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            dot.edge(state_from, state_to, label=label, arrowhead='normal')
        dot.view()

    # b.2)将lts转换为dot文件~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def lts_to_dot_index(self, index):
        dot = Digraph('lts{}'.format(index), format='jpg')
        dot.graph_attr['rankdir'] = 'LR'
        dot.graph_attr['dpi'] = '300'
        for state in self.states:
            print('state', state)
            dot.node(name=state, shape='circle')
            # 如果该节点在终止状态集,变将shape写成doublecircle
            if state in self.ends:
                dot.node(name=state, shape='doublecircle')
            if state == self.start:
                dot.node(name=state, shape='circle')
                # 添加一个空节点，并将其颜色设置为白色
                dot.node(name='', color='white')
                dot.edge('', state, label='', arrowhead='normal')
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            dot.edge(state_from, state_to, label=label, arrowhead='normal')
        dot.view()

    # b.3)将lts转换为dot文件~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def lts_to_dot_name(self, fig_name, label_map=None):
        dot = Digraph(fig_name, format='jpg')
        dot.graph_attr['rankdir'] = 'LR'
        # 设置dpi=600
        dot.graph_attr['dpi'] = '300'
        for state in self.states:
            dot.node(name=state, shape='circle')
            # 如果该节点在终止状态集,变将shape写成doublecircle
            if state in self.ends:
                dot.node(name=state, shape='doublecircle')
            if state == self.start:
                dot.node(name=state, shape='circle')
                # 添加一个空节点，并将其颜色设置为白色
                dot.node(name='', color='white')
                dot.edge('', state, label='', arrowhead='normal')
        for tran in self.trans:
            state_from, label, state_to = tran.get_infor()
            if label_map is not None and label in label_map:
                label = label_map[label]
            dot.edge(state_from, state_to, label=label, arrowhead='normal')
        dot.view()

    # 返回可达图中标识在states中位置
    def get_marking_index(self, state):
        for index, temp_state in enumerate(self.states):
            if nt.equal_markings(state, temp_state):
                return index
        return -1

    # 返回资源可达图中state在states中位置
    def get_state_index(self, state):
        print('state:', state)
        [marking, res] = state
        for index, temp_state in enumerate(self.states):
            [temp_marking, temp_res] = temp_state
            if nt.equal_markings(
                    marking,
                    temp_marking) and Counter(res) == Counter(temp_res):
                return index
        return -1


# 2.定义变迁---------------------------------------------
class Tran(object):

    def __init__(self, state_from, label, state_to):
        self.state_from = state_from
        self.label = label
        self.state_to = state_to

    def get_infor(self):
        return self.state_from, self.label, self.state_to
