# coding=gbk

from collections import defaultdict


class TS:  # 有向图

    def __init__(self):
        self.states = set()
        self.transitions = defaultdict(
            list)  # state -> [(label, target_state)]
        self.reverse_transitions = defaultdict(
            list)  # target_state -> [(label, source_state)]
        self.alphabet = set()

    def add_transition(self, u, label, v):
        self.states.add(u)
        self.states.add(v)
        self.transitions[u].append((label, v))
        self.reverse_transitions[v].append((label, u))
        self.alphabet.add(label)


def build_lts_from_data(states, transitions):
    '''
    从 states 和 transitions 列表构建TS
    参数:
        states: 状态列表，例如 [0, 1, 2, 3]
        transitions: 转换列表，每个元素为 (源状态, 动作标签, 目标状态) 的元组
                    例如 [(0, 'a', 1), (1, 'b', 2)]
    返回:
        TS 对象
    '''
    ts = TS()

    # 添加所有状态（确保孤立状态也被包含）
    for state in states:
        ts.states.add(state)

    # 添加所有转换
    for u, label, v in transitions:
        ts.add_transition(u, label, v)

    return ts


def compute_branching_bisimilarity(lts, tau_label="tau"):
    '''
    计算分支互模拟(Branching Bisimilarity)
    算法基于分割细化, 正确处理 tau(静默动作):
    - 块内的 tau 转换被视为"惰性"(inert), 不区分状态
    - 状态可以通过惰性 tau 序列到达能执行可见动作的状态
    算法思路：
    1. 初始时所有状态在一个块中
    2. 迭代地计算每个状态的"签名"(signature):
       - 从状态 s 出发，经过惰性 tau* 后能执行的动作及其目标块
    3. 将具有相同签名的状态归为同一块
    4. 重复直到分区稳定
    '''
    if not lts.states:
        return []

    # 初始化分区
    P = [set(lts.states)]
    block_map = {s: 0 for s in lts.states}

    def inert_tau_closure(state, current_block_idx):
        # 计算从 state 出发，在当前块内的惰性 tau 闭包
        closure = {state}
        stack = [state]
        while stack:
            u = stack.pop()
            for label, v in lts.transitions.get(u, []):
                if label == tau_label and block_map.get(
                        v) == current_block_idx and v not in closure:
                    closure.add(v)
                    stack.append(v)
        return closure

    changed = True
    while changed:
        changed = False

        # 计算每个状态的签名
        signatures = {}
        for s in lts.states:
            current_block = block_map[s]
            closure = inert_tau_closure(s, current_block)

            # 收集从闭包中发出的非惰性转换
            sig = set()
            for u in closure:
                for label, v in lts.transitions.get(u, []):
                    target_block = block_map.get(v)
                    if label == tau_label:
                        # tau 转换：只有跨越块边界的才计入签名
                        if target_block != current_block:
                            sig.add((tau_label, target_block))
                    else:
                        # 可见动作：总是计入签名
                        sig.add((label, target_block))

            signatures[s] = tuple(sorted(sig))

        # 根据签名重新分组
        sig_to_states = defaultdict(set)
        for s, sig in signatures.items():
            sig_to_states[sig].add(s)

        new_P = list(sig_to_states.values())

        # 检查分区是否改变
        if len(new_P) != len(P) or any(
                frozenset(b) not in {frozenset(b2)
                                     for b2 in P} for b in new_P):
            changed = True
            P = new_P

            # 更新 block_map
            block_map = {}
            for idx, block in enumerate(P):
                for s in block:
                    block_map[s] = idx

    return P


def print_partition(partition):
    print(f"计算完成。发现 {len(partition)} 个等价类:")
    # 按类的大小排序输出，方便查看
    sorted_partition = sorted(partition, key=lambda x: len(x), reverse=True)
    for i, block in enumerate(sorted_partition):
        print(f"等价类 {i+1} (包含 {len(block)} 个状态): {sorted(list(block))}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='计算 TS 上的分支互模拟等价类')
    parser.add_argument('--tau',
                        default='tau',
                        help='tau (静默动作) 的标签名称，默认为 "tau"')

    args = parser.parse_args()

    try:
        # 内置示例数据
        states = [0, 1, 2, 3, 4, 5, 6]
        transitions = [(0, 'a', 1), (1, 'b', 2), (1, 'e', 3), (2, 'c', 6),
                       (3, 'g', 6), (2, 'd', 4), (4, 'tau', 6), (3, 'f', 5),
                       (5, 'tau', 6)]

        ts = build_lts_from_data(states, transitions)

        print("使用内置示例数据:")
        print(f"  states = {states}")
        print(f"  transitions = {transitions}")
        print()
        print(f"已加载 TS: {len(ts.states)} 个状态, {len(ts.alphabet)} 个不同动作。")
        print(f"正在计算分支互模拟等价类 (处理 tau='{args.tau}')...")

        partition = compute_branching_bisimilarity(ts, tau_label=args.tau)
        print('partition:', partition)
        print_partition(partition)

    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
