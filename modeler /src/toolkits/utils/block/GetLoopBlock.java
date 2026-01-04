package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import toolkits.def.petri.Flow;
import toolkits.utils.petri.PetriUtils;

/**
 * @author Moqi
 * 通过DFS计算内网中迭代块
 */
public class GetLoopBlock {
	
	private List<Block> loopBlocks;//存储所有迭代块
	private List<Block> metaLoopBlocks;//存储所有元迭代块
	private PetriUtils petriUtils;
	
	public GetLoopBlock() {
		loopBlocks = new ArrayList<Block>();
		metaLoopBlocks = new ArrayList<Block>();
		petriUtils = new PetriUtils();
	}
	
	//获取所有迭代块
	public List<Block> getLoopBlocks() {
		return loopBlocks;
	}

	@SuppressWarnings("static-access")
	public List<Block> getMetaLoopBlocks(InnerNet net) {//获取所有元迭代块
		compute(net);
        for (Block loopBlock : loopBlocks) {
        	List<String> entryActs = loopBlock.getEntryPost();
			List<String> exitActs = loopBlock.getExitPre();
			if (entryActs.size() == 1 && exitActs.size() == 1) {//出发及到达活动均为1个
				String entryAct = entryActs.get(0);
				String exitAct = exitActs.get(0);
				if (entryAct.equals(exitAct)) {
					List<String> preSet = petriUtils.getPreSet(entryAct, net.getFlows());
					List<String> postSet = petriUtils.getPostSet(exitAct, net.getFlows());
					if (CollectionUtils.isEqualCollection(preSet, postSet)) {
						metaLoopBlocks.add(loopBlock);
					}
				}
			}
		}
        return metaLoopBlocks;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void compute(InnerNet net) {
		
		//计算之前先清空缓存
		loopBlocks.clear();
		metaLoopBlocks.clear();
		
		//获取source
		String initState = net.getSource();
		List route = new ArrayList();
		route.add(initState);
		
		List<String> visitedQueue = new ArrayList<>();
		
		do {
			int size = route.size();
			//取最后元素,即当前状态
			String curState = (String) route.get(size-1);
			//获得以curState开始的流
			List<Flow> succeedFlows = getSucceedTrans(net, curState, route, size-1);
			if (succeedFlows.size() == 0) {//不能迁移,则回溯
				//回溯路径
				route.remove(size-1);
				//设置已经访问
				visitedQueue.add(curState);
			}else {
				Flow firstNotVisitedFlow = getFirstNotVisited(visitedQueue, succeedFlows);
				//孩子展开状态均已经访问,则回溯并置为已访问
			    if (firstNotVisitedFlow == null) {
			    	//回溯路径
			    	route.remove(size-1);
					//设置已经访问
					visitedQueue.add(curState);
				}else {
					String succeedeState = firstNotVisitedFlow.getFlowTo();
					route.add(succeedeState);
				}
			}
		} while (route.size() > 0);
    }

	//获得未被访问的第一条流,若所有流均被访问,则返回null
	private Flow getFirstNotVisited(List<String> visitedQueue, List<Flow> succeedFlows) {
		for (Flow flow : succeedFlows) {
			String stateTo = flow.getFlowTo();
			if (!stateIsVisited(visitedQueue, stateTo)) {
				return flow;
			}
		}
		return null;
	}
	
	//判断后继状态是否访问
	public boolean stateIsVisited(List<String> visitedQueue, String succeedState) {
		for (String state : visitedQueue) {
			if (state.equals(succeedState)) {
				return true;
			}
		}
		return false;
	}

	//获得state后继流集
	@SuppressWarnings("rawtypes")
	private List<Flow> getSucceedTrans(InnerNet net, String state, List route, int curIndex) {
		List<Flow> succeedFlows = new ArrayList<>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (state.equals(from)) {
				if (curIndex%2 == 1) {//若from为变迁(即当前位置为奇数),to是库所,则计算是否存在循环结构
					int index = isLoop(route, to);
					if (index == -1) {//由to未构成环结构
						succeedFlows.add(flow);
					}else {//由to构成环结构,则生成循环部件,即entry->act->...->from/state->to/entry
						String entry = (String) route.get(index);
						String act = (String) route.get(index+1);
						//建立迭代块
						Block block = new Block();
						//人口和出口均为库所
						block.setEntry(entry);
						block.setExit(entry);
						//入口出发活动
						block.addEntryPost(act);
						//到达出口活动
						block.addExitPre(state);
						block.setType("Loop");
						loopBlocks.add(block);
					}
				}else {//若to是变迁,则直接添加
					succeedFlows.add(flow);
				}
			}
		}
		return succeedFlows;
	}
	
	//判断是否存在循环结构
	@SuppressWarnings("rawtypes")
	public int isLoop(List route, String to) {
		int routeSize = route.size();
		for (int i = 0; i < routeSize; i++) {
			if (i%2 == 0) {//偶数为标识,余数为0
				String state = (String) route.get(i);
				if (to.equals(state)) {
					return i;
				}
			}else {
				continue;
			}
		}
		return -1;
	}

}
