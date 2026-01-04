package toolkits.utils.block;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import toolkits.def.petri.Flow;

/**
 * @author Moqi 
 * 计算内网中最大顺序块
 */
public class GetSeqBlock {
	
	private List<Block> maxSEQBlocks;
	
	public GetSeqBlock() {
		maxSEQBlocks = new ArrayList<Block>();
	}

	public List<Block> getMaxSEQBlocks() {
		return maxSEQBlocks;
	}
	
	public void compute(InnerNet net) {
		
		maxSEQBlocks.clear();//计算之前先清空缓存
		
		//计算所有顺序对
		List<SEQPair> seqPairs = new ArrayList<>();
		List<String> places = net.getPlaces();
		for (String place : places) {
			List<String> preSet = getPreSet(place, net.getFlows());
			List<String> postSet = getPostSet(place, net.getFlows());
			if (preSet.size() == 1 && postSet.size() == 1 //place前面及后面均只有1个变迁a和b,即(a,place,b)
					&& (getPreSet(preSet.get(0), net.getFlows())).size() == 1//变迁a的前集为1
					&& (getPostSet(preSet.get(0), net.getFlows())).size() == 1//变迁a的后集为1
					&& (getPreSet(postSet.get(0), net.getFlows())).size() == 1//变迁b的前集为1
					&& (getPostSet(postSet.get(0), net.getFlows())).size() == 1) {//变迁b的后集为1
				//System.out.println("entry: " + place);
				SEQPair seqPair = new SEQPair();
				seqPair.setPreAct(preSet.get(0));
				seqPair.setSuccAct(postSet.get(0));
				seqPairs.add(seqPair);
			}
		}
		
		//计算最大顺序块,即若(a,b),(c,d),(f,g),则最大顺序块为(a,b),(f,g)
		List<SEQPair> maxSeqPairs = getMaxSeqPairs(seqPairs);
		for (SEQPair maxSeqPair : maxSeqPairs) {
			String preAct = maxSeqPair.getPreAct();
			String succAct = maxSeqPair.getSuccAct();
			List<String> succActs = getSeqActs(succAct, seqPairs);
			List<String> seqActs = new ArrayList<String>();
			seqActs.add(preAct);
			seqActs.add(succAct);
			seqActs.addAll(succActs);
			Block part = new Block();
			part.setEntry(seqActs.get(0));
			part.setExit(seqActs.get(seqActs.size()-1));
			part.setSeqActs(seqActs);
			part.setType("SEQ");
			maxSEQBlocks.add(part);
		}
	}
	
	//获取elem的后集
	public List<String> getPostSet(String elem, List<Flow> flows) {
		List<String> postSet = new ArrayList<String>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (elem.equals(from)) {
				postSet.add(to);
			}
		}
		return postSet;
	}
	
	//获取elem的前集
	public List<String> getPreSet(String elem, List<Flow> flows) {
		List<String> preSet = new ArrayList<String>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (elem.equals(to)) {
				preSet.add(from);
			}
		}
		return preSet;
	}
	
	//求act引导后续变迁系列,即若act=a,且(a,b),(b,c),(c,d),则<b,c,d>
	public List<String> getSeqActs(String act, List<SEQPair> seqPairs) {
		
		List<String> succActs = new ArrayList<String>();
		//即将访问的队列visitingQueue
		Queue<String> visitingQueue = new LinkedList<>();
		visitingQueue.offer(act);
		//succActs.add(act);
		//迭代计算
	    while(visitingQueue.size() > 0){
	    	//出对一个标识,以此进行迁移
		    String actFrom = visitingQueue.poll();
	    	for (SEQPair seqPair : seqPairs) {
	    		String preAct = seqPair.getPreAct();
				String succAct = seqPair.getSuccAct();
				if (actFrom.equals(preAct)) {
					succActs.add(succAct);
					visitingQueue.offer(succAct);
					break;
				}
			}
	    }
		return succActs;
	}
	
	//求最大顺序对集
	public List<SEQPair> getMaxSeqPairs(List<SEQPair> seqPairs) {
		List<SEQPair> maxSeqPairs = new ArrayList<SEQPair>();
		for (SEQPair seqPair : seqPairs) {
			if (isMaxSeqPair(seqPair, seqPairs)) {
				maxSeqPairs.add(seqPair);
			}
		}
		return maxSeqPairs;
	}

	//判断是否为最大顺序对,即该顺序对前面不包括其他活动
	private boolean isMaxSeqPair(SEQPair seqPair, List<SEQPair> seqPairs) {
		String preAct = seqPair.getPreAct();
		for (SEQPair tempSeqPair : seqPairs) {
			String tempPostAct = tempSeqPair.getSuccAct();
			if (preAct.equals(tempPostAct)) {
				return false;
			}else {
				continue;
			}
		}
		return true;
	}

}
