package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.Bag;
import org.apache.commons.collections4.bag.HashBag;

import toolkits.def.petri.Flow;
import toolkits.utils.petri.PetriUtils;

/**
 * @author Moqi
 * 通过DFS计算内网中选择块
 */
public class GetXorBlock {
	
	//缓存元选择部件
	private List<Block> metaXORBlocks;
	private List<String> orSplits;
	private List<String> orJoins;
	
	public GetXorBlock() {
		metaXORBlocks = new ArrayList<Block>();
		orSplits = new ArrayList<>();
		orJoins = new ArrayList<>();
	}
	
	public List<Block> getMetaXORBlocks() {
		return metaXORBlocks;
	}
	
	public List<String> getOrSplits() {
		return orSplits;
	}

	public List<String> getOrJoins() {
		return orJoins;
	}

	public void compute(InnerNet net, List<Block> loopBlocks) {
		
		//计算前先清空缓存
		metaXORBlocks.clear();
		orSplits.clear();
		orJoins.clear();
		
		// 1.计算循环块中从入口出发的活动集及到达出口的活动集
		List<String> entryActsInLoop = new ArrayList<String>();
		List<String> exitActsInLoop = new ArrayList<String>();
		for (Block block : loopBlocks) {
			entryActsInLoop.addAll(block.getEntryPost());
			exitActsInLoop.addAll(block.getExitPre());
		}
		
		// 2.针对每个分支库所进行计算元选择块
		List<String> places = net.getPlaces();
		for (String place : places) {
			//获取从place出发的流集(Note:排除进入循环流集)
			List<Flow> splitFlows = computeSplitFlows(place, net.getFlows(), entryActsInLoop);
			if (splitFlows.size() > 1) {//2.1 为分支库所
				orSplits.add(place);
				Block block = creatMetaXORBlock(place, splitFlows, net.getFlows());
				if (block != null) {
					metaXORBlocks.add(block);
				}
			}
			//获取到达place的流集(Note:排除出循环流集)
			List<Flow> joinFlows = computeJoinFlows(place, net.getFlows(), exitActsInLoop);
			if (joinFlows.size() > 1) {//2.2 为汇合库所
				orJoins.add(place);
			}
		}
		
	}
	
	//计算从place出发的流集(Note:排除进循环流集)
	private List<Flow> computeSplitFlows(String place, List<Flow> flows, List<String> entryActsInLoop) {
		
		List<Flow> succFlows = new ArrayList<Flow>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (place.equals(from) && !entryActsInLoop.contains(to)) {
				succFlows.add(flow);
			}
		}
		return succFlows;
	}
	
	//计算到达place的流集(Note:排除出循环流集)
	private List<Flow> computeJoinFlows(String place, List<Flow> flows, List<String> exitActsInLoop) {
		
		List<Flow> reachFlows = new ArrayList<Flow>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (place.equals(to) && !exitActsInLoop.contains(from)) {
				reachFlows.add(flow);
			}
		}
		return reachFlows;
	}
	
	//创建元选择块
	public Block creatMetaXORBlock(String place, List<Flow> splitFlows, List<Flow> flows) {
		
		List<String> exits = new ArrayList<String>();
		List<String> exitsGen = new ArrayList<String>();
		for (Flow flow : splitFlows) {//针对每条分支流进行计算
			String act = flow.getFlowTo();
			List<String> preSet = PetriUtils.getPreSet(act, flows);
			List<String> postSet = PetriUtils.getPostSet(act, flows);
			exitsGen.addAll(postSet);
			if (preSet.size() == 1 && postSet.size() == 1) {
				exits.add(postSet.get(0));
			}
		}
		
		/**C1:针对一般元选择和XOR2(元选择后接),先利用Bag确认不重复exitsGen集..............................*/
		Bag<String> bagGen = new HashBag<String>(exitsGen);
		Set<String> uniqueSetGen = bagGen.uniqueSet();
		if (uniqueSetGen.size() == 1) {//对应测试用例中XOR2
			Block block = new Block();
			block.setEntry(place);
			List<String> actsInMetaXor = PetriUtils.getPostSet(place, flows);
			for (String actInMetaXor : actsInMetaXor) {
				block.addEntryPost(actInMetaXor);
				block.addExitPre(actInMetaXor);
			}
			block.setType("XOR");
			block.setExit(exitsGen.get(0));
			//System.out.println("actInMetaXor: " + actsInMetaXor + ", uniqueSetGen: " + uniqueSetGen);
			return block;
		}
		
		/**C2:针对XOR1(元选择前接),先利用Bag确认不重复exits集...................................*/
		Bag<String> bag = new HashBag<String>(exits);
		Set<String> uniqueSet = bag.uniqueSet();
		if (uniqueSet.size() == 0) {
			return null;
		}else {
			for (String exit : uniqueSet) {
				if (bag.getCount(exit) > 1) {
					if (!isValidExit(exit, flows)) {
						continue;
					}
					Block block = new Block();
					block.setEntry(place);
					List<String> actsInMetaXor = PetriUtils.getPreSet(exit, flows);
					for (String actInMetaXor : actsInMetaXor) {
						block.addEntryPost(actInMetaXor);
						block.addExitPre(actInMetaXor);
					}
					block.setType("XOR");
					block.setExit(exit);
					//System.out.println("actInMetaXor: " + actsInMetaXor + ", uniqueSet: " + uniqueSet);
					return block;
				}else {
					continue;
				}
			}
		}
	    return null;
		
	}
	
	//判断exit结点是否有效,参见测试用例中XOR1
	public boolean isValidExit(String exit, List<Flow> flows) {
		
		List<String> entries = new ArrayList<String>();
		List<String> preSet = PetriUtils.getPreSet(exit, flows);
		for (String tran : preSet) {
			List<String> places = PetriUtils.getPreSet(tran, flows);
			entries.addAll(places);
		}
		//利用Bag去重entries
		Bag<String> bag = new HashBag<String>(entries);
		Set<String> uniqueSet = bag.uniqueSet();
		if (uniqueSet.size() == 1) {
			return true;
		}
		return false;
		
	}
	
	//计算从place出发的变迁集(Note:排除到循环流集)
	public List<String> getToTransNotInLoop(String place, InnerNet net, List<Block> loopBlocks) {
		
		List<String> trans = new ArrayList<String>();
		//计算循环部件中从入口出发的活动集及到达出口的活动集
		List<String> entryActsInLoop = new ArrayList<String>();
		List<String> exitActsInLoop = new ArrayList<String>();
		for (Block block : loopBlocks) {
			entryActsInLoop.addAll(block.getEntryPost());
			exitActsInLoop.addAll(block.getExitPre());
		}
		//从place出发的流集(排除到循环流集)
		List<Flow> splitFlows = computeSplitFlows(place, net.getFlows(), entryActsInLoop);
		for (Flow flow : splitFlows) {
			String tran = flow.getFlowTo();
			trans.add(tran);
		}
		return trans;
	}
	
}
