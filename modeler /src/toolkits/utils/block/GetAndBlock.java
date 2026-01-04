package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import toolkits.def.petri.Flow;
import toolkits.utils.petri.PetriUtils;

/**
 * @author Moqi
 * 计算内网中并发部件
 * Note:如果变迁t的后集大于1,则t必定为AndSplit
 * 而如果变迁t的前集大于1,则t必定为AndJoin
 */
public class GetAndBlock {
	
	private List<Block> metaANDBlocks;
	
	public GetAndBlock() {
		metaANDBlocks = new ArrayList<Block>();
	}
	
	public List<Block> getMetaANDBlocks() {
		return metaANDBlocks;
	}

	public void compute(InnerNet net) {
		
		//计算前先清空缓存metaANDBlocks
		metaANDBlocks.clear();
		
		//获取net中所有入口
		List<String> trans = net.getTrans();
		for (String tran : trans) {
			List<String> fromPlaces = PetriUtils.getPostSet(tran, net.getFlows());
			if (fromPlaces.size() > 1) {//并发变迁
				Block block = creatMetaANDBlock(tran, fromPlaces, net.getFlows());
				if (block != null) {
					metaANDBlocks.add(block);
				}
			}
		}
	}
	
	//创建元并发块(e.g., entry->p1->tran->p2->exit)
	public Block creatMetaANDBlock(String entry, List<String> fromPlaces, List<Flow> flows) {
		
		List<String> postActs1 = new ArrayList<String>();
		for (String place : fromPlaces) {
			List<String> preSet = PetriUtils.getPreSet(place, flows);
			List<String> postSet = PetriUtils.getPostSet(place, flows);
			if (postSet.size() == 1 && preSet.size() == 1) {
				postActs1.addAll(postSet);
			}else {
				return null;
			}
		}
		List<String> postActs2 = new ArrayList<String>();
        for (String tran : postActs1) {
        	List<String> preSet = PetriUtils.getPreSet(tran, flows);
			List<String> postSet = PetriUtils.getPostSet(tran, flows);
			if (postSet.size() == 1 && preSet.size() == 1) {
				postActs2.addAll(postSet);
			}else {
				return null;
			}
		}
        List<String> postActs3 = new ArrayList<String>();
        for (String place : postActs2) {
        	List<String> preSet = PetriUtils.getPreSet(place, flows);
			List<String> postSet = PetriUtils.getPostSet(place, flows);
			if (postSet.size() == 1 && preSet.size() == 1) {
				postActs3 = (List<String>) CollectionUtils.union(postActs3, postSet);
			}else {
				return null;
			}
		}
        
        if (postActs3.size() == 1) {//分支汇合为一个变迁,则为元并发结构
        	Block block = new Block();
			block.setEntry(entry);
			for (String place : fromPlaces) {//入口出发库所
				block.addEntryPost(place);
			}
			for (String place : postActs2) {//到达出口库所
				block.addExitPre(place);
			}
			block.setType("AND");
			block.setExit(postActs3.get(0));
			return block;
		}else {
			return null;
		}

	}
	
}

