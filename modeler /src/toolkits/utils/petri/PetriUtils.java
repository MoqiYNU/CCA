package toolkits.utils.petri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.collections4.CollectionUtils;

import toolkits.def.petri.Edge;
import toolkits.def.petri.Flow;
import toolkits.def.petri.Marking;
import toolkits.def.petri.ProNet;
import toolkits.def.petri.RG;
import toolkits.utils.block.Block;
import toolkits.utils.block.GetLoopBlock;
import toolkits.utils.block.InnerNet;
import toolkits.utils.block.ProTreeUtils;

/**
 * @author Moqi
 * 定义过程网的Utils
 */
public class PetriUtils {
	
	//定义非稳固集
	private Map<Marking, List<String>> noStubSetMap;
	private ProTreeUtils proTreeUtils;
	
	public PetriUtils() {
		noStubSetMap = new LinkedHashMap<Marking, List<String>>();
		proTreeUtils = new ProTreeUtils();
	}
	
	//记录RRG中每个标识未触发非稳固集
	public Map<Marking, List<String>> getNoStubSetMap() {
		return noStubSetMap;
	}
	
	
	/******************************在过程网的并发和循环前后添加控制变迁******************************/
	
	public ProNet updateProNetWithCtrlTrans(ProNet proNet, int i) {
		
		int placeIndex = 0;
		int tranIndex = 0;
		
		List<String> andSplits = getAndSplits(proNet);
		List<String> andJoins = getAndJoins(proNet);
		List<String> loopPlaces = getLoopPlaces(proNet);
		List<String> inLoopTrans = getAllInLoopTrans(proNet);
		List<String> outLoopTrans = getAllOutLoopTrans(proNet);
		
		// 1.添加控制变迁以标识And-Split(ie.,andSplit->ctrlPlace->ctrlTran->andSplit·)
		for (String andSplit : andSplits) {
			
			String ctrlPlace = "fp" + i + placeIndex;
			placeIndex ++;
			String ctrlTran = "fc" + i + tranIndex;
			tranIndex ++;
			//Note:获取未更新的后集
			List<String> postSet = getPostSet(andSplit, proNet.getFlows());
			
			proNet.addCtrlTran(ctrlTran);
			proNet.addTran(ctrlTran);
			// 控制变迁的label是其Id
			proNet.getTranLabelMap().put(ctrlTran, ctrlTran);
			proNet.addPlace(ctrlPlace);
			
			proNet.addFlow(andSplit, ctrlPlace);
			proNet.addFlow(ctrlPlace, ctrlTran);
			
			for (String place : postSet) {
				proNet.rovFlow(andSplit, place);
				proNet.addFlow(ctrlTran, place);
			}
		}
		
		// 2.添加控制变迁以标识And-Join(ie.,·andJoin->ctrlTran->ctrlPlace->->andJoin)
        for (String andJoin : andJoins) {
        	
        	String ctrlPlace = "fp" + i + placeIndex;
			placeIndex ++;
        	String ctrlTran = "fc" + i + tranIndex;
			tranIndex ++;
			//Note:获取未更新的前集
			List<String> preSet = getPreSet(andJoin, proNet.getFlows());
			
			proNet.addCtrlTran(ctrlTran);
			proNet.addTran(ctrlTran);
			//控制变迁的label是其Id
			proNet.getTranLabelMap().put(ctrlTran, ctrlTran);
			proNet.addPlace(ctrlPlace);
			
			proNet.addFlow(ctrlTran, ctrlPlace);
			proNet.addFlow(ctrlPlace, andJoin);
			
			for (String place : preSet) {
				proNet.rovFlow(place, andJoin);
				proNet.addFlow(place, ctrlTran);
			}
		}
        
        // 3.在每个循环库所前后各添加一个控制变迁
        for (String loopPlace : loopPlaces) {
        	
        	List<String> preSet = (List<String>) CollectionUtils.subtract(getPreSet(loopPlace, proNet.getFlows()), outLoopTrans);
        	List<String> postSet = (List<String>) CollectionUtils.subtract(getPostSet(loopPlace, proNet.getFlows()), inLoopTrans);
            
        	String ctrlPlace1 = "fp" + i + placeIndex;
			placeIndex ++;
        	String ctrlTran1 = "fc" + i + tranIndex;
			tranIndex ++;
			
			proNet.addCtrlTran(ctrlTran1);
			proNet.addTran(ctrlTran1);
			//控制变迁的label是其Id
			proNet.getTranLabelMap().put(ctrlTran1, ctrlTran1);
			proNet.addPlace(ctrlPlace1);
			
			proNet.addFlow(ctrlPlace1, ctrlTran1);
			proNet.addFlow(ctrlTran1, loopPlace);
			
			for (String tran : preSet) {
				proNet.rovFlow(tran, loopPlace);
				proNet.addFlow(tran, ctrlPlace1);
			}
			
			String ctrlPlace2 = "fp" + i + placeIndex;
			placeIndex ++;
        	String ctrlTran2 = "fc" + i + tranIndex;
			tranIndex ++;
			
			proNet.addCtrlTran(ctrlTran2);
			proNet.addTran(ctrlTran2);
			//控制变迁的label是其Id
			proNet.getTranLabelMap().put(ctrlTran2, ctrlTran2);
			proNet.addPlace(ctrlPlace2);
			
			proNet.addFlow(loopPlace, ctrlTran2);
			proNet.addFlow(ctrlTran2, ctrlPlace2);
			
			for (String tran : postSet) {
				proNet.rovFlow(loopPlace, tran);
				proNet.addFlow(ctrlPlace2, tran);
			}
        	
        }
		
        return proNet;
		
	}
	
	//返回退出循环的变迁集
	public List<String> getAllOutLoopTrans(ProNet proNet) {
		List<String> outLoopTrans = new ArrayList<>();
		InnerNet innerNet1 = proTreeUtils.rovLinkPlaces(proNet.getInnerNet());
		InnerNet innerNet = proTreeUtils.rovRedPlaces(innerNet1);
		GetLoopBlock getLoopBlock = new GetLoopBlock();
		getLoopBlock.compute(innerNet);
		List<Block> blocks = getLoopBlock.getLoopBlocks();
		for (Block block : blocks) {
			List<String> out = block.getExitPre();
			// 避免重复添加
			outLoopTrans = (List<String>) CollectionUtils.union(outLoopTrans, out);
		}
		return outLoopTrans;
	}
		
	//返回进入循环的变迁集
	public List<String> getAllInLoopTrans(ProNet proNet) {
		List<String> inLoopTrans = new ArrayList<>();
		InnerNet innerNet1 = proTreeUtils.rovLinkPlaces(proNet.getInnerNet());
		InnerNet innerNet = proTreeUtils.rovRedPlaces(innerNet1);
		GetLoopBlock getLoopBlock = new GetLoopBlock();
		getLoopBlock.compute(innerNet);
		List<Block> blocks = getLoopBlock.getLoopBlocks();
		for (Block block : blocks) {
			List<String> in = block.getEntryPost();
			// 避免重复添加
			inLoopTrans = (List<String>) CollectionUtils.union(inLoopTrans, in);
		}
		return inLoopTrans;
	}
		
	//获取过程网中所有环的导致库所
	public List<String> getLoopPlaces(ProNet proNet) {
		List<String> loopPlaces = new ArrayList<>();
		InnerNet innerNet1 = proTreeUtils.rovLinkPlaces(proNet.getInnerNet());
		InnerNet innerNet = proTreeUtils.rovRedPlaces(innerNet1);
		GetLoopBlock getLoopBlock = new GetLoopBlock();
		getLoopBlock.compute(innerNet);
		List<Block> blocks = getLoopBlock.getLoopBlocks();
		for (Block block : blocks) {
			String place = block.getEntry();
			//避免重复添加
			if (!loopPlaces.contains(place)) {
				loopPlaces.add(place);
			}
		}
		return loopPlaces;
	}
	
	//获取过程网中所有的And-Split
	public List<String> getAndSplits(ProNet proNet) {
		List<String> andSplits = new ArrayList<>();
		List<String> linkPlaces = proNet.getLinkPlaces();
		List<String> trans = proNet.getTrans();
		for (String tran : trans) {
			if (CollectionUtils.subtract(getPostSet(tran, proNet.getFlows()), linkPlaces).size() > 1) {
				andSplits.add(tran);
			}
		}
		return andSplits;
	}
	
	//获取过程网中所有的And-Join
	public List<String> getAndJoins(ProNet proNet) {
		List<String> andJoins = new ArrayList<>();
		List<String> linkPlaces = proNet.getLinkPlaces();
		List<String> trans = proNet.getTrans();
		for (String tran : trans) {
			if (CollectionUtils.subtract(getPreSet(tran, proNet.getFlows()), linkPlaces).size() > 1) {
				andJoins.add(tran);
			}
		}
		return andJoins;
	}

	
	/******************************产生稳固集(Note:未考虑忽视)********************************/
	
	public List<String> getStubSet(ProNet proNet, Marking marking) {
		
		List<String> S = new ArrayList<String>();//返回稳固集
		Queue<String> U = new LinkedList<>();//未处理迁移集
		
		//所有使能活动集合
		List<String> enableTrans = getEnableTrans(proNet, marking.getPlaces());
		if (enableTrans.size() == 0) {//没有使能迁移,直接返回空稳固集S
			return S;
		}else {//有使能迁移,随机选择一个使能迁移
			
			String firstAct = enableTrans.get(0);
			S.add(firstAct);
			U.add(firstAct);
			
		    while(U.size() > 0){//迭代计算
		    	List<String> N;
		    	//出对一个变迁,以此进行计算
			    String actFrom = U.poll();
			    if (enableTrans.contains(actFrom)) {//在marking处使能,则获取冲突迁移集
			    	N = getDisablingTrans(proNet, actFrom);
			    	//System.out.println("Disabling: " + N);
				}else {//在marking处不使能,则获取导致其使能迁移集
					N = getEnablingTrans(proNet, actFrom, marking);
					//System.out.println("Enabling: " + N);
				}
			    
			    List<String> subSet = (List<String>) CollectionUtils.subtract(N, S);//避免重复添加
			    for (String subElem : subSet) {
					if (!U.contains(subElem)) {
						U.add(subElem);
					}
				}
			    //System.out.println("U: " + U);
                for (String elem : N) {//添加稳固集到S
					if (!S.contains(elem)) {
						S.add(elem);
					}
				}
		    }
		    return S;
		}
	}
	
	//获取导致变迁使能的变迁集(以Id标识)
	public List<String> getEnablingTrans(ProNet proNet, String tran, Marking marking) {
		
		List<String> trans = new ArrayList<String>();
		
		List<String> preSet = getPreSet(tran, proNet.getFlows());
		for (String place : preSet) {
			if (marking.getPlaces().contains(place)) {//跳过已经含有托肯的库所
				continue;
			}
			List<String> tempTrans = getPreSet(place, proNet.getFlows());
			//System.out.println("tempTranIds: " + tempTranIds);
			for (String tempTran : tempTrans) {
				if (!trans.contains(tempTran)) {
					trans.add(tempTran);
				}
			}
		}
		return trans;
	}
	
	//获取变迁的冲突变迁集(以tranId标识)
	public List<String> getDisablingTrans(ProNet proNet, String tran) {
		
		List<String> trans = new ArrayList<String>();
		
		List<String> preSet = getPreSet(tran, proNet.getFlows());
		for (String tempTran : proNet.getTrans()) {
			if (tran.equals(tempTran)) {//不包括自己
				continue;
			}
			List<String> tempPreSet = getPreSet(tempTran, proNet.getFlows());
			if (CollectionUtils.intersection(preSet, tempPreSet).size() > 0) {//前集相交
				trans.add(tempTran);
			}
		}
		return trans;
	}
	

	/******************************产生Petri网可达图********************************/
	
	//利用稳固集从开放网中产生其约减可达图,即每次迁移稳固集(Note:未考虑忽视问题)
	public RG genRGWithStubSet(ProNet proNet) {
		
		noStubSetMap.clear();
		
		//RRG中变迁到标号映射
		Map<String, String> tranLabelMap = new HashMap<String, String>();
		
		//开始标识
		Marking initMarking = proNet.getSource();
		
		//终止标识
		List<Marking> finalMarkings = proNet.getSinks();
		
		List<Edge> edges = new ArrayList<Edge>();
		
		//即将访问的队列visitingQueue和已经访问过队列visitedQueue
		Queue<Marking> visitingQueue = new LinkedList<>(); 
		List<Marking> visitedQueue = new ArrayList<>();
		//将初始标识入队并置为已经访问
		visitingQueue.offer(initMarking);
		visitedQueue.add(initMarking);
		
		//迭代计算
	    while(visitingQueue.size() > 0){

		    //出对一个标识,以此进行迁移
		    Marking markingFrom = visitingQueue.poll();
		    List<String> placesFrom = markingFrom.getPlaces();
		    
		    //System.out.println("markingFrom: " + markingFrom.getPlaces());
		    
		    //markingFrom下所有使能活动集
			List<String> allEnableTrans = getEnableTrans(proNet, placesFrom);
		    //获得稳固集(未考虑消除忽视问题)
			List<String> S = getStubSet(proNet, markingFrom);
			
			//Note:只迁移稳固集中使能活动
			List<String> enableTrans = (List<String>) CollectionUtils.intersection(allEnableTrans, S);
			System.out.println("Marking" + markingFrom.getPlaces() + " Stub set: " + S);
			System.out.println("Stub set: " + S + ", all enable acts: " + allEnableTrans);
			
			List<String> noStubSet = (List<String>) CollectionUtils.subtract(allEnableTrans, S); 
			noStubSetMap.put(markingFrom, noStubSet);
			System.out.println("Marking" + markingFrom.getPlaces() + " no Stub set: " + noStubSet);

			//利用稳固集中使能变迁产生后继
            for (String tran : enableTrans) {
				
				List<String> placesTo = getPlacesTo(proNet, placesFrom, tran);
				Marking markingTo = new Marking();
				markingTo.setPlaces(placesTo);
				
				Edge edge = new Edge();
				edge.setFrom(markingFrom);
				
				edge.setTran(tran);
				edge.setTo(markingTo);
				edges.add(edge);
				
				tranLabelMap.put(tran, getLabel(proNet.getTranLabelMap(), tran));
				
				if (!MarkingUtils.markingIsExist(visitedQueue, markingTo)) {
					visitingQueue.offer(markingTo);
					visitedQueue.add(markingTo);
				}
				
			}
	    }
	    
	    //Note:终止标识中可含资源库所
	    List<Marking> ends = new ArrayList<Marking>();
        for (Marking marking : visitedQueue) {
        	List<String> tempPlaces = new ArrayList<String>();
			List<String> places = marking.getPlaces();
			for (String place : places) {
				if (!proNet.getResPlaces().contains(place)) {
					tempPlaces.add(place);
				}
			}
			Marking tempMarking = new Marking();
			tempMarking.setPlaces(tempPlaces);
			if (MarkingUtils.markingIsExist(finalMarkings, tempMarking)) {
				ends.add(marking);
			}
		}
	    
	    RG rrg = new RG();
	    rrg.setStart(initMarking);
	    rrg.setEnds(ends);
	    rrg.setVertexs(visitedQueue);
	    rrg.setEdges(edges);
	    rrg.setTranLabelMap(tranLabelMap);
	    return rrg;
		
	}
	
	
	// 2.利用传统方法从过程网中产生其可达图
	public static RG genRG(ProNet proNet) {
		
		Map<String, String> tranLabelMap = new HashMap<String, String>();//定义标号函数
		
		//开始标识
		Marking initMarking = proNet.getSource();
		//终止标识
		List<Marking> finalMarkings = proNet.getSinks();
		List<Edge> edges = new ArrayList<Edge>();
		
		//即将访问的队列visitingQueue和已经访问过队列visitedQueue
		Queue<Marking> visitingQueue = new LinkedList<>(); 
		List<Marking> visitedQueue = new ArrayList<>();
		//将初始标识入队并置为已经访问
		visitingQueue.offer(initMarking);
		visitedQueue.add(initMarking);
		
		//迭代计算
	    while(visitingQueue.size() > 0){

		    //出对一个标识,以此进行迁移
		    Marking markingFrom = visitingQueue.poll();
		    List<String> placesFrom = markingFrom.getPlaces();
		    
		    //所有使能活动集合
		    List<String> enableTrans = getEnableTrans(proNet, placesFrom);
		    
			for (String tran : enableTrans) {
				List<String> placesTo = getPlacesTo(proNet, placesFrom, tran);
				Marking markingTo = new Marking();
				markingTo.setPlaces(placesTo);
				Edge edge = new Edge();
				edge.setFrom(markingFrom);
				edge.setTran(tran);
				edge.setTo(markingTo);
				tranLabelMap.put(tran, getLabel(proNet.getTranLabelMap(), tran));
				edges.add(edge);
				if (!MarkingUtils.markingIsExist(visitedQueue, markingTo)) {
					visitingQueue.offer(markingTo);
					visitedQueue.add(markingTo);
				}
			}
	    }
	    
	    //Note:终止标识中可含资源库所
	    List<Marking> ends = new ArrayList<Marking>();
        for (Marking marking : visitedQueue) {
        	List<String> tempPlaces = new ArrayList<String>();
			List<String> places = marking.getPlaces();
			for (String place : places) {
				if (!proNet.getResPlaces().contains(place)) {
					tempPlaces.add(place);
				}
			}
			Marking tempMarking = new Marking();
			tempMarking.setPlaces(tempPlaces);
			if (MarkingUtils.markingIsExist(finalMarkings, tempMarking)) {
				ends.add(marking);
			}
		}
	    
	    RG rg = new RG();
	    rg.setStart(initMarking);
	    rg.setEnds(ends);
	    rg.setVertexs(visitedQueue);
	    rg.setEdges(edges);
	    rg.setTranLabelMap(tranLabelMap);
	    return rg;
		
	}
	
	
	//获得tran对应的label
	public static String getLabel(Map<String, String> tranLabelMap, String tran) {
		String label = tranLabelMap.get(tran);
		//System.out.println("tranId: " + tran + ", " + label);
		if (label == null) {
			return "sync";
		}else {
			return label;
		}
	}
	
	//确定tran在第几个过程网中出现
	public static int getProNetIndex(List<ProNet> proNets, String tran) {
		int index = 0;
		for (ProNet proNet : proNets) {
			if (proNet.getTrans().contains(tran)) {
				return index;
			}
			index ++;
		}
		return -1;
		
	}
	
	//获取tran的位置标记
	public static int getTranIndex(String tran, List<String> trans) {
		int index = 0;
		for (String tempTran : trans) {
			if (tempTran.equals(tran)) {
				return index;
			}
			index ++;
		}
		return -1;
		
	}
	
	/**********************定义P/T网中活动使能,点火规则及求库所/变迁前后集*********************/
	
	//获得tran的后继库所集合(由Petri导论P/T变迁规则确定)
	public static List<String> getPlacesTo(ProNet proNet, List<String> places, String tran) {
		
		//获得tran的前集和后集
		List<String> preSet = getPreSet(tran, proNet.getFlows());
		List<String> postSet = getPostSet(tran, proNet.getFlows());
		
		//获得当前标识中的条件集
	    List<String> placesFrom = places;
	    List<String> placesTo = new ArrayList<String>();
	    
	    //1.1 preSet-postSet
	    List<String> preSetNotInPostSet = new ArrayList<String>();
	    //1.2 postSet-preSet
	    List<String> postSetNotInPreSet = new ArrayList<String>();
	    //1.3 else
	    List<String> elseSet = new ArrayList<String>();
	    
        for (String placeFrom : placesFrom) {
			if (preSet.contains(placeFrom) && !postSet.contains(placeFrom)) {
				preSetNotInPostSet.add(placeFrom);
			}else if (postSet.contains(placeFrom) && !preSet.contains(placeFrom)) {
				postSetNotInPreSet.add(placeFrom);
			}else {
				elseSet.add(placeFrom);
			}
		}
	    
        // 1.前集减1,使用CollectionUtils中的差运算
        placesTo.addAll((List<String>) CollectionUtils.subtract(preSetNotInPostSet, preSet));
        // 2.后集在以前基础上加1
        placesTo.addAll(postSetNotInPreSet);
        for (String post : postSet) {
			if (!preSet.contains(post)) {
				placesTo.add(post);
			}
		}
        // 3.剩下不变
        placesTo.addAll(elseSet);
        
		return placesTo;
	}
	
	//获得places下所有使能变迁(P/T网)
	public static List<String> getEnableTrans(ProNet openNet, List<String> places) {
		List<String> enableTrans = new ArrayList<String>();
		//计算每个活动是否使能
		List<String> trans = openNet.getTrans();
		for (String tran : trans) {
			if (isEnable(openNet, places, tran)) {
				enableTrans.add(tran);
			}
		}
		return enableTrans;
	}
		
	//判断tran是否能够点火(P/T网)
	public static boolean isEnable(ProNet openNet, List<String> places, String tran) {
		//获得tran的前集
		List<String> preSet = getPreSet(tran, openNet.getFlows());
		//如果tran的前集属于places,则可以点火
		if (CollectionUtils.isSubCollection(preSet, places)) {
			return true;
		}
		return false;
	}
	
	//获取元素elem(库所或变迁)的前集
	public static List<String> getPreSet(String elem, List<Flow> flows) {
		List<String> preSet = new ArrayList<String>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (elem.equals(to)) {
				if (!preSet.contains(from)) {
					preSet.add(from);
				}
			}
		}
		return preSet;
	}
	
	//获取元素elem(库所或变迁)的后集
	public static List<String> getPostSet(String elem, List<Flow> flows) {
		List<String> postSet = new ArrayList<String>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (elem.equals(from)) {
				if (!postSet.contains(to)) {
					postSet.add(to);
				}
			}
		}
		return postSet;
	}

}
