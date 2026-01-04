package pipe.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.collections4.CollectionUtils;

import pipe.dataLayer.AnnotationNote;
import pipe.dataLayer.Arc;
import pipe.dataLayer.DataLayer;
import pipe.dataLayer.Place;
import pipe.dataLayer.Transition;
import toolkits.def.petri.Flow;
import toolkits.def.petri.Marking;
import toolkits.def.petri.ProNet;
import toolkits.utils.petri.PetriUtils;

/**
 * @author Moqi
 * Pipe中数据层Utils
 */
public class DataLayerUtils {
	
	
	/*************************从dataLayer中产生多个开放网*************************/
	
	public List<ProNet> genProNetsFromDL(DataLayer dataLayer) {

		//最终获得开放网(相较中间网,最终网中消息库所用label替代)
		List<ProNet> openNets = new ArrayList<ProNet>();

		//存储切分得到中间网
		List<ProNet> interOpenNets = new ArrayList<ProNet>();

		//获取DataLayer中的相关数据
		Place[] places = dataLayer.getPlaces();
		Marking initMarking = dataLayer.createInitMarking();
		List<String> initPlaces = initMarking.getPlaces();
		Transition[] transitions = dataLayer.getTransitions();
//		List<String> ctrlTrans = dataLayer.getCtrlTrans();//控制变迁集
		Arc[] arcs = dataLayer.getArcs();
	    List<String> msgPlaces = dataLayer.getMsgPlaces();//用Id标识
	    List<String> resPlaces = dataLayer.getResPlaces();//用Id标识
//	    List<String> linkPlaces = dataLayer.getLinkPlaces();//用Id标识
	    //System.out.println("msgPlaces: " + msgPlaces);
	    AnnotationNote[] annotationNotes = dataLayer.getLabels();
	    
	    // 1.获得所有终止标识
	    List<Marking> sinks = new ArrayList<Marking>();
        for (AnnotationNote annotationNote : annotationNotes) {
			if (annotationNote.isFinalMarkingNote()) {
				String marksStr = annotationNote.getText();
				sinks.addAll(parseMarkSet(marksStr));
			}
		}
	    
	    // 2.根据每个变迁获取开放网片段,并以此构建中间网(中间开放网中只含库所,变迁及流)
	    List<String> visitedTrans = new ArrayList<>();
	    for (Transition tran : transitions) {
	    	
	    	String tranId = tran.getId();
	    	
	    	if (visitedTrans.contains(tranId)) {//若生成过,则跳过
				continue;
			}
	    	
	    	//待生成开放网
	    	ProNet interOpenNet = new ProNet();
	    	Bag bag = getBag(arcs, tranId);
	    	List<String> tranIdsInBag = bag.getTrans();
	    	
	    	// 2.1确定变迁
	    	for (String tranIdInBag : tranIdsInBag) {
	    		interOpenNet.addTran(tranIdInBag);
	    	}
	    	
	    	// 2.2确定库所
	    	List<String> tempPlaces = new ArrayList<String>();
	    	for (String tranIdInBag : tranIdsInBag) {
		    	List<String> preSet = getPreSet(arcs, tranIdInBag);
		    	for (String place : preSet) {
					if (!tempPlaces.contains(place)) {
						tempPlaces.add(place);
					}
				}
		    	List<String> postSet = getPostSet(arcs, tranIdInBag);
		    	for (String place : postSet) {
		    		if (!tempPlaces.contains(place)) {
						tempPlaces.add(place);
					}
				}
			}
	    	interOpenNet.addPlaces(tempPlaces);
	    	
	    	// 2.3确定流关系
	    	for (String tranIdInBag : tranIdsInBag) {
		    	interOpenNet.addFlows(getFlowsByTran(arcs, tranIdInBag));
	    	}
			interOpenNets.add(interOpenNet);
			
			//置tranIdsInBag已被访问
			visitedTrans.addAll(tranIdsInBag);
			//System.out.println("PN size: " + petris.size() + " Visiting tran: " + tranId + " Bag: " + tranIdsInBag);
	    }
	    
	    // 3.针对每个中间网,计算最终网
        for (ProNet interOpenNet : interOpenNets) {

			//获取中间网中库所,变迁及流关系
        	List<String> interPlaces = interOpenNet.getPlaces();
        	List<String> interTrans = interOpenNet.getTrans();
        	List<Flow> interFlows = interOpenNet.getFlows();

			//待生成Petri网
			ProNet openNet = new ProNet();

			//设置资源库所集:用Name标识(需共享)且避免重复
            List<String> rPlaces = new ArrayList<String>();
			for (String rPace : resPlaces) {
				if (interPlaces.contains(rPace)) {
					rPlaces.add(rPace);
				}
			}
			for (String rPlace : rPlaces) {
				openNet.addResPlace(getPlaceLabel(places, rPlace));
			}

			//设置源库所和资源
			List<String> tempPlaces = new ArrayList<String>();
			for (String initPlace : initPlaces) {
				if (interPlaces.contains(initPlace)) {
					tempPlaces.add(initPlace);
				}
			}
			//Note:sourcePlaces不含资源库所
			List<String> sourcePlaces = new ArrayList<String>();
			List<String> resources = new ArrayList<String>();
			for (String tempPlace : tempPlaces) {
				if (rPlaces.contains(tempPlace)) {
					//添加资源,即资源库所的Name
					resources.add(getPlaceLabel(places, tempPlace));
				}else {
					sourcePlaces.add(tempPlace);
				}
			}
			
			Marking source = new Marking();
			//源库所中不含资源库所
			source.addPlaces(sourcePlaces);
			openNet.setSource(source);
			openNet.setResources(resources);

			//设置消息库所(用Name标识,且避免重复)
			List<String> mPlaces = new ArrayList<String>();
			
			for (String mPlace : msgPlaces) {
				if (interPlaces.contains(mPlace)) {
					mPlaces.add(mPlace);
				}
			}
            for (String mPlace : mPlaces) {
				openNet.addMsgPlace(getPlaceLabel(places, mPlace));
			}
            
            //设置链接库所(用Id标识,且避免重复)
//            List<String> lPlaces = new ArrayList<String>();
//            for (String lPlace : linkPlaces) {
//				if (interPlaces.contains(lPlace)) {
//					lPlaces.add(lPlace);
//				}
//			}
//            for (String lPlace : lPlaces) {
//				openNet.addLinkPlace(lPlace);
//			}

			//设置Places
            for (String interPlace : interPlaces) {
				if (mPlaces.contains(interPlace)) {
					openNet.addPlace(getPlaceLabel(places, interPlace));
				}else if (rPlaces.contains(interPlace)) {
					openNet.addPlace(getPlaceLabel(places, interPlace));
				}else{
					openNet.addPlace(interPlace);
				}
			}

			//设置终止标识集
			List<Marking> fMarkings = new ArrayList<Marking>();
			for (Marking sink : sinks) {
				if (interPlaces.containsAll(sink.getPlaces())) {
					fMarkings.add(sink);
				}
			}
			openNet.setSinks(fMarkings);

			//设置活动及控制活动
			List<String> trans = interOpenNet.getTrans();
			openNet.setTrans(trans);
//			for (String tran : trans) {
//				if (ctrlTrans.contains(tran)) {
//					openNet.addCtrlTran(tran);
//				}
//			}
			
			//设置Flow
			for (Flow flow : interFlows) {
		    	   String flowFrom = flow.getFlowFrom();
		    	   String flowTo = flow.getFlowTo();
		    	   if (mPlaces.contains(flowFrom)) {
				       Flow fFlow = new Flow();
				       fFlow.setFlowFrom(getPlaceLabel(places, flowFrom)); 
				       fFlow.setFlowTo(flowTo);
				       openNet.addFlow(fFlow);
				   }else if (mPlaces.contains(flowTo)) {
					   Flow fFlow = new Flow();
				       fFlow.setFlowFrom(flowFrom); 
				       fFlow.setFlowTo(getPlaceLabel(places, flowTo));
				       openNet.addFlow(fFlow);
				   }else if (rPlaces.contains(flowFrom)) {
					   Flow rFlow = new Flow();
				       rFlow.setFlowFrom(getPlaceLabel(places, flowFrom)); 
				       rFlow.setFlowTo(flowTo);
				       openNet.addFlow(rFlow);
				   }else if (rPlaces.contains(flowTo)) {
					   Flow rFlow = new Flow();
				       rFlow.setFlowFrom(flowFrom); 
				       rFlow.setFlowTo(getPlaceLabel(places, flowTo));
				       openNet.addFlow(rFlow);
				   }else{
					   openNet.addFlow(flow);
				   }
			}

			//设置输入和输出消息库所集
			List<String> msgPls = openNet.getMsgPlaces();
			for (String msgPl : msgPls) {
				List<String> preSet = PetriUtils.getPreSet(msgPl, openNet.getFlows());
				if (preSet.size() == 0) {//1.前集为空,则为输入消息库所
					openNet.addInputMsgPlace(msgPl);
				}else {//2.前集不为空,则为输出消息库所
					openNet.addOutputMsgPlace(msgPl);
				}
			}

			//设置请求资源变迁集和资源属性映射
			Map<String, List<String>> reqResMap = new HashMap<String, List<String>>();
			Map<String, Integer> resProperMap = new HashMap<String, Integer>();
			List<String> resPls = openNet.getResPlaces();
			for (String resPl : resPls) {
				List<String> preSet = PetriUtils.getPreSet(resPl, openNet.getFlows());
				List<String> postSet = PetriUtils.getPostSet(resPl, openNet.getFlows());
				if (postSet.size() != 0) {//请求资源变迁集
					reqResMap.put(resPl, postSet);
				}
				if (preSet.size() != 0 && postSet.size() != 0) {//1.前后集均不为空,则为重复利用资源
					resProperMap.put(resPl, 0);
				}else {//2.否则为消耗资源
					resProperMap.put(resPl, 1);
				}
			}
			openNet.setResProperMap(resProperMap);
			openNet.setReqResMap(reqResMap);

			//设置变迁Id及名字映射
			Map<String, String> tranLabelMap = new HashMap<String, String>();
			for (String interTran : interTrans) {
				tranLabelMap.put(interTran, getTranLabel(transitions, interTran));
			}
			openNet.setTranLabelMap(tranLabelMap);
			
			openNets.add(openNet);
		}
        
        /*//打印每个开放网
	    int size = openNets.size();
	    for (int i = 0; i < size; i++) {
	    	System.out.println("Print each petri net......" + "\n");
	    	PrintUtils.printNet(openNets.get(i));
	    	System.out.println("CtrlTrans: " + openNets.get(i).getCtrlTrans());
		}*/
        
        return openNets;
        
	}

	//获得库所的标号
	public String getPlaceLabel(Place[] places, String placeId) {
		for (Place place : places) {
			String tempId = place.getId();
			String label = place.getName();
			if (tempId.equals(placeId)) {
				return label;
			}
		}
		return null;
	}

	//获得变迁的标号
	public String getTranLabel(Transition[] trans, String tranId) {
		for (Transition tran : trans) {
			String tempId = tran.getId();
			String label = tran.getName();
			if (tempId.equals(tranId)) {
				return label;
			}
		}
		return null;
	}

	//判断当前片段是否产生,若产生则返回对应开放网(以index标识)
	public int isInGenPNs(List<ProNet> openNets, List<String> tempPlaces) {
		if (openNets.size() == 0) {
			return -1;
		}else {
			int size = openNets.size();
			for (int i = 0; i < size; i++) {
				List<String> places = openNets.get(i).getPlaces();
				//System.out.println("Places in: " + i + places);
				//Note:???????,??size???
				if (CollectionUtils.intersection(places, tempPlaces).size() != 0) {
					return i;
				}
			}
		}
		return -1;
	}

	//获得变迁对应的flow
	public List<Flow> getFlowsByTran(Arc[] arcs, String tran) {
		List<Flow> flows = new ArrayList<Flow>();
		for (Arc arc : arcs) {
			String from = arc.getSource().getId();
	    	String to = arc.getTarget().getId();
	    	if (from.equals(tran) || to.equals(tran)) {
	    		Flow flow = new Flow();
	    		flow.setFlowFrom(from);
	    		flow.setFlowTo(to);
	    		flows.add(flow);
			}
		}
		return flows;
	}

	//获取变迁的前集
	public List<String> getPreSet(Arc[] arcs, String tran) {
		List<String> preSet = new ArrayList<String>();
		for (Arc arc : arcs) {
			String from = arc.getSource().getId();
	    	String to = arc.getTarget().getId();
	    	if (tran.equals(to)) {
	    		if (!preSet.contains(from)) {
					preSet.add(from);
				}
			}
		}
		return preSet;
	}

	//获取变迁的后集
	public List<String> getPostSet(Arc[] arcs, String tran) {
		List<String> postSet = new ArrayList<String>();
		for (Arc arc : arcs) {
			String from = arc.getSource().getId();
	    	String to = arc.getTarget().getId();
	    	if (tran.equals(from)) {
	    		if (!postSet.contains(to)) {
	    			postSet.add(to);
				}
			}
		}
		return postSet;
	}

	//获得Id为tranId的变迁的包
	public Bag getBag(Arc[] arcs, String tranId) {
		
		Queue<String> visitingQueue = new LinkedList<>(); 
  		List<String> visitedQueue = new ArrayList<>(); 
  		visitingQueue.offer(tranId);
  		visitedQueue.add(tranId);//包括tranId自己

		//迭代计算
  		while(visitingQueue.size() > 0){

			//出对一个变迁(即Id),以此进行前后扩张
  			String tranIdFrom = visitingQueue.poll();

			//tempPlaces用于存储tran的前集和后集,避免自循环前后集重复
  	    	List<String> tempPlaces = new ArrayList<String>();
  	    	List<String> preSetPlace = getPreSet(arcs, tranIdFrom);
  	    	for (String place : preSetPlace) {
  				if (!tempPlaces.contains(place)) {
  					tempPlaces.add(place);
  				}
  			}
  	    	List<String> postSetPlace = getPostSet(arcs, tranIdFrom);
  	    	for (String place : postSetPlace) {
  	    		if (!tempPlaces.contains(place)) {
  					tempPlaces.add(place);
  				}
  			}

			//tempTrans用于存储tran可达的前后迁移集,避免自循环前后集重复
  	    	List<String> tempTrans = new ArrayList<String>();
            for (String tempPlace : tempPlaces) {
				List<String> preSetTran = getPreSet(arcs, tempPlace);
				for (String preTran : preSetTran) {
					if (!tempTrans.contains(preTran)) {
						tempTrans.add(preTran);
					}
				}
				List<String> postSetTran = getPostSet(arcs, tempPlace);
				for (String postTran : postSetTran) {
					if (!tempTrans.contains(postTran)) {
						tempTrans.add(postTran);
					}
				}
			}
  			
  			for (String tempTran : tempTrans) {
				if (!visitedQueue.contains(tempTran)) {
					visitingQueue.offer(tempTran);
			  		visitedQueue.add(tempTran);
				}
			}
  		}
  		Bag bag = new Bag();
  		bag.addTrans(visitedQueue);
		return bag;
	}
	
	/******************************解析终止标识******************************/

	//解析每个组织对应字符串标识,这些标识件用";"分隔,例如1*P1+2*P2;2*P3+3*P4
	public List<Marking> parseMarkSet(String marksStr) {
		List<Marking> markings = new ArrayList<Marking>();
		//每个组织的终止标识字符串,多个标识以";"分隔
		String[] marks = marksStr.split(";");
		for (String mark : marks) {
			Marking tempMarking = parseOneMark(mark);
			markings.add(tempMarking);
			System.out.println("split marking: " + tempMarking.getPlaces());
		}
		return markings;
	}

	//解析一个字符串标识,例如1*P1+2*P3+3*P4
	public Marking parseOneMark(String markStr) {
		Marking marking = new Marking();
		List<String> places = new ArrayList<String>();
		String[] maps = markStr.split("\\+");
		for (String map : maps) {
			String[] mappingArray = map.split("\\*");
			int num = Integer.parseInt(mappingArray[0].trim());
			for (int i = 0; i < num; i++) {
				places.add(mappingArray[1].trim());
			}
		}
		marking.setPlaces(places);
		return marking;
	}
	
}
