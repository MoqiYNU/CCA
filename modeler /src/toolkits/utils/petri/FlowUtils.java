package toolkits.utils.petri;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import toolkits.def.petri.Flow;
import toolkits.def.petri.ProNet;

public class FlowUtils {
	
	//获得net的内网,即将消息库所全部移除
	public static ProNet getInterNet(List<String> msgPlaces, ProNet net) {
		
		//删除网中与msgPlaces有关的流集
		List<Flow> flows = FlowUtils.getReducedFlowsByObjs(msgPlaces, net);
		//库所集:网中库所集-消息库所
		List<String> places = (List<String>) CollectionUtils.subtract(net.getPlaces(), msgPlaces);
        
		//返回内网(内部若干组合网不需要考虑)
		ProNet interNet = new ProNet();
		interNet.setSource(net.getSource());
		interNet.setSinks(net.getSinks());
		interNet.setPlaces(places);
		interNet.setTrans(net.getTrans());
		interNet.setFlows(flows);
		interNet.setMsgPlaces(net.getMsgPlaces()); 
		interNet.setTranLabelMap(net.getTranLabelMap());
		
		return interNet;
       
	}
	
	//将flows中所有names重命名为name,主要用于库所合并
	public static List<Flow> rename(List<String> names, String name, List<Flow> flows) {
		
		List<Flow> renameFlows = new ArrayList<Flow>();
        for (Flow flow : flows) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (names.contains(from) && names.contains(to)) {
				Flow renameFlow = new Flow();
				renameFlow.setFlowFrom(name);
				renameFlow.setFlowTo(name);
				renameFlows.add(renameFlow);
			}else if (!names.contains(from) && names.contains(to)) {
				Flow renameFlow = new Flow();
				renameFlow.setFlowFrom(from);
				renameFlow.setFlowTo(name);
				renameFlows.add(renameFlow);
			}else if (names.contains(from) && !names.contains(to)) {
				Flow renameFlow = new Flow();
				renameFlow.setFlowFrom(name);
				renameFlow.setFlowTo(to);
				renameFlows.add(renameFlow);
			}else {
				renameFlows.add(flow);
			}
		}
        return renameFlows;
		
	}
	
	//获得与一组对象(库所或变迁)相关流集
	public static List<Flow> getRelatedFlowsByObjs(List<String> netObjs, ProNet net) {
		List<Flow> relatedFlows = new ArrayList<Flow>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (netObjs.contains(from) || netObjs.contains(to)) {
				relatedFlows.add(flow);
			}
		}
		return relatedFlows;
	}
	
	//由流集生成库所集
	public static List<String> genPlacesByFlows(List<Flow> reducedFlows, ProNet net) {
		
		List<String> reducedPlaces = new ArrayList<String>();
		List<String> places = net.getPlaces();
        for (Flow flow : reducedFlows) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				if (!reducedPlaces.contains(from)) {
					reducedPlaces.add(from);
				}
			}
			if (places.contains(to)) {
				if (!reducedPlaces.contains(to)) {
					reducedPlaces.add(to);
				}
			}
		}
        return reducedPlaces;
        
	}
	
	//由流生成变迁集
	public static List<String> genTransByFlows(List<Flow> reducedFlows, ProNet net) {
		
		List<String> reducedTrans = new ArrayList<String>();
        List<String> trans = net.getTrans();
        for (Flow flow : reducedFlows) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				if (!reducedTrans.contains(from)) {
					reducedTrans.add(from);
				}
			}
			if (trans.contains(to)) {
				if (!reducedTrans.contains(to)) {
					reducedTrans.add(to);
				}
			}
		}
        return reducedTrans;
		
	}
	
	//约减一个对象(库所或变迁)后形成流集
	public static List<Flow> getReducedFlowsByOneObj(String netObj, ProNet net) {
		
		List<Flow> reducedFlows = new ArrayList<Flow>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (netObj.equals(from) || netObj.equals(to)) {
				continue;
			}else {
				reducedFlows.add(flow);
			}
		}
		return reducedFlows;
		
	}
	
	//约减一组对象(库所或变迁)后形成流集
	public static List<Flow> getReducedFlowsByObjs(List<String> netObjs, ProNet net) {
		
		List<Flow> reducedFlows = new ArrayList<Flow>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (netObjs.contains(from) || netObjs.contains(to)) {
				continue;
			}else {
				reducedFlows.add(flow);
			}
		}
		return reducedFlows;
		
	}

}
