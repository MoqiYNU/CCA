package toolkits.def.petri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import toolkits.utils.block.InnerNet;

/**
 * @author Moqi
 * 定义过程网,用来建模业务过程及其组合
 */
public class ProNet {
	
	private Marking source;//一个开始标识及多个终止标识
	private List<Marking> sinks;
	private List<String> places;//库所集(包含消息库所)
	private List<String> msgPlaces;
	private List<String> resPlaces;
	private List<String> linkPlaces;
	private List<String> resources;//资源,用资源库所多集表示
	private List<String> inputMsgs;
	private List<String> outputMsgs;
	private List<String> trans;//变迁集
	private List<String> ctrlTrans;//控制变迁集
	private List<Flow> flows;//流关系
	private Map<String, String> tranLabelMap;//标号函数
	//资源属性映射:0为可重复使用资源,1为消耗资源
	private Map<String, Integer> resProperMap;
	//映射:<资源,请求变迁>
	private Map<String, List<String>> reqResMap;
	
	public ProNet() {
		sinks = new ArrayList<Marking>();
		places = new ArrayList<String>();
		msgPlaces = new ArrayList<String>();
		resPlaces = new ArrayList<String>();
		linkPlaces = new ArrayList<String>();
		inputMsgs = new ArrayList<String>();
		outputMsgs = new ArrayList<String>();
		trans = new ArrayList<String>();
		ctrlTrans = new ArrayList<String>();
		flows = new ArrayList<Flow>();
		tranLabelMap = new HashMap<String, String>();
		resProperMap = new HashMap<String, Integer>();
		reqResMap = new HashMap<String, List<String>>();
	}
	
	
	/****************************Utils方法******************************/
	
	//添加一个sink库所
	public void addSink(Marking sink) {
		sinks.add(sink);
	}
	//添加一个库所(避免重复添加)
	public void addPlace(String place) {
		if (!places.contains(place)) {
			places.add(place);
		}
	}
	//添加多个库所(避免重复添加)
	public void addPlaces(List<String> tempPlaces) {
		for (String tempPlace : tempPlaces) {
			if (!places.contains(tempPlace)) {
				places.add(tempPlace);
			}
		}
	}
	//添加tran(避免重复添加)
	public void addTran(String tran) {
		if (!trans.contains(tran)) {
			trans.add(tran);
		}
	}
	//添加控制变迁(避免重复添加)
	public void addCtrlTran(String tran) {
		if (!ctrlTrans.contains(tran)) {
			ctrlTrans.add(tran);
		}
	}
	//添加控制变迁(避免重复添加)
	public void addCtrlTrans(List<String> trans) {
		for (String tran : trans) {
			if (!ctrlTrans.contains(tran)) {
				ctrlTrans.add(tran);
			}
		}
	}
	//添加一条flow
	public void addFlow(Flow flow) {
		flows.add(flow);
	}
	//添加多条flow
	public void addFlows(List<Flow> tempFlows) {
		flows.addAll(tempFlows);
	}
	//根据流from和to构建流
	public void addFlow(String flowFrom, String flowTo) {
		Flow flow = new Flow();
		flow.setFlowFrom(flowFrom);
		flow.setFlowTo(flowTo);
		flows.add(flow);
	}

	public void rovFlow(String flowFrom, String flowTo) {
		for (int i = 0; i < flows.size(); i++) {
			Flow tempFlow = flows.get(i);
			String from = tempFlow.getFlowFrom();
			String to = tempFlow.getFlowTo();
			if (flowFrom.equals(from) && flowTo.equals(to)) {
				flows.remove(i);
				break;
			}
		}
	}
	//添加消息库所(避免重复添加)
	public void addMsgPlace(String msgPlace) {
		if (!msgPlaces.contains(msgPlace)) {
			msgPlaces.add(msgPlace);
		}
	}
	//添加输入消息库所(避免重复添加)
	public void addInputMsgPlace(String msgPlace) {
		if (!inputMsgs.contains(msgPlace)) {
			inputMsgs.add(msgPlace);
		}
	}
	//添加输出消息库所(避免重复添加)
	public void addOutputMsgPlace(String msgPlace) {
		if (!outputMsgs.contains(msgPlace)) {
			outputMsgs.add(msgPlace);
		}
	}
	//添加资源库所(避免重复添加)
	public void addResPlace(String resPlace) {
		if (!resPlaces.contains(resPlace)) {
			resPlaces.add(resPlace);
		}
	}
	//添加链接库所(避免重复添加)
	public void addLinkPlace(String linkPlace) {
		if (!linkPlaces.contains(linkPlace)) {
			linkPlaces.add(linkPlace);
		}
	}
	//获取ProNet中异步交互活动集
	public List<String> getAsynInterTrans() {
		List<String> asynInterTrans = new ArrayList<String>();
		for (Flow flow : getFlows()) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (getMsgPlaces().contains(from)) {
				asynInterTrans.add(to);
			}else if (getMsgPlaces().contains(to)) {
				asynInterTrans.add(from);
			}
        }
		return asynInterTrans;
	}
	
	/*************************Get和Set方法****************************/
	
	public Marking getSource() {
		return source;
	}
	public void setSource(Marking source) {
		this.source = source;
	}	
	public List<Marking> getSinks() {
		return sinks;
	}
	public void setSinks(List<Marking> sinks) {
		this.sinks = sinks;
	}
	public List<String> getPlaces() {
		return places;
	}
	public void setPlaces(List<String> places) {
		this.places = places;
	}
	public List<String> getTrans() {
		return trans;
	}
	public void setTrans(List<String> trans) {
		this.trans = trans;
	}
	public List<String> getCtrlTrans() {
		return ctrlTrans;
	}
	public void setCtrlTrans(List<String> ctrlTrans) {
		this.ctrlTrans = ctrlTrans;
	}
	public List<Flow> getFlows() {
		return flows;
	}
	public void setFlows(List<Flow> flows) {
		this.flows = flows;
	}
	public List<String> getMsgPlaces() {
		return msgPlaces;
	}
	public void setMsgPlaces(List<String> msgPlaces) {
		this.msgPlaces = msgPlaces;
	}
	public List<String> getInputMsgs() {
		return inputMsgs;
	}
	public void setInputMsgs(List<String> inputMsgs) {
		this.inputMsgs = inputMsgs;
	}
	public List<String> getOutputMsgs() {
		return outputMsgs;
	}
	public void setOutputMsgs(List<String> outputMsgs) {
		this.outputMsgs = outputMsgs;
	}
	public List<String> getResPlaces() {
		return resPlaces;
	}
	public void setResPlaces(List<String> resPlaces) {
		this.resPlaces = resPlaces;
	}
	public List<String> getLinkPlaces() {
		return linkPlaces;
	}
	public void setLinkPlaces(List<String> linkPlaces) {
		this.linkPlaces = linkPlaces;
	}
	public List<String> getResources() {
		return resources;
	}
	public void setResources(List<String> resources) {
		this.resources = resources;
	}
	public Map<String, String> getTranLabelMap() {
		return tranLabelMap;
	}
	public void setTranLabelMap(Map<String, String> tranLabelMap) {
		this.tranLabelMap = tranLabelMap;
	}
	public Map<String, Integer> getResProperMap() {
		return resProperMap;
	}
	public void setResProperMap(Map<String, Integer> resProperMap) {
		this.resProperMap = resProperMap;
	}
	public Map<String, List<String>> getReqResMap() {
		return reqResMap;
	}
	public void setReqResMap(Map<String, List<String>> reqResMap) {
		this.reqResMap = reqResMap;
	}


	/***********************返回过程网的内网(工作流网描述且无交互变迁)**************************/
	
	public InnerNet getInnerNet() {
		
		InnerNet innerNet = new InnerNet();
		//源库所只有1个
        innerNet.setSource(getSource().getPlaces().get(0));
        //终止库所1个
        innerNet.setSink(getSinks().get(0).getPlaces().get(0));
        innerNet.setLinkPlaces(getLinkPlaces());
        innerNet.setTrans(getTrans());
        List<Flow> interFlows = new ArrayList<Flow>();
        for (Flow flow : getFlows()) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			//移除消息流和资源流
			if (getMsgPlaces().contains(from) || getMsgPlaces().contains(to)
					|| getResPlaces().contains(from) || getResPlaces().contains(to)) {
				continue;
			}
			//添加链接输入流和链接输出流
			if (getLinkPlaces().contains(from)) {//1.链接出流(linkPlace, tran)
				//System.out.println("add outLinkFlow: " + from + to);
				innerNet.addOutLinkFlow(from, to);
			}else if (getLinkPlaces().contains(to)) {//2.链接入流(tran, linkPlace)
				//System.out.println("add inLinkFlow: " + from + to);
				innerNet.addInLinkFlow(from, to);
			}
			interFlows.add(flow);
        }
        innerNet.setFlows(interFlows);
        //获取库所集(排除消息/资源库所)
        for (Flow flow : interFlows) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (getPlaces().contains(from)) {
				innerNet.addPlace(from);
			}
			if (getPlaces().contains(to)) {
				innerNet.addPlace(to);
			}
		}
        innerNet.setTranLabelMap(getTranLabelMap());
        return innerNet;
        
	}
	
	//获取消息或资源流
	public List<Flow> getMsgAndResFlows() {
		List<Flow> msgAndResFlows = new ArrayList<>();
		for (Flow flow : getFlows()) {
        	String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			//消息流和资源流
			if (getMsgPlaces().contains(from) || getMsgPlaces().contains(to)
					|| getResPlaces().contains(from) || getResPlaces().contains(to)) {
				msgAndResFlows.add(flow);
			}
		}
		return msgAndResFlows;
	}
	
}
