package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import toolkits.def.petri.Flow;
import toolkits.utils.petri.PetriUtils;

/**
 * @author Moqi 
 * 定义过程网中内网,即将消息和资源库所移除后形成的网
 */
public class InnerNet {
	
	private String source;
	private String sink;
	private List<String> places;
	private List<String> linkPlaces;
	//交互变迁集(Note:需要通过过程网的组合获得)
	private List<String> interTrans;
	private List<String> trans;
	private List<Flow> inLinkFlows;
	private List<Flow> outLinkFlows;
	private List<Flow> flows;
	private Map<String, String> tranLabelMap;
	private PetriUtils petriUtils;
	
	public InnerNet() {
		places = new ArrayList<String>();
		linkPlaces = new ArrayList<String>();
        interTrans = new ArrayList<String>();
		trans = new ArrayList<String>();
		inLinkFlows = new ArrayList<>();
		outLinkFlows = new ArrayList<>();
		flows = new ArrayList<Flow>();
		petriUtils = new PetriUtils();
	}
	
	/****************************Utils方法******************************/
	
	public void addPlace(String place) {
		if (!places.contains(place)) {
			places.add(place);
		}
	}
	
	public void addLinkPlace(String place) {
		if (!linkPlaces.contains(place)) {
			linkPlaces.add(place);
		}
	}
	
	//根据place移除链接(Note:每个link只有一个唯一source和target)
	public void rovLinkPlace(String place) {
		if (linkPlaces.contains(place)) {
			for (int i = 0; i < inLinkFlows.size(); i++) {
				Flow tempFlow = inLinkFlows.get(i);
				String to = tempFlow.getFlowTo();
				if (place.equals(to)) {
					inLinkFlows.remove(i);
					break;
				}
			}
			for (int i = 0; i < outLinkFlows.size(); i++) {
				Flow tempFlow = outLinkFlows.get(i);
				String from = tempFlow.getFlowFrom();
				if (place.equals(from)) {
					outLinkFlows.remove(i);
					break;
				}
			}
		}
	}
	
	//添加交互变迁(避免重复添加)
	public void addInterTran(String tran) {
		if (!interTrans.contains(tran)) {
			interTrans.add(tran);
		}
	}
	public void addInterTrans(List<String> trans) {
		for (String tran : trans) {
			if (!interTrans.contains(tran)) {
				interTrans.add(tran);
			}
		}
	}
	
	public void addTran(String tran) {
		if (!trans.contains(tran)) {
			trans.add(tran);
		}
	}
	
	public void rovTran(String tran) {
		trans.remove(tran);
	}
	
	public void addInLinkFlow(String flowFrom, String flowTo) {
		Flow flow = new Flow();
		flow.setFlowFrom(flowFrom);
		flow.setFlowTo(flowTo);
		inLinkFlows.add(flow);
	}
	
	//移除链接输入流
	public void rovInLinkFlow(String flowFrom, String flowTo) {
		for (int i = 0; i < inLinkFlows.size(); i++) {
			Flow tempFlow = inLinkFlows.get(i);
			String from = tempFlow.getFlowFrom();
			String to = tempFlow.getFlowTo();
			if (flowFrom.equals(from) && flowTo.equals(to)) {
				inLinkFlows.remove(i);
				break;
			}
		}
	}
	
	public void addOutLinkFlow(String flowFrom, String flowTo) {
		Flow flow = new Flow();
		flow.setFlowFrom(flowFrom);
		flow.setFlowTo(flowTo);
		outLinkFlows.add(flow);
	}
	
	//移除链接输出流
	public void rovOutLinkFlow(String flowFrom, String flowTo) {
		for (int i = 0; i < outLinkFlows.size(); i++) {
			Flow tempFlow = outLinkFlows.get(i);
			String from = tempFlow.getFlowFrom();
			String to = tempFlow.getFlowTo();
			if (flowFrom.equals(from) && flowTo.equals(to)) {
				outLinkFlows.remove(i);
				break;
			}
		}
	}
	
	public void addFlow(Flow flow) {
		flows.add(flow);
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
	
	public void addFlows(List<Flow> tempFlows) {
		flows.addAll(tempFlows);
	}
	
	//前向插入控制变迁(eg., tran->ctrlPlace->ctrlTran->placeFrom)
	@SuppressWarnings("static-access")
	public void insertForwardCtrlTran(String placeFrom, String ctrlTran, String ctrlPlace) {

		List<String> preSet = petriUtils.getPreSet(placeFrom, getFlows());
		for (String tran : preSet) {
			addFlow(tran, ctrlPlace);
			rovFlow(tran, placeFrom);
		}
		addFlow(ctrlPlace, ctrlTran);
		addFlow(ctrlTran, placeFrom);
		
		//添加增加的控制库所和变迁
		getPlaces().add(ctrlPlace);
		getTrans().add(ctrlTran);
		// 控制变迁的label是其Id
		getTranLabelMap().put(ctrlTran, ctrlTran);

	}
		
	//后向插入控制变迁(eg., placeTo->ctrlTran->ctrlPlace->tran)
	@SuppressWarnings("static-access")
	public void insertBackCtrlTran(String placeTo, String ctrlTran, String ctrlPlace) {

		List<String> postSet = petriUtils.getPostSet(placeTo, getFlows());
		for (String tran : postSet) {
			addFlow(ctrlPlace, tran);
			rovFlow(placeTo, tran);
		}
		addFlow(placeTo, ctrlTran);
		addFlow(ctrlTran, ctrlPlace);
		
		//添加增加的控制库所和变迁
		getPlaces().add(ctrlPlace);
		getTrans().add(ctrlTran);
		// 控制变迁的label是其Id
		getTranLabelMap().put(ctrlTran, ctrlTran);

	}
	
	
	/*************************Get和Set方法****************************/
	
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getSink() {
		return sink;
	}
	public void setSink(String sink) {
		this.sink = sink;
	}
	public List<String> getPlaces() {
		return places;
	}
	public void setPlaces(List<String> places) {
		this.places = places;
	}
	public List<String> getLinkPlaces() {
		return linkPlaces;
	}
	public void setLinkPlaces(List<String> linkPlaces) {
		this.linkPlaces = linkPlaces;
	}
	public List<String> getInterTrans() {
		return interTrans;
	}
	public void setInterTrans(List<String> interTrans) {
		this.interTrans = interTrans;
	}
	public List<String> getTrans() {
		return trans;
	}
	public void setTrans(List<String> trans) {
		this.trans = trans;
	}
	public List<Flow> getInLinkFlows() {
		return inLinkFlows;
	}
	public void setInLinkFlows(List<Flow> inLinkFlows) {
		this.inLinkFlows = inLinkFlows;
	}
	public List<Flow> getOutLinkFlows() {
		return outLinkFlows;
	}
	public void setOutLinkFlows(List<Flow> outLinkFlows) {
		this.outLinkFlows = outLinkFlows;
	}
	public List<Flow> getFlows() {
		return flows;
	}
	public void setFlows(List<Flow> flows) {
		this.flows = flows;
	}
	public Map<String, String> getTranLabelMap() {
		return tranLabelMap;
	}
	public void setTranLabelMap(Map<String, String> tranLabelMap) {
		this.tranLabelMap = tranLabelMap;
	}
	
	
}
