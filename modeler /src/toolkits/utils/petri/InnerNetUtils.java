package toolkits.utils.petri;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import toolkits.def.petri.Composition;
import toolkits.def.petri.Flow;
import toolkits.def.petri.ProNet;
import toolkits.utils.block.InnerNet;

public class InnerNetUtils {
	
	//获取约简后的过程网(Note:innerNet的控制流中不含链接流,消息流和资源流)
    public ProNet getReduceProNet(ProNet proNet, InnerNet innerNet) {
        
    	ProNet reduceProNet = new ProNet();
    	
    	reduceProNet.setSource(proNet.getSource());
    	
    	reduceProNet.setSinks(proNet.getSinks());
    	
    	reduceProNet.setPlaces(innerNet.getPlaces());
    	reduceProNet.addPlaces(proNet.getMsgPlaces());
    	reduceProNet.addPlaces(proNet.getResPlaces());
    	//添加链接库所(Note:约简中被更新)
    	reduceProNet.addPlaces(getLinkPlaces(innerNet));
    	
    	reduceProNet.setMsgPlaces(proNet.getMsgPlaces());
    	
    	reduceProNet.setResPlaces(proNet.getResPlaces());
    	
    	reduceProNet.setLinkPlaces(getLinkPlaces(innerNet));
    	
    	reduceProNet.setResources(proNet.getResources()); 
    	reduceProNet.setInputMsgs(proNet.getInputMsgs());
    	reduceProNet.setOutputMsgs(proNet.getOutputMsgs());
    	reduceProNet.setTrans(innerNet.getTrans());
    	
    	reduceProNet.setCtrlTrans(proNet.getCtrlTrans());
    	//Note:添加前向或后向插入引入的控制变迁集
		reduceProNet.addCtrlTrans(getCtrlTrans(innerNet));
		
		reduceProNet.setFlows(innerNet.getFlows());
		//添加输入链接的流
		List<Flow> inLinkFlows = innerNet.getInLinkFlows();
		for (Flow flow : inLinkFlows) {
			reduceProNet.addFlow(flow);
		}
		//添加输出链接的流
		List<Flow> outLinkFlows = innerNet.getOutLinkFlows();
		for (Flow flow : outLinkFlows) {
			reduceProNet.addFlow(flow);
		}
		//添加消息和资源流
		reduceProNet.addFlows(proNet.getMsgAndResFlows());
		
		reduceProNet.setTranLabelMap(innerNet.getTranLabelMap());
		
		reduceProNet.setResProperMap(proNet.getResProperMap());
		
		reduceProNet.setReqResMap(proNet.getReqResMap());
		
		return reduceProNet;
		
	}
    
    //获取内网中的链接库所集
    public List<String> getLinkPlaces(InnerNet innerNet) {
		List<String> linkPlaces = new ArrayList<>();
		List<Flow> inLinkFlows = innerNet.getInLinkFlows();
		for (Flow flow : inLinkFlows) {
			String to = flow.getFlowTo();
			if (!linkPlaces.contains(to)) {
				linkPlaces.add(to);
			}
		}
		return linkPlaces;
	}
    
    //获取内网中因前向或后向插入引入的控制变迁集(Note:以ctrl前缀开始变迁)
    public List<String> getCtrlTrans(InnerNet innerNet) {
		List<String> ctrlTrans = new ArrayList<>();
		List<String> trans = innerNet.getTrans();
		for (String tran : trans) {
			if (tran.length() > 4 && tran.substring(0, 4).equals("ctrl")) {
				ctrlTrans.add(tran);
			}
		}
		return ctrlTrans;
	}
	
	/***********************获取一组过程网的内网**************************/
	
	//获取一组过程网对应的内网
	public List<InnerNet> getInnerNets(List<ProNet> proNets) {
		List<InnerNet> innerNets = new ArrayList<>();
		Composition composition = new Composition();
		composition.setProNets(proNets);//首先需要设置
		ProNet compNet = composition.compose();
		List<String> syncInterTrans = getSyncInterTrans(compNet);
        for (ProNet proNet : proNets) {
			InnerNet innerNet = proNet.getInnerNet();
			//交互变迁集需要根据组合计算得到
			innerNet.addInterTrans(proNet.getAsynInterTrans());
			innerNet.addInterTrans((List<String>) CollectionUtils.intersection(syncInterTrans, proNet.getTrans()));
			innerNets.add(innerNet);
		}
        return innerNets;
	}
	
	//获取组合网中的同步变迁集
	public List<String> getSyncInterTrans(ProNet compNet) {
		List<String> syncInterTrans = new ArrayList<>();
		List<String> trans = compNet.getTrans();
		for (String tran : trans) {
			if (tran.contains("_")) {
				String[] mergeTrans = tran.split("\\_");
				for (int i = 0; i < mergeTrans.length; i++) {
					//避免重复的同步变迁(一个变迁可以参与多个同步交互)
					if (!syncInterTrans.contains(mergeTrans[i])) {
						syncInterTrans.add(mergeTrans[i]);
					}
				}
			}
		}
		return syncInterTrans;
	}
	
	
    /*************************获取内网的逆网****************************/
	
	public InnerNet genInverNet(InnerNet innerNet) {
		InnerNet inverNet = new InnerNet();
		inverNet.setSource(innerNet.getSink());
		inverNet.setSink(innerNet.getSource());
		inverNet.setPlaces(innerNet.getPlaces());
		inverNet.setLinkPlaces(innerNet.getLinkPlaces());
		inverNet.setInterTrans(innerNet.getInterTrans());
		inverNet.setTrans(innerNet.getTrans());
		inverNet.setTranLabelMap(innerNet.getTranLabelMap());
		List<Flow> inverFlows = new ArrayList<Flow>();
        for (Flow flow : innerNet.getFlows()) {
			Flow inverFlow = new Flow();
			inverFlow.setFlowFrom(flow.getFlowTo());
			inverFlow.setFlowTo(flow.getFlowFrom());
			inverFlows.add(inverFlow);
		}
        //inverNet.setInLinkFlows(innerNet.getInLinkFlows());
        //inverNet.setOutLinkFlows(innerNet.getOutLinkFlows());
        inverNet.setFlows(inverFlows);
        return inverNet;
	}
	

}
