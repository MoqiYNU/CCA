package toolkits.utils.petri;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import toolkits.def.petri.Flow;
import toolkits.utils.block.InnerNet;

/**
 * @author Moqi
 * 从PNML解析过程网(返回是内网,即不含消息库所并用于测试结构分解)
 */
public class ParsePetri {
	
	@SuppressWarnings("unchecked")
	public InnerNet parse(String filePath) throws Exception {
		
		//待生成内网
		InnerNet interNet = new InnerNet();
		List<String> places = new ArrayList<String>();
		List<String> trans = new ArrayList<String>();
		List<Flow> flows = new ArrayList<Flow>();
		Map<String, String> tranLabelMap = new HashMap<String, String>();
		
		//Dom4j初始化文档
		SAXReader saxReader = new SAXReader();
		Document document = saxReader.read(new File(filePath));
		
		//获取根元素,即pnml
        Element root = document.getRootElement();
        
        //获取网元素,即net
        Element netElem = root.element("net");
        
        //获取网元素中库所元素
        List<Element> placeElems = netElem.elements("place");
        for (Element placeElem : placeElems) {
        	
        	//获得库所
        	String placeId = placeElem.attributeValue("id");
        	
        	Element msgPlaceElem = placeElem.element("msgPlace");
        	String msgPlaceText = msgPlaceElem.getText();
        	if (msgPlaceText.equals("true")) {//排除消息库所
				continue;
			}
        	Element resPlaceElem = placeElem.element("resPlace");
        	String resPlaceText = resPlaceElem.getText();
        	if (resPlaceText.equals("true")) {//排除资源库所
				continue;
			}
        	//否则,添加至库所集中
        	places.add(placeId);
		}
        
        //获取网元素中变迁元素
        List<Element> tranElems = netElem.elements("transition");
        for (Element tranElem : tranElems) {
        	//获得变迁
        	String tranId = tranElem.attributeValue("id");
        	Element nameElem = tranElem.element("name");
        	Element textElem = nameElem.element("value");
        	String tranName = textElem.getText();
        	trans.add(tranId);
        	//System.out.println("tranId: " + tranId + " tranName: " + tranName);
        	tranLabelMap.put(tranId, tranName);
		}
		
        //获取网元素中arc元素
        List<Element> arcElems = netElem.elements("arc");
        for (Element arcElem : arcElems) {
        	//获得变迁
        	String source = arcElem.attributeValue("source");
        	String target = arcElem.attributeValue("target");
        	//排除消息/资源流
        	if (places.contains(source) && trans.contains(target)
        			|| trans.contains(source) && places.contains(target)) {
        		Flow flow = new Flow();
            	flow.setFlowFrom(source);
            	flow.setFlowTo(target);
            	flows.add(flow);
			}
		}
        
        //设置petri网并返回
        interNet.setPlaces(places);
        interNet.setTrans(trans);
        interNet.setFlows(flows);
        interNet.setTranLabelMap(tranLabelMap);
		return interNet;
		
	}
	
	
}
