package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Moqi
 * 定义过程树中节点(包括idf,节点类型及孩子节点)
 */
public class Node {
	
	private String id;
	private String type;
	//Note:孩子结点用id标识
	private List<String> chaNodes;
	
	public Node() {
		chaNodes = new ArrayList<>();
	}
	
	/****************************Utils方法******************************/
	
	//添加孩子节点(避免重复添加)
	public void addChaNode(String node) {
		if (!chaNodes.contains(node)) {
			chaNodes.add(node);
		}
	}
	public void addChaNodes(List<String> nodes) {
		for (String node : nodes) {
			if (!chaNodes.contains(node)) {
				chaNodes.add(node);
			}
		}
	}
	
	/*************************Get和Set方法****************************/
	
	public String getId() {
		return id;
	}
	public void setId(String idf) {
		this.id = idf;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public List<String> getChaNodes() {
		return chaNodes;
	}
	public void setChaNodes(List<String> chaNodes) {
		this.chaNodes = chaNodes;
	}

}
