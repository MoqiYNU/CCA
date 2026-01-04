package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

/**
 * *@author Moqi
 * 定义过程树,包括节点及其孩子节点集
 */
public class ProTree {

	private List<Node> nodes;
	// 多值映射存储并发块中的同步链接
	private MultiValuedMap<String, String> links;
	// 存储内部可约变迁(既非控制变迁也非交互变迁)
	private List<String> innerTrans;

	public ProTree() {
		nodes = new ArrayList<>();
		links = new ArrayListValuedHashMap<>();
		innerTrans = new ArrayList<>();
	}

	/**************************** Utils方法 ******************************/

	public void addNode(Node node) {
		if (!isExistingNode(node.getId(), nodes)) {
			nodes.add(node);
		}
	}

	public void addNodes(List<Node> addNodes) {
		for (Node addNode : addNodes) {
			if (!isExistingNode(addNode.getId(), nodes)) {
				nodes.add(addNode);
			}
		}
	}

	// 添加内部变迁
	public void addInnerTran(String tran) {
		if (!innerTrans.contains(tran)) {
			innerTrans.add(tran);
		}
	}

	// 判断结点是否存在
	public boolean isExistingNode(String id, List<Node> nodes) {
		for (Node node : nodes) {
			if (id.equals(node.getId())) {
				return true;
			}
		}
		return false;
	}

	// 移除树中节点node
	public void removeNode(Node node) {
		for (int i = 0; i < nodes.size(); i++) {
			Node tempNode = nodes.get(i);
			if (tempNode.getId().equals(node.getId())) {
				nodes.remove(i);
				break;
			}
		}
	}

	/************************* Get和Set方法 ****************************/

	public List<Node> getNodes() {
		return nodes;
	}

	public void setNodes(List<Node> nodes) {
		this.nodes = nodes;
	}

	public MultiValuedMap<String, String> getLinks() {
		return links;
	}

	public void setLinks(MultiValuedMap<String, String> links) {
		this.links = links;
	}

	public List<String> getInnerTrans() {
		return innerTrans;
	}

	public void setInnerTrans(List<String> innerTrans) {
		this.innerTrans = innerTrans;
	}

}
