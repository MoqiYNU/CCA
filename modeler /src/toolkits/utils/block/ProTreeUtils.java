package toolkits.utils.block;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import toolkits.def.petri.Flow;
import toolkits.def.petri.ProNet;
import toolkits.utils.petri.InnerNetUtils;
import toolkits.utils.petri.PetriUtils;

/**
 * *@author Moqi
 * 定义与过程树相关的Utils
 */
public class ProTreeUtils {

	private ProTreeBuilder proTreeBuilder;

	public ProTreeUtils() {
		proTreeBuilder = new ProTreeBuilder();
	}

	/**************************** 从过程网中生成过程树 ******************************/

	public List<ProTree> genProTrees(List<ProNet> proNets) throws Exception {
		List<ProTree> proTrees = new ArrayList<>();
		InnerNetUtils innerNetUtils = new InnerNetUtils();
		List<InnerNet> innerNets = innerNetUtils.getInnerNets(proNets);
		for (int i = 0; i < innerNets.size(); i++) {
			// 1.获取proNet内网(Note:必须先移除并发块中链接库所然后移除冗余库所,否则链接将导致错误并发)
			InnerNet innerNet1 = rovLinkPlaces(innerNets.get(i));
			InnerNet innerNet = rovRedPlaces(innerNet1);
			// dotUtils.innerNet2Dot(proNets.get(i).getCtrlTrans(), innerNet, "ipn" + i);
			// 2.获得初始过程树
			ProTree initProTree = proTreeBuilder.compute(innerNet);
			// dotUtils.pt2Dot(initProTree, proNet.getTranLabelMap(), "pt" + index);
			// 3.压缩初始过程树
			ProTree proTree = compress(initProTree);
			// 4.设置过程树中的同步链接
			List<String> links = proNets.get(i).getLinkPlaces();
			for (String link : links) {// 同步链接前集合后集均为一个变迁
				String from = PetriUtils.getPreSet(link, proNets.get(i).getFlows()).get(0);
				String to = PetriUtils.getPostSet(link, proNets.get(i).getFlows()).get(0);
				proTree.getLinks().put(from, to);
			}
			// 5.设置过程树中的内部可约活动
			List<String> trans = innerNets.get(i).getTrans();
			for (String tran : trans) {
				if (!innerNets.get(i).getInterTrans().contains(tran)
						&& !proNets.get(i).getCtrlTrans().contains(tran)) {
					proTree.addInnerTran(tran);
				}
			}
			proTrees.add(proTree);
			// dotUtils.proTree2Dot(proTree, proNets.get(i).getTranLabelMap(), "cpt" + i);
		}
		return proTrees;
	}

	/******************************** 预处理内网 **********************************/

	// 1.在生成过程树之前,必须先移除内网并发块中链接库所,否则会影响计算并发块中冗余库所
	public InnerNet rovLinkPlaces(InnerNet net) {

		InnerNet interNet = new InnerNet();// 待生成内网

		// Note: 链接库所即为先删除库所
		List<String> rovPlaces = new ArrayList<String>();
		rovPlaces.addAll(net.getLinkPlaces());

		// 1.生成interNet中的流集
		List<Flow> noRovFlows = new ArrayList<Flow>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (rovPlaces.contains(from) || rovPlaces.contains(to)) {
				continue;
			} else {
				noRovFlows.add(flow);
			}
		}

		// 2.生成interNet中的库所
		List<String> places = net.getPlaces();
		for (Flow flow : noRovFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				interNet.addPlace(from);
			}
			if (places.contains(to)) {
				interNet.addPlace(to);
			}
		}

		// 3.生成interNet中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : noRovFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				interNet.addTran(from);
			}
			if (trans.contains(to)) {
				interNet.addTran(to);
			}
		}

		// 返回interNet
		interNet.setSource(net.getSource());
		interNet.setSink(net.getSink());
		interNet.setLinkPlaces(net.getLinkPlaces());
		interNet.setInterTrans(net.getInterTrans());
		interNet.setInLinkFlows(net.getInLinkFlows());
		interNet.setOutLinkFlows(net.getOutLinkFlows());
		interNet.setFlows(noRovFlows);
		interNet.setTranLabelMap(net.getTranLabelMap());
		return interNet;

	}

	// 2)在移除链接库所后，再移除内网并发块中冗余库所
	public InnerNet rovRedPlaces(InnerNet net) {

		InnerNet interNet = new InnerNet();// 待生成内网

		List<String> rovPlaces = new ArrayList<String>();
		List<String> places = net.getPlaces();
		for (String place : places) {
			List<String> fromTrans = PetriUtils.getPreSet(place, net.getFlows());
			List<String> toTrans = PetriUtils.getPostSet(place, net.getFlows());
			if (fromTrans.size() == 1 && toTrans.size() == 1) {
				String fromTran = fromTrans.get(0);
				String toTran = toTrans.get(0);
				// 添加并发块中冗余的库所
				if (PetriUtils.getPostSet(fromTran, net.getFlows()).size() > 1
						&& PetriUtils.getPreSet(toTran, net.getFlows()).size() > 1) {
					rovPlaces.add(place);
				}
			}
		}

		// 1.生成interNet中的流集
		List<Flow> noRovFlows = new ArrayList<Flow>();
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (rovPlaces.contains(from) || rovPlaces.contains(to)) {
				continue;
			} else {
				noRovFlows.add(flow);
			}
		}

		// 2.生成interNet中的库所
		for (Flow flow : noRovFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				interNet.addPlace(from);
			}
			if (places.contains(to)) {
				interNet.addPlace(to);
			}
		}

		// 3.生成interNet中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : noRovFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				interNet.addTran(from);
			}
			if (trans.contains(to)) {
				interNet.addTran(to);
			}
		}

		// 返回interNet
		interNet.setSource(net.getSource());
		interNet.setSink(net.getSink());
		interNet.setLinkPlaces(net.getLinkPlaces());
		interNet.setInterTrans(net.getInterTrans());
		interNet.setInLinkFlows(net.getInLinkFlows());
		interNet.setOutLinkFlows(net.getOutLinkFlows());
		interNet.setFlows(noRovFlows);
		interNet.setTranLabelMap(net.getTranLabelMap());
		return interNet;

	}

	/******************************** 压缩过程树 **********************************/

	// 将过程树进行压缩,以保证其中每个孩子节点与父亲节点的类型不同
	public ProTree compress(ProTree proTree) {

		// 即将访问的队列visitingQueue并将proTree入队
		Queue<ProTree> visitingQueue = new LinkedList<>();
		visitingQueue.offer(proTree);

		// 迭代计算
		while (visitingQueue.size() > 0) {

			ProTree proTreeFrom = visitingQueue.poll();

			// 1.若proTreeFrom不可压缩,则直接返回
			Node compNode = isCompress(proTreeFrom);
			if (compNode == null) {
				return proTreeFrom;
			}

			// 2.可以压缩,则新建过程树(Note:必须新建避免出错,因为更新*******)
			Node compNodeFather = getFatherNode(proTreeFrom, compNode);
			// 存储压缩过程树中的节点
			List<Node> compNodes = new ArrayList<Node>();
			for (Node node : proTreeFrom.getNodes()) {
				if (node.getId().equals(compNode.getId())) {// 跳过待压缩节点
					continue;
				}
				if (node.getId().equals(compNodeFather.getId())) {// 重新排列父亲节点下面的孩子节点
					// 存储压缩后父节点的孩子节点
					List<String> updaChaNodesFather = new ArrayList<String>();
					for (String chaNode : compNodeFather.getChaNodes()) {
						if (chaNode.equals(compNode.getId())) {
							updaChaNodesFather.addAll(compNode.getChaNodes());
						} else {
							updaChaNodesFather.add(chaNode);
						}
					}
					// Note:必须新建(因为更新),否则出错
					Node newCompNodeFather = new Node();
					newCompNodeFather.setId(compNodeFather.getId());
					newCompNodeFather.setType(compNodeFather.getType());
					newCompNodeFather.setChaNodes(updaChaNodesFather);
					compNodes.add(newCompNodeFather);
				} else {
					compNodes.add(node);
				}
			}

			// 创建新压缩过程树
			ProTree compProTree = new ProTree();
			compProTree.setNodes(compNodes);
			visitingQueue.offer(compProTree);

		}
		return null;

	}

	// 判断pt能否压缩,即存在一个节点且其类型与其父亲节点一致
	public Node isCompress(ProTree pt) {
		List<Node> nodes = pt.getNodes();
		for (Node node : nodes) {// 叶子节点无须压缩
			if (node.getType().equals("leaf")) {
				continue;
			}
			Node nodeFather = getFatherNode(pt, node);
			if (nodeFather == null) {// 父节点为空无须压缩
				continue;
			}
			if (nodeFather.getType().equals(node.getType())) {
				return node;
			}
		}
		return null;
	}

	/*********** 将过程树分解为片段 (自顶向下以避免生成重复片段) ***********************/

	public List<ProTree> genFragments(ProTree proTree/* , InnerNet innerNet */) throws Exception {

		// 根据XOR节点分解后得到片段
		List<ProTree> fragments = new ArrayList<>();

		// 即将访问的队列visitingQueue并将proTree入队
		Queue<ProTree> visitingQueue = new LinkedList<>();
		visitingQueue.offer(proTree);
		// 迭代计算
		while (visitingQueue.size() > 0) {

			ProTree proTreeFrom = visitingQueue.poll();

			Node xorNode = isDecompose(proTreeFrom);

			if (xorNode == null) {// 1.proTreeFrom不可分解,则直接添加到片段中并跳出本次循环
				// Note:最终片段需压缩处理
				fragments.add(compress(proTreeFrom));
				continue;
			}

			// 2.proTreeFrom可分解,则首先获得其父亲节点
			Node xorNodeFather = getFatherNode(proTreeFrom, xorNode);

			if (xorNodeFather == null) {// 2.1若父亲节点为空,则以每个孩子节点为根生成一颗过程树

				List<String> chaNodes = xorNode.getChaNodes();
				for (String chaNode : chaNodes) {
					List<Node> desc = getDesc(proTree, chaNode);
					ProTree depProTree = new ProTree();// 分解后过程树
					depProTree.setNodes(desc);
					visitingQueue.offer(depProTree);
				}

			} else {// 2.2若父亲节点非空,则以xorNode节点的每个孩子节点生成一颗对应片段

				// System.out.println("test............................");

				List<String> chaNodes = xorNode.getChaNodes();
				for (String chaNode : chaNodes) {

					/*
					 * if (chaNode.getType().equals("leaf")) {
					 * System.out.println("chaNode: " +
					 * innerNet.getTranLabelMap().get(chaNode.getIdf()));
					 * }else {
					 * System.out.println("chaNode: " + chaNode.getIdf());
					 * }
					 */

					// 2.2.1 构建片段中节点及其关联孩子节点集
					List<Node> depNodes = new ArrayList<Node>();
					for (Node node : proTreeFrom.getNodes()) {
						// 1.是父亲节点,则将其孩子节点中xorNode替换为chaNode
						if (node.getId().equals(xorNodeFather.getId())) {
							List<String> updaChaNodesFather = new ArrayList<String>();
							List<String> chaNodesFather = xorNodeFather.getChaNodes();
							for (String chaNodeFather : chaNodesFather) {
								if (chaNodeFather.equals(xorNode.getId())) {
									// System.out.println("add node: " + chaNode.getIdf());
									updaChaNodesFather.add(chaNode);
								} else {
									updaChaNodesFather.add(chaNodeFather);
								}
							}
							// Note:必须新建,否则出错
							Node newXorNodeFather = new Node();
							newXorNodeFather.setId(xorNodeFather.getId());
							newXorNodeFather.setType(xorNodeFather.getType());
							newXorNodeFather.setChaNodes(updaChaNodesFather);
							depNodes.add(newXorNodeFather);
						} else {// 2.否则直接添加
							depNodes.add(node);
						}
					}

					// 2.2.2 构建分解过程树
					ProTree depProTree = new ProTree();
					depProTree.setNodes(depNodes);

					// 2.2.3 移除xorNode节点
					depProTree.removeNode(xorNode);

					// 2.2.4 移除非chaNode节点及其子孙节点
					List<String> restNodes = getRestNodes(chaNodes, chaNode);
					for (String restNode : restNodes) {
						List<Node> desc = getDesc(proTree, restNode);
						for (Node descNode : desc) {
							// System.out.println("remove node:" + descNode.getIdf());
							depProTree.removeNode(descNode);
						}
					}

					/*
					 * List<Node> nodes = depProTree.getNodes();
					 * for (Node node : nodes) {
					 * if (node.getType().equals("leaf")) {
					 * System.out.println("updated nodes: " +
					 * innerNet.getTranLabelMap().get(node.getIdf()));
					 * }else {
					 * System.out.println("updated nodes: " + node.getIdf());
					 * }
					 * }
					 */

					// 2.2.4添加第j个分解树到访问队列
					visitingQueue.offer(depProTree);

				}
			}
		}
		return fragments;

	}

	/****************************** 过程树Utils ********************************/

	// 获取chaNodes中除node外节点集
	public List<String> getRestNodes(List<String> chaNodes, String node) {
		List<String> restNodes = new ArrayList<String>();
		for (String chaNode : chaNodes) {
			if (chaNode.equals(node)) {
				continue;
			}
			restNodes.add(chaNode);
		}
		return restNodes;

	}

	// 获取以node为根的所有叶子子孙节点的Id
	public List<String> getLeafDesc(ProTree proTree, Node node) {
		List<String> desc = new ArrayList<>();
		Queue<Node> queue = new LinkedList<>();
		queue.add(node);// Note:不包含自己(若非叶子结点)
		while (queue.size() > 0) {
			Node tempNode = queue.poll();
			if (tempNode.getType().equals("leaf")) {
				desc.add(tempNode.getId());
			} else {
				List<String> chaNodes = tempNode.getChaNodes();
				for (String chaNode : chaNodes) {
					queue.add(getNodeById(proTree, chaNode));
				}
			}
		}
		return desc;
	}

	// 获取构建最小公共And祖先结点的控制结点And-Split和And-Join
	public List<String> getCtrlTransFromMinCommAndAncestor(ProTree proTree, String node1, String node2) {
		List<String> ctrlTrans = new ArrayList<>();
		String minCommAndAncestor = getMinCommAndAncestor(proTree, node1, node2);
		Node fatherNode = getFatherNode(proTree, minCommAndAncestor);
		List<String> chaNodes = fatherNode.getChaNodes();
		for (int i = 0; i < chaNodes.size(); i++) {
			if (chaNodes.get(i).equals(minCommAndAncestor)) {
				ctrlTrans.add(chaNodes.get(i - 1));
				ctrlTrans.add(chaNodes.get(i + 1));
			}
		}
		return ctrlTrans;
	}

	// 获取结点node1和node2的最小的公共And祖先结点
	public String getMinCommAndAncestor(ProTree proTree, String node1, String node2) {
		List<String> ancestor1 = getAncestor(proTree, node1);
		List<String> ancestor2 = getAncestor(proTree, node2);
		for (String node : ancestor1) {
			if (ancestor2.contains(node)) {
				return node;
			}
		}
		return null;
	}

	// 获取以node出发的所有AND祖先结点序列
	public List<String> getAncestor(ProTree proTree, String nodeId) {
		List<String> ancestor = new ArrayList<>();
		Queue<Node> queue = new LinkedList<>();
		Node fatherNode = getFatherNode(proTree, nodeId);
		queue.add(fatherNode);// Note:不包含自己
		if (fatherNode.getType().equals("AND")) {
			ancestor.add(fatherNode.getId());
		}
		while (queue.size() > 0) {
			Node tempNode = queue.poll();
			Node tempFatherNode = getFatherNode(proTree, tempNode);
			queue.add(tempFatherNode);
			if (tempFatherNode.getType().equals("AND")) {
				ancestor.add(tempFatherNode.getId());
			}
		}
		return ancestor;
	}

	// 获取id对应过程树中的结点
	public Node getNodeById(ProTree proTree, String idf) {
		List<Node> nodes = proTree.getNodes();
		for (Node node : nodes) {
			String tempIdf = node.getId();
			if (tempIdf.equals(idf)) {
				return node;
			}
		}
		return null;
	}

	// 获取以node为根的所有子孙节点
	public List<Node> getDesc(ProTree proTree, String nodeIdf) {
		List<Node> desc = new ArrayList<Node>();
		Node node = getNodeById(proTree, nodeIdf);
		desc.add(node);// Note:包含自己
		Queue<Node> queue = new LinkedList<>();
		List<String> chaNodes = node.getChaNodes();
		for (String chaNodeIdf : chaNodes) {
			Node chaNode = getNodeById(proTree, chaNodeIdf);
			desc.add(chaNode);
			queue.add(chaNode);
		}
		while (queue.size() > 0) {
			Node tempNode = queue.poll();
			if (tempNode.getType().equals("leaf")) {
				continue;
			}
			List<String> descNodes = tempNode.getChaNodes();
			for (String descIdf : descNodes) {
				Node desdNode = getNodeById(proTree, descIdf);
				desc.add(desdNode);
				queue.add(desdNode);
			}
		}
		return desc;
	}

	// 获取父亲节点
	public Node getFatherNode(ProTree proTree, Node node) {
		List<Node> nodes = proTree.getNodes();
		for (Node tempNode : nodes) {
			List<String> chaNodes = tempNode.getChaNodes();
			if (chaNodes.contains(node.getId())) {
				return tempNode;
			}
		}
		return null;
	}

	// 获取父亲节点
	public Node getFatherNode(ProTree proTree, String nodeId) {
		List<Node> nodes = proTree.getNodes();
		for (Node tempNode : nodes) {
			List<String> chaNodes = tempNode.getChaNodes();
			if (chaNodes.contains(nodeId)) {
				return tempNode;
			}
		}
		return null;
	}

	// 判断是否是顺序块中第一个节点
	public boolean isFirstNodeInSEQ(Node fatherNode, Node node) {
		List<String> chaNodes = fatherNode.getChaNodes();
		if (chaNodes.get(0).equals(node.getId())) {
			return true;
		}
		return false;
	}

	// 判断是否是顺序块中第一个节点
	public boolean isFirstNodeInSEQ(Node fatherNode, String nodeId) {
		List<String> chaNodes = fatherNode.getChaNodes();
		if (chaNodes.get(0).equals(nodeId)) {
			return true;
		}
		return false;
	}

	// 判断node是否为孩子节点
	public boolean isChaNode(List<Node> chaNodes, Node node) {
		for (Node chaNode : chaNodes) {
			if (chaNode.getId().equals(node.getId())) {
				return true;
			}
		}
		return false;
	}

	// 判断proTree是否可以分解,即存在XOR节点
	public Node isDecompose(ProTree proTree) {
		List<Node> nodes = proTree.getNodes();
		int size = nodes.size();
		// Note:从根开始访问,避免生成重复的片段(测试B31)
		for (int i = size - 1; i >= 0; i--) {
			Node node = nodes.get(i);
			String type = node.getType();
			if (type.equals("XOR")) {
				return node;
			}
		}
		return null;
	}

	// 获取proTree中SEQ节点集
	public List<Node> getSEQNode(ProTree proTree) {
		List<Node> SEQNodes = new ArrayList<Node>();
		List<Node> nodes = proTree.getNodes();
		for (Node node : nodes) {
			String type = node.getType();
			if (type.equals("SEQ")) {
				SEQNodes.add(node);
			}
		}
		return SEQNodes;
	}

}
