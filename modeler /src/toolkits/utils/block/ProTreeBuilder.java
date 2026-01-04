package toolkits.utils.block;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import toolkits.def.petri.Flow;
import toolkits.utils.petri.InnerNetUtils;
import toolkits.utils.petri.PetriUtils;

/**
 * *@author Moqi
 * 生成内网对应的过程树
 */
public class ProTreeBuilder {

	public ProTree compute(InnerNet net) throws Exception {

		// DotUtils dotUtils = new DotUtils();
		// int index = 0;

		ProTree tree = new ProTree();// 定义过程树

		// 顺序部件,并发部件,选择部件及循环部件标识
		int seqIndex = 0;
		int andIndex = 0;
		int xorIndex = 0;
		int loopIndex = 0;

		// 标识替换后产生库所编号,用于循环和并发结构(Note:更新需要注意)
		int pIndex = 0;

		// 即将访问的队列visitingQueue
		Queue<InnerNet> visitingQueue = new LinkedList<>();
		visitingQueue.offer(net);

		// 迭代计算
		outer: while (visitingQueue.size() > 0) {

			// 出对一个内网,以此进行迁移
			InnerNet netFrom = visitingQueue.poll();
			// netFrom只含有1个变迁,则设为叶子节点然后退出迭代
			if (netFrom.getTrans().size() == 1) {
				Node preNode = getNodeByIdf(tree, netFrom.getTrans().get(0));
				if (preNode == null) {// 若未生成过
					Node node = new Node();
					node.setId(netFrom.getTrans().get(0));
					node.setType("leaf");
					tree.addNode(node);
				}
				break;
			}

			// 1.计算最大顺序部件
			GetSeqBlock getSeqPart = new GetSeqBlock();
			getSeqPart.compute(netFrom);
			List<Block> maxSEQBlocks = getSeqPart.getMaxSEQBlocks();

			// 取第一个最大顺序块进行替换
			if (maxSEQBlocks.size() > 0) {// 至少有1个最大顺序块

				// System.out.println("SEQ....................................");
				Block block = maxSEQBlocks.get(0);
				Node seqNode = new Node();
				// 替换顺序部件的变迁
				String seqTran = "SEQ" + seqIndex;
				seqIndex++;
				seqNode.setId(seqTran);
				seqNode.setType("SEQ");

				// System.out.println("顺序分解测试: " + seqTran + " 活动: " + block.getSeqActs());

				List<String> seqActs = block.getSeqActs();
				// 每个seqAct对应一个叶子节点
				for (String seqAct : seqActs) {
					// 首先确认是否已经生成节点
					Node node = getNodeByIdf(tree, seqAct);
					if (node == null) {// 未生成过
						// 生成叶子节点,并分别添加至seqNode和tree中
						Node chaNode = new Node();
						chaNode.setId(seqAct);
						chaNode.setType("leaf");
						seqNode.addChaNode(chaNode.getId());
						tree.addNode(chaNode);
					} else {// 生成过,则添加至seqNode
						seqNode.addChaNode(node.getId());
					}
				}
				tree.addNode(seqNode);

				InnerNet netTo = seqBlockReplace(netFrom, seqActs, seqTran);
				// dotUtils.innerNet2Dot(netTo, "innerNet" + index);
				// index ++;
				visitingQueue.offer(netTo);
				// Note:重新进行迭代计算,即goto至while
				continue outer;
			}

			// 2.计算元并行部件,分如下两种情况进行处理
			/**
			 * C1:针对一般元并发及AND2(元并发后接情形)情形进行处理-------------------------------------
			 */

			GetAndBlock getAndBlock = new GetAndBlock();
			getAndBlock.compute(netFrom);
			List<Block> metaANDBlocksC1 = getAndBlock.getMetaANDBlocks();
			// 取第一个元并发块进行替换
			if (metaANDBlocksC1.size() > 0) {// 至少有1个元并行部件
				// System.out.println("AND2....................................");
				Block block = metaANDBlocksC1.get(0);

				// 2.1生成替换的并行节点
				Node ANDNode = new Node();
				String flowTran = "AND" + andIndex;
				andIndex++;
				ANDNode.setId(flowTran);
				ANDNode.setType("AND");

				// System.out.println("并发分解测试: " + flowTran + " 活动: " + block.getEntry());

				// 2.2并进行并发部件替换
				List<String> fromPlaces = block.getEntryPost();
				List<String> postActs = new ArrayList<String>();
				for (String place : fromPlaces) {
					List<String> postSet = getPostSet(place, netFrom.getFlows());
					postActs.addAll(postSet);
				}
				for (String postAct : postActs) {// 针对并发块中的每个迁移进行计算
					// 首先确认是否已经生成节点
					Node node = getNodeByIdf(tree, postAct);
					if (node == null) {// 未生成过
						// 生成叶子节点
						Node chaNode = new Node();
						chaNode.setId(postAct);
						chaNode.setType("leaf");
						ANDNode.addChaNode(chaNode.getId());
						tree.addNode(chaNode);
					} else {
						ANDNode.addChaNode(node.getId());
					}
				}
				tree.addNode(ANDNode);

				String entry = block.getEntry();
				String exit = block.getExit();
				InnerNet netTo = andBlockReplace(netFrom, entry, exit, postActs, flowTran, pIndex);
				// dotUtils.innerNet2Dot(netTo, "innerNet" + index);
				// index ++;
				// Note:pIndex在andBlockReplace更新不会影响主函数,因此需对应更新
				pIndex++;
				pIndex++;
				visitingQueue.offer(netTo);
				// 重新进行迭代计算,即goto至while
				continue outer;
			}

			/**
			 * C2:针对AND1(元并发前接)情形进行处理(从逆网视角处理)-------------------------------------
			 */

			InnerNetUtils innerNetUtils = new InnerNetUtils();
			getAndBlock.compute(innerNetUtils.genInverNet(netFrom));
			List<Block> metaANDBlocksC2 = getAndBlock.getMetaANDBlocks();
			// 取第一个元并发块进行替换
			if (metaANDBlocksC2.size() > 0) {// 至少有1个元并行部件
				// System.out.println("AND1....................................");
				Block block = metaANDBlocksC2.get(0);

				// 2.1生成替换的并行节点
				Node ANDNode = new Node();
				String flowTran = "AND" + andIndex;
				andIndex++;
				ANDNode.setId(flowTran);
				ANDNode.setType("AND");

				// System.out.println("并发分解测试: " + flowTran + " 活动: " + block.getExit());

				// 2.2并进行并发部件替换
				List<String> fromPlaces = block.getExitPre();// 是逆网,故fromPlace相反
				List<String> postActs = new ArrayList<String>();
				for (String place : fromPlaces) {
					List<String> postSet = getPostSet(place, netFrom.getFlows());
					postActs.addAll(postSet);
				}
				for (String postAct : postActs) {// 针对并发块中的每个迁移进行计算
					// 首先确认是否已经生成节点
					Node node = getNodeByIdf(tree, postAct);
					if (node == null) {// 未生成过
						// 生成叶子节点
						Node chaNode = new Node();
						chaNode.setId(postAct);
						chaNode.setType("leaf");
						ANDNode.addChaNode(chaNode.getId());
						tree.addNode(chaNode);
					} else {
						ANDNode.addChaNode(node.getId());
					}
				}
				tree.addNode(ANDNode);
				// 是逆网,故entry和exit相反
				String entry = block.getExit();
				String exit = block.getEntry();
				InnerNet netTo = andBlockReplace(netFrom, entry, exit, postActs, flowTran, pIndex);
				// dotUtils.innerNet2Dot(netTo, "innerNet" + index);
				// index ++;
				// Note:pIndex在andBlockReplace更新不会影响主函数,因此需对应更新
				pIndex++;
				pIndex++;
				visitingQueue.offer(netTo);
				// 重新进行迭代计算,即goto至while
				continue outer;
			}

			// 3.计算元选择块,然后进行替换
			GetLoopBlock getLoopBlock = new GetLoopBlock();
			getLoopBlock.compute(netFrom);
			List<Block> loopBlocks = getLoopBlock.getLoopBlocks();
			GetXorBlock getXorBlock = new GetXorBlock();
			getXorBlock.compute(netFrom, loopBlocks);
			List<Block> metaXORBlocks = getXorBlock.getMetaXORBlocks();

			// 取第一个元并发块进行替换
			if (metaXORBlocks.size() > 0) {// 至少有1个元选择块
				// System.out.println("XOR....................................");
				Block block = metaXORBlocks.get(0);

				// 替换的选择变迁
				Node XORNode = new Node();
				String xorTran = "XOR" + xorIndex;
				xorIndex++;
				XORNode.setId(xorTran);
				XORNode.setType("XOR");

				// System.out.println("互斥分解测试: " + xorTran + " 活动: " + block.getEntryPost());

				String entry = block.getEntry();
				List<String> transInXor = block.getEntryPost();
				for (String tranInXor : transInXor) {
					// 首先确认是否已经生成节点
					Node node = getNodeByIdf(tree, tranInXor);
					if (node == null) {// 未生成过
						// 生成叶子节点
						Node chaNode = new Node();
						chaNode.setId(tranInXor);
						chaNode.setType("leaf");
						XORNode.addChaNode(chaNode.getId());
						tree.addNode(chaNode);
					} else {
						XORNode.addChaNode(node.getId());
					}
				}
				// 添加选择节点
				tree.addNode(XORNode);

				InnerNet netTo = xorBlockReplace(netFrom, entry, block.getExit(), transInXor, xorTran);
				// dotUtils.innerNet2Dot(netTo, "innerNet" + index);
				// index ++;
				visitingQueue.offer(netTo);
				continue outer;
			}

			// 4.计算元循环块
			List<Block> metaLoopBlocks = getLoopBlock.getMetaLoopBlocks(netFrom);
			// 取第一个元循环块进行替换
			if (metaLoopBlocks.size() > 0) {// 至少有1个元循环块

				// System.out.println("loop....................................");

				// 从entry出发若存在多个循环结构
				List<Block> replacedLoopBlocks = getXORLooBlocks(metaLoopBlocks, loopBlocks);
				if (replacedLoopBlocks.size() > 0) {// 从entry出发的每个环均是元环
					String entry = replacedLoopBlocks.get(0).getEntry();

					// 循环节点
					Node loopNode = new Node();
					String loopTran = "Loop" + loopIndex;
					loopIndex++;
					loopNode.setId(loopTran);
					loopNode.setType("Loop");

					if (replacedLoopBlocks.size() == 1) {// 4.1 从入口出发只有一个元环
						String act = replacedLoopBlocks.get(0).getEntryPost().get(0);
						// 首先确认是否已经生成节点
						Node node = getNodeByIdf(tree, act);
						if (node == null) {// 未生成过
							// 生成叶子节点
							Node chaNode = new Node();
							chaNode.setId(act);
							chaNode.setType("leaf");
							loopNode.addChaNode(chaNode.getId());
							tree.addNode(chaNode);
						} else {// 生成过,则直接添加至switNode
							loopNode.addChaNode(node.getId());
						}
						tree.addNode(loopNode);

					} else {// 4.2 从入口出发有多个元环

						// 选择节点
						Node xorNode = new Node();
						String xorTran = "XOR" + xorIndex;
						xorIndex++;
						xorNode.setId(xorTran);
						xorNode.setType("XOR");

						for (Block part : replacedLoopBlocks) {
							// 最大循环块中只有1个act
							String act = part.getEntryPost().get(0);
							// 首先确认是否已经生成节点
							Node node = getNodeByIdf(tree, act);
							if (node == null) {// 未生成过
								// 生成叶子节点
								Node chaNode = new Node();
								chaNode.setId(act);
								chaNode.setType("leaf");
								xorNode.addChaNode(chaNode.getId());
								tree.addNode(chaNode);
							} else {// 生成过,则直接添加至switNode
								xorNode.addChaNode(node.getId());
							}
						}
						tree.addNode(xorNode);
						loopNode.addChaNode(xorNode.getId());
						tree.addNode(loopNode);
					}

					// 获取循环中的所有活动
					List<String> actsInLoop = new ArrayList<String>();
					for (Block block : replacedLoopBlocks) {
						String act = block.getEntryPost().get(0);
						if (!actsInLoop.contains(act)) {
							actsInLoop.add(act);
						}
					}

					List<String> transInXor = getXorBlock.getToTransNotInLoop(entry, netFrom, loopBlocks);
					InnerNet netTo = loopBlockReplace(netFrom, actsInLoop, transInXor, entry, loopTran, pIndex);
					// Note:pIndex在loopBlockReplace更新不会影响主函数,因此需对应更新
					pIndex++;
					visitingQueue.offer(netTo);
					continue outer;
				}
			}

		}
		return tree;

	}

	/********************** 对内网进行顺序块替换 ******************************/

	public InnerNet seqBlockReplace(InnerNet net, List<String> seqActs,
			String seqTran) {

		InnerNet replaceNet = new InnerNet();

		// 1.替换后的网中的流集
		List<Flow> replacedFlows = new ArrayList<Flow>();
		// 1.1 移除所有由seqActs中变迁生成流集
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (seqActs.contains(from) || seqActs.contains(to)) {
				continue;
			} else {
				replacedFlows.add(flow);
			}
		}
		// 1.2 添加顺序块对应变迁seqTran形成流
		String placeFrom = getEntryPlaceInSeqPart(seqActs.get(0), flows);
		Flow flow1 = new Flow();
		flow1.setFlowFrom(placeFrom);
		flow1.setFlowTo(seqTran);
		replacedFlows.add(flow1);
		String placeTo = getExitPlaceInSeqPart(seqActs.get(seqActs.size() - 1), flows);
		Flow flow2 = new Flow();
		flow2.setFlowFrom(seqTran);
		flow2.setFlowTo(placeTo);
		replacedFlows.add(flow2);

		// 2.生成替换后网中的库所
		List<String> places = net.getPlaces();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				replaceNet.addPlace(from);
			}
			if (places.contains(to)) {
				replaceNet.addPlace(to);
			}
		}

		// 3.生成替换后网中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				replaceNet.addTran(from);
			}
			if (trans.contains(to)) {
				replaceNet.addTran(to);
			}
		}
		replaceNet.addTran(seqTran);

		// 返回替换网
		replaceNet.setSource(net.getSource());
		replaceNet.setSink(net.getSink());
		replaceNet.setFlows(replacedFlows);
		return replaceNet;

	}

	/********************** 对内网进行并发块替换 ******************************/

	private InnerNet andBlockReplace(InnerNet net, String entry, String exit,
			List<String> postActs, String flowTran, int pIndex) {

		InnerNet replaceNet = new InnerNet();

		// 1.替换后网中的流集
		List<Flow> replacedFlows = new ArrayList<Flow>();
		// 1.1计算与并发块中活动关联的库所集
		List<String> relPlaces = new ArrayList<String>();
		for (String postAct : postActs) {// 针对并发块中的每个迁移进行计算
			relPlaces.addAll(PetriUtils.getPreSet(postAct, net.getFlows()));
			relPlaces.addAll(PetriUtils.getPostSet(postAct, net.getFlows()));
		}
		// 1.2移除所有relPlaces生成流集
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (relPlaces.contains(from) || relPlaces.contains(to)) {
				continue;
			} else {
				replacedFlows.add(flow);
			}
		}

		// 1.3添加并发部件flowTran形成流,即entry->rp1->flowTran->rp2->exit
		// Note:pIndex未更新主函数.....................................
		Flow flow1 = new Flow();
		flow1.setFlowFrom(entry);
		String rp1 = "rp" + pIndex;
		pIndex++;
		flow1.setFlowTo(rp1);
		Flow flow2 = new Flow();
		flow2.setFlowFrom(rp1);
		flow2.setFlowTo(flowTran);
		Flow flow3 = new Flow();
		flow3.setFlowFrom(flowTran);
		String rp2 = "rp" + pIndex;
		pIndex++;
		flow3.setFlowTo(rp2);
		Flow flow4 = new Flow();
		flow4.setFlowFrom(rp2);
		flow4.setFlowTo(exit);

		replacedFlows.add(flow1);
		replacedFlows.add(flow2);
		replacedFlows.add(flow3);
		replacedFlows.add(flow4);

		// 2.生成替换后网中的库所
		List<String> places = net.getPlaces();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				replaceNet.addPlace(from);
			}
			if (places.contains(to)) {
				replaceNet.addPlace(to);
			}
		}
		replaceNet.addPlace(rp1);
		replaceNet.addPlace(rp2);

		// 3.生成替换后网中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				replaceNet.addTran(from);
			}
			if (trans.contains(to)) {
				replaceNet.addTran(to);
			}
		}
		replaceNet.addTran(flowTran);

		// 返回替换网
		replaceNet.setSource(net.getSource());
		replaceNet.setSink(net.getSink());
		replaceNet.setFlows(replacedFlows);

		return replaceNet;

	}

	/********************** 对内网进行选择块替换 ******************************/

	public InnerNet xorBlockReplace(InnerNet net, String entry, String exit,
			List<String> transInXor, String xorTran) {

		// 定义替换后的内网
		InnerNet replaceNet = new InnerNet();

		// 1.生成替换后的流
		List<Flow> replacedFlows = new ArrayList<Flow>();
		// 移除所有由transInXor中变迁生成流集
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (transInXor.contains(from) || transInXor.contains(to)) {
				continue;
			} else {
				replacedFlows.add(flow);
			}
		}
		// 添加选择部件对应变迁xorTran形成流
		Flow flow1 = new Flow();
		flow1.setFlowFrom(entry);
		flow1.setFlowTo(xorTran);
		replacedFlows.add(flow1);
		Flow flow2 = new Flow();
		flow2.setFlowFrom(xorTran);
		flow2.setFlowTo(exit);
		replacedFlows.add(flow2);

		// 2.生成替换后的网中库所
		List<String> places = net.getPlaces();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				replaceNet.addPlace(from);
			}
			if (places.contains(to)) {
				replaceNet.addPlace(to);
			}
		}

		// 3.生成替换后网中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				replaceNet.addTran(from);
			}
			if (trans.contains(to)) {
				replaceNet.addTran(to);
			}
		}
		// 添加xorTran
		replaceNet.addTran(xorTran);

		// 返回替换网
		replaceNet.setSource(net.getSource());
		replaceNet.setSink(net.getSink());
		replaceNet.setFlows(replacedFlows);
		return replaceNet;

	}

	/********************** 对内网进行循环块替换 ****************************/

	private InnerNet loopBlockReplace(InnerNet net, List<String> actsInLoop,
			List<String> transInXor, String entry, String loopTran, int pIndex) {
		InnerNet replaceNet = new InnerNet();
		// 1.替换后的网中的流集
		List<Flow> replacedFlows = new ArrayList<Flow>();
		// 1.1 移除所有由actsInLoop中变迁生成流集及从entry出发流集
		List<Flow> flows = net.getFlows();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (actsInLoop.contains(from) || actsInLoop.contains(to)
					|| from.equals(entry)) {// 从entry出发流
				continue;
			} else {
				replacedFlows.add(flow);
			}
		}

		Flow flow1 = new Flow();
		flow1.setFlowFrom(entry);
		flow1.setFlowTo(loopTran);
		replacedFlows.add(flow1);
		Flow flow2 = new Flow();
		flow2.setFlowFrom(loopTran);
		String rp = "rp" + pIndex;
		pIndex++;
		flow2.setFlowTo(rp);
		replacedFlows.add(flow2);

		for (String tranInXor : transInXor) {
			Flow flow = new Flow();
			flow.setFlowFrom(rp);
			flow.setFlowTo(tranInXor);
			replacedFlows.add(flow);
		}

		// 2.生成替换后网中的库所
		List<String> places = net.getPlaces();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (places.contains(from)) {
				replaceNet.addPlace(from);
			}
			if (places.contains(to)) {
				replaceNet.addPlace(to);
			}
		}
		replaceNet.addPlace(rp);

		// 3.生成替换后网中的变迁
		List<String> trans = net.getTrans();
		for (Flow flow : replacedFlows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (trans.contains(from)) {
				replaceNet.addTran(from);
			}
			if (trans.contains(to)) {
				replaceNet.addTran(to);
			}
		}
		replaceNet.addTran(loopTran);

		// 返回替换网
		replaceNet.setSource(net.getSource());
		replaceNet.setSink(net.getSink());
		replaceNet.setFlows(replacedFlows);
		return replaceNet;

	}

	// 获取顺序部件中入口(变迁)到达的库所
	public String getEntryPlaceInSeqPart(String entry, List<Flow> flows) {
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (to.equals(entry)) {
				return from;
			}
		}
		return null;
	}

	// 获取顺序部件中出口(变迁)到达的库所
	public String getExitPlaceInSeqPart(String exit, List<Flow> flows) {
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (from.equals(exit)) {
				return to;
			}
		}
		return null;
	}

	// 通过idf获取过程树中节点
	public Node getNodeByIdf(ProTree tree, String idf) {
		List<Node> nodes = tree.getNodes();
		for (Node node : nodes) {
			String tempIdf = node.getId();
			if (idf.equals(tempIdf)) {
				return node;
			}
		}
		return null;
	}

	// 获取元素elem的后集
	public List<String> getPostSet(String elem, List<Flow> flows) {
		List<String> postSet = new ArrayList<String>();
		for (Flow flow : flows) {
			String from = flow.getFlowFrom();
			String to = flow.getFlowTo();
			if (elem.equals(from)) {
				postSet.add(to);
			}
		}
		return postSet;
	}

	// 确定从entry出发的所有环结构均是元结构
	public List<Block> getXORLooBlocks(List<Block> maxLoopBlocks, List<Block> loopBlocks) {
		List<String> entries = getEntriesInLoopBlocks(maxLoopBlocks);
		for (String entry : entries) {
			List<Block> blocksInMax = getBlocksWithTheSameEntry(entry, maxLoopBlocks);
			List<Block> blocksInAll = getBlocksWithTheSameEntry(entry, loopBlocks);
			if (blocksInMax.size() == blocksInAll.size()) {// 从entry出发的所有环均是最大的
				return blocksInMax;
			}
		}
		return null;
	}

	// 求得从entry出发的所有环结构
	public List<Block> getBlocksWithTheSameEntry(String entry, List<Block> loopBlocks) {
		List<Block> blacks = new ArrayList<Block>();
		for (Block loopBlock : loopBlocks) {
			String tempEntry = loopBlock.getEntry();
			if (entry.equals(tempEntry)) {
				blacks.add(loopBlock);
			}
		}
		return blacks;
	}

	// 获取looParts中的所有入口(库所)
	public List<String> getEntriesInLoopBlocks(List<Block> looParts) {
		List<String> entries = new ArrayList<String>();
		for (Block part : looParts) {
			String entry = part.getEntry();
			if (!entries.contains(entry)) {
				entries.add(entry);
			}
		}
		return entries;
	}

}
