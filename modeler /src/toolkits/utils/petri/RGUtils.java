package toolkits.utils.petri;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import javax.swing.ImageIcon;

import net.sourceforge.jpowergraph.defaults.DefaultGraph;
import net.sourceforge.jpowergraph.defaults.TextEdge;
import pipe.gui.CreateGui;
import pipe.gui.widgets.GraphFrame;
import pipe.jpowergraph.RGGeneralState;
import pipe.jpowergraph.RGInitialState;
import pipe.jpowergraph.RGLoopWithTextEdge;
import pipe.jpowergraph.RGNode;
import toolkits.def.lts.LTS;
import toolkits.def.lts.LTSTran;
import toolkits.def.petri.Edge;
import toolkits.def.petri.Marking;
import toolkits.def.petri.Path;
import toolkits.def.petri.RG;
import toolkits.def.petri.Trace;

/**
 * @author Moqi
 * 定义可达图Utils
 */
public class RGUtils {
	
	
	/***************************由路径产生轨迹和变迁集*****************************/
	
	@SuppressWarnings("rawtypes")
	public static Trace genTraceFromPath(Path path) {
		
		List<String> events = new ArrayList<String>();
		List sequence = path.getSequence();
		int size = sequence.size();
		for (int i = 0; i < size; i++) {
			if (i%2 == 0) {//偶数为标识,余数为0
				continue;
			}else {
				String event = (String) sequence.get(i);
				events.add(event);
			}
		}
		Trace trace = new Trace();
		trace.setEvents(events);
	    return trace;
		
	}
	
	@SuppressWarnings("rawtypes")
	public static List<String> genTransFromPath(Path path) {
		
		List<String> events = new ArrayList<String>();
		List sequence = path.getSequence();
		int size = sequence.size();
		for (int i = 0; i < size; i++) {
			if (i%2 == 0) {//偶数为标识,余数为0
				continue;
			}else {
				String event = (String) sequence.get(i);
				events.add(event);
			}
		}
	    return events;
		
	}
	
	
    /****************************获取无环RG中合法路径******************************/
	
	@SuppressWarnings({ "rawtypes"})
	public static List<Path> getLegalPaths(RG rg) {
		
		List<Path> legalPaths = new ArrayList<Path>();
		
		Marking start = rg.getStart();
		List<Marking> visitedQueue = new ArrayList<>();
		Stack route = new Stack();
		genLegalPaths(start, route, visitedQueue, legalPaths, rg);
		return legalPaths;
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void genLegalPaths(Marking marking, Stack route, 
			List<Marking> visitedQueue, List<Path> legalPaths, RG rg) {
		
		visitedQueue.add(marking);
		route.add(marking);
		
		//获得以marking开始的边
		List<Edge> succEdges = getSuccEdges(marking, rg);
		
		if (succEdges.size() == 0) {//marking不能迁移
			
			// 1.保存该合法路径(需将route拷贝,否则在回溯过程中会清空)
			// Note:marking是一个终止标识)
			if (MarkingUtils.markingIsExist(rg.getEnds(), marking)) {
				Path path = new Path();
				List sequence = new ArrayList<>();
				for (int j = 0; j < route.size(); j++) {
					sequence.add(route.get(j));
				}
				path.setSequence(sequence);
				legalPaths.add(path);
				System.out.println("size: " + legalPaths.size() + ", legal path: " + RGUtils.genTraceFromPath(path).getEvents());
			}
			
			// 2.回溯路径(防止越界)并重置为未访问
			if (route.size() != 0) {
				route.pop();
				if (route.size() != 0) {
					//System.out.println("Back: " + rg.getTranLabelMap().get((String)route.peek()));
					route.pop();
				}
			}
			removeMarking(marking, visitedQueue);
		}else {//marking可迁移
			int succSize = succEdges.size();
			for (int i = 0; i < succSize; i++) {//Note:确保出度全部访问
				Edge edge = succEdges.get(i);
				String tran = edge.getTran();
				Marking markingTo = edge.getTo();
				if (!MarkingUtils.markingIsExist(visitedQueue, markingTo)) {//markingTo未访问
					route.add(tran);
	        		genLegalPaths(markingTo, route, visitedQueue, legalPaths, rg);
				}
				if (i == succSize - 1) {//如果无其它出度
					//回溯路径(防止越界)并重置为未访问
					if (route.size() != 0) {
						route.pop();
						if (route.size() != 0) {
							//System.out.println("Back: " + rg.getTranLabelMap().get((String)route.peek()));
							route.pop();
						}
					}
					removeMarking(marking, visitedQueue);
				}
			}
		}
	}
	
	//从visitedQueue中移除marking
	public static void removeMarking(Marking marking, List<Marking> visitedQueue) {
		for (Marking visitedMarking : visitedQueue) {
			if (MarkingUtils.twoMarkingsIsEqual(marking, visitedMarking)) {
				visitedQueue.remove(visitedMarking);
				break;
			}
		}
	}

	//获取标识marking后续边集
	public static List<Edge> getSuccEdges(Marking marking, RG rg) {
		List<Edge> succEdges = new ArrayList<Edge>();
		List<Edge> edges = rg.getEdges();
		for (Edge edge : edges) {
			Marking from = edge.getFrom();
			if (MarkingUtils.twoMarkingsIsEqual(marking, from)) {
				succEdges.add(edge);
			}
		}
		return succEdges;
		
	}
	
	
	/******************************将可达图转为LTS*******************************/
	
	public static LTS rg2lts(RG rg) {
		
		//生成LTS中状态到开放网中标识映射
		Map<String, Marking> stateMarkingMap = new HashMap<String, Marking>();
		
		LTS lts = new LTS();
		lts.setStart("S" + 0);
		
		List<String> ltsStates = new ArrayList<String>();
		List<Marking> vertexs = rg.getVertexs();
		int size = vertexs.size();
		for (int i = 0; i < size; i++) {
			String ltsState = "S" + i;
			ltsStates.add(ltsState);
			stateMarkingMap.put(ltsState, vertexs.get(i));
		}
		lts.setStates(ltsStates);
		lts.setStateMarkingMap(stateMarkingMap);
		
		List<LTSTran> ltsTrans = new ArrayList<LTSTran>();
		List<Edge> edges = rg.getEdges();
        for (Edge edge : edges) {
			Marking markingFrom = edge.getFrom();
			String tran = edge.getTran();
			Marking markingTo = edge.getTo();
			//映射为S(i)
			int from = MarkingUtils.getIndex(vertexs, markingFrom);
			int to = MarkingUtils.getIndex(vertexs, markingTo);
			LTSTran tranLTS = new LTSTran();
			tranLTS.setFrom("S" + from);
			tranLTS.setTran(tran);
			tranLTS.setTo("S" + to);
			ltsTrans.add(tranLTS);
        }
        lts.setLTSTrans(ltsTrans);
        
        //设置终止标识对应的LTS状态
        List<String> ends = new ArrayList<String>();
        List<Marking> finalMarkings = rg.getEnds();
        for (Marking finalMarking : finalMarkings) {
        	int index = MarkingUtils.getIndex(vertexs, finalMarking);
     		ends.add("S" + index);
		}
        lts.setEnds(ends);
        
        //lts.setTranLabelMap(rg.getTranLabelMap());
        
        return lts;
		
	}
	
	/**********************************在PIPE中生成可达图***********************************/
	
	// 1.在PIPE中产生LTS图
 	public static void genGraphFromLTS(LTS lts, int orgNum, int placeNum, int tranNum, int inters){
 	    DefaultGraph graph = createGraphFromLTS(lts);
 	    GraphFrame frame = new GraphFrame();
 	    int nodeNum = lts.getStates().size();
 	    int edgeNum = lts.getLTSTrans().size();
 	    String legend = "orgNum: " + orgNum + "; placeNum: " + placeNum + "; tranNum: " + tranNum + "; inters: " + inters +"\n" +
 	    		 "The RG covers: " + nodeNum + " Nodes, " + edgeNum + " Edges";
 	    frame.constructGraphFrame(graph, legend);
 	    frame.toFront();
 	    frame.setIconImage(new ImageIcon(CreateGui.imgPath + "icon.png").getImage());
 	    frame.setTitle("The RG of" + " " + CreateGui.getTab().getTitleAt(CreateGui.getTab().getSelectedIndex()));
 	}
 	
 	//创建LTS图
 	@SuppressWarnings({ "rawtypes", "unchecked" })
 	private static DefaultGraph createGraphFromLTS(LTS lts){
 			
 			DefaultGraph graph = new DefaultGraph();
 			
 			//graph结点集和边集
 			ArrayList nodes = new ArrayList();
 			ArrayList edges = new ArrayList();
 			
 			//处理可达图中的节点
 			List<String> states = lts.getStates();
 			// 1.rg中第一个状态为初始状态
 			RGInitialState ltsInitialState = new RGInitialState("S0", lts.getStateMarkingMap().get("S0"));
 			nodes.add(ltsInitialState);
 			
 			// 2.从第二个开始均为普通状态
 			for (int i = 1; i < states.size(); i++) {
 				//label为 Si
 				String label = states.get(i);
 				Marking tempMarking = lts.getStateMarkingMap().get(label);
 				RGGeneralState rgGeneralState = new RGGeneralState(label, tempMarking);
 				nodes.add(rgGeneralState);
 			}
 			
 			//处理可达图中的边
 			List<LTSTran> ltsTrans = lts.getLTSTrans();
             for (LTSTran ltsTran : ltsTrans) {
 				String from = ltsTran.getFrom();
 				//形如t1_!order
 				String tran = ltsTran.getTran();
 				String to = ltsTran.getTo();
 				TextEdge textEdge = new TextEdge((RGNode)(nodes.get(getIndex(states, from))), (RGNode)(nodes.get(getIndex(states, to))), tran);
			    edges.add(textEdge);
					
 			 }
             graph.addElements(nodes, edges);
             
             return graph;
             
 	}
 	
 	public static int getIndex(List<String> states, String label) {
 		for (int i = 0; i < states.size(); i++) {
			if (states.get(i).equals(label)) {
				return i;
			}
		}
 		return -1;
		
	}
 	
 	// 2.在PIPE中产生RG图
 	public static void genGraphFromRG(RG rg, int orgNum, int placeNum, int tranNum, int inters){
 	    DefaultGraph graph = createGraphFromRG(rg);
 	    GraphFrame frame = new GraphFrame();
 	    int nodeNum = rg.getVertexs().size();
 	    int edgeNum = rg.getEdges().size();
 	    String legend = "orgNum: " + orgNum + "; placeNum: " + placeNum + "; tranNum: " + tranNum + "; inters: " + inters +"\n" +
 	    		 "The RG covers: " + nodeNum + " Nodes, " + edgeNum + " Edges";
 	    frame.constructGraphFrame(graph, legend);
 	    frame.toFront();
 	    frame.setIconImage(new ImageIcon(CreateGui.imgPath + "icon.png").getImage());
 	    frame.setTitle("The RG of" + " " + CreateGui.getTab().getTitleAt(CreateGui.getTab().getSelectedIndex()));
 	}
 	
 	//创建RG图
 	@SuppressWarnings({ "rawtypes", "unchecked", "static-access" })
 	private static DefaultGraph createGraphFromRG(RG rg){
 			
 		    PetriUtils petriUtils = new PetriUtils();
 			DefaultGraph graph = new DefaultGraph();
 			
 			//graph结点集和边集
 			ArrayList nodes = new ArrayList();
 			ArrayList edges = new ArrayList();
 			
 			//处理可达图中的节点
 			List<Marking> states = rg.getVertexs();
 			// 1.rg中第一个状态为初始状态
 			RGInitialState ltsInitialState = new RGInitialState("S0", states.get(0));
 			nodes.add(ltsInitialState);
 			
 			// 2.从第二个开始均为普通状态
 			for (int i = 1; i < states.size(); i++) {
 				//label为 Si
 				String label = "S" + i;
 				Marking tempMarking = states.get(i);
 				RGGeneralState rgGeneralState = new RGGeneralState(label, tempMarking);
 				nodes.add(rgGeneralState);
 			}
 			
 			//处理可达图中的边
 			List<Edge> rgTrans = rg.getEdges();
             for (Edge rgTran : rgTrans) {
 				Marking markingFrom = rgTran.getFrom();
 				//形如t1_!order
 				String tran = /*rgTran.getTran() + "_" + */petriUtils.getLabel(rg.getTranLabelMap(), rgTran.getTran());
 				Marking markingTo = rgTran.getTo();
 				//映射为Si
 				int from = MarkingUtils.getIndex(states, markingFrom);
 				int to = MarkingUtils.getIndex(states, markingTo);
 				//处理循环
 				if (from == to) {//如果是循环边
 					RGLoopWithTextEdge loopWithTextEdge = new RGLoopWithTextEdge((RGNode)(nodes.get(from)), tran);
 					edges.add(loopWithTextEdge);
 				}else {//如果是非循环边
 					TextEdge textEdge = new TextEdge((RGNode)(nodes.get(from)), (RGNode)(nodes.get(to)), tran);
 					edges.add(textEdge);
 				}
 			}
             graph.addElements(nodes, edges);
             
             return graph;
             
 	}


}
