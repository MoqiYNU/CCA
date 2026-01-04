package toolkits.def.petri;

import java.util.List;
import java.util.Map;

/**
 * @author Moqi
 * 定义过程网的可达图
 */
public class RG {
	
	private Marking start;
	private List<Marking> ends;
	private List<Marking> vertexs;
	private List<Edge> edges;
	private Map<String, String> tranLabelMap;//标号函数
	
	public Marking getStart() {
		return start;
	}
	public void setStart(Marking start) {
		this.start = start;
	}
	public List<Marking> getEnds() {
		return ends;
	}
	public void setEnds(List<Marking> ends) {
		this.ends = ends;
	}
	public List<Marking> getVertexs() {
		return vertexs;
	}
	public void setVertexs(List<Marking> vertexs) {
		this.vertexs = vertexs;
	}
	public List<Edge> getEdges() {
		return edges;
	}
	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}
	public Map<String, String> getTranLabelMap() {
		return tranLabelMap;
	}
	public void setTranLabelMap(Map<String, String> tranLabelMap) {
		this.tranLabelMap = tranLabelMap;
	}

}
