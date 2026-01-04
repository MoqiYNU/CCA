package toolkits.utils.plan;

import java.util.List;

/**
 * @author QiMo
 * 定义次序矩阵,对应一个判断
 */
public class ORDMatrix {
	
	private int[][] graph;  //次序矩阵
	private List<String> acts;//次序矩阵中变迁集
	
	public int[][] getGraph() {
		return graph;
	}
	public void setGraph(int[][] graph) {
		this.graph = graph;
	}
	public List<String> getActs() {
		return acts;
	}
	public void setActs(List<String> acts) {
		this.acts = acts;
	}

	
}
