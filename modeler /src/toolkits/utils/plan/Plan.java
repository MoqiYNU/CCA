package toolkits.utils.plan;

import java.util.ArrayList;
import java.util.List;

/**
 * @author QiMo
 * 定义计划,对应一个组合片段且每个片段是一个次序矩阵
 */
public class Plan {
	
	private List<ORDMatrix> matrices;
	
	public Plan() {
		matrices = new ArrayList<ORDMatrix>();
	}

	public void addORDMatrix(ORDMatrix order) {
		matrices.add(order);
	}

	public List<ORDMatrix> getMatrices() {
		return matrices;
	}
	public void setMatrices(List<ORDMatrix> matrices) {
		this.matrices = matrices;
	}
	

}
