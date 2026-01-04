package toolkits.def.petri;

import java.util.List;

/**
 * @author Moqi
 * 定义可达图中路径
 */
public class Path {
	
	@SuppressWarnings("rawtypes")
	private List sequence;

	@SuppressWarnings("rawtypes")
	public List getSequence() {
		return sequence;
	}

	@SuppressWarnings("rawtypes")
	public void setSequence(List sequence) {
		this.sequence = sequence;
	}
	
}
