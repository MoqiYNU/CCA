package toolkits.def.petri;

/**
 * @author Moqi
 * 定义流关系, 包括:
 * 1)流开始条件/活动flowFrom;
 * 2)流结束条件/活动flowTo;
 */

public class Flow {

	//flowFrom及flowTo均用id进行标识
	private String flowFrom;
	private String flowTo;
	
	public String getFlowFrom() {
		return flowFrom;
	}
	public void setFlowFrom(String flowFrom) {
		this.flowFrom = flowFrom;
	}
	public String getFlowTo() {
		return flowTo;
	}
	public void setFlowTo(String flowTo) {
		this.flowTo = flowTo;
	}
	
}
