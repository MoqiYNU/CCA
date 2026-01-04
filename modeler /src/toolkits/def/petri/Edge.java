package toolkits.def.petri;

/**
 * @author Moqi
 * 定义可达图中边,包括开始标识,变迁及终止标识
 */
public class Edge {
	
	private Marking from;
	private String tran;
	private Marking to;
	
	public Marking getFrom() {
		return from;
	}
	public void setFrom(Marking from) {
		this.from = from;
	}
	public String getTran() {
		return tran;
	}
	public void setTran(String tran) {
		this.tran = tran;
	}
	public Marking getTo() {
		return to;
	}
	public void setTo(Marking to) {
		this.to = to;
	}
 
}
