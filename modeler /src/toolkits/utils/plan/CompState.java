package toolkits.utils.plan;

import java.util.List;
import toolkits.def.petri.Marking;
import toolkits.utils.plan.Bag;

/**
 * @author Moqi
 * 定义面向执行计划实施中组合状态
 */
public class CompState {
	
	private String idf;
	private Marking marking;
	private List<Bag> bags;
	
	public String getIdf() {
		return idf;
	}
	public void setIdf(String idf) {
		this.idf = idf;
	}
	public Marking getMarking() {
		return marking;
	}
	public void setMarking(Marking marking) {
		this.marking = marking;
	}
	public List<Bag> getBags() {
		return bags;
	}
	public void setBags(List<Bag> bags) {
		this.bags = bags;
	}
	
}
