package toolkits.def.lts;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import toolkits.def.petri.Marking;

/**
 * @author Moqi
 * 定义标号迁移系统
 */
public class LTS {
	
	private String start;
	private List<String> ends;
	private List<String> states;
	private List<LTSTran> ltsTrans;
	private Map<String, Marking> stateMarkingMap;
	
	public LTS() {
		stateMarkingMap = new HashMap<String, Marking>();
	}
	
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public List<String> getEnds() {
		return ends;
	}
	public void setEnds(List<String> ends) {
		this.ends = ends;
	}
	public List<String> getStates() {
		return states;
	}
	public void setStates(List<String> states) {
		this.states = states;
	}
	public List<LTSTran> getLTSTrans() {
		return ltsTrans;
	}
	public void setLTSTrans(List<LTSTran> ltstrans) {
		this.ltsTrans = ltstrans;
	}
	public Map<String, Marking> getStateMarkingMap() {
		return stateMarkingMap;
	}
	public void setStateMarkingMap(Map<String, Marking> stateMarkingMap) {
		this.stateMarkingMap = stateMarkingMap;
	}

}
