package toolkits.utils.plan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author QiMo
 * 定义包:
 * 1)已执行活动;
 * 2)可用计划集;
 * 3)计划中负责片段中活动次序映射.
 */
public class Bag {
	
	//定义包中已执行活动和可用计划集
	private List<String> execActs;
	private List<Integer> availPlans;
	private List<Map<String, List<String>>> maps;
	
	public Bag() {
		execActs = new ArrayList<String>();
		availPlans = new ArrayList<Integer>();
		maps = new ArrayList<Map<String,List<String>>>();
	}
	
	public void addAct(String act) {
		execActs.add(act);
	}
	public void addPlan(Integer plan) {
		availPlans.add(plan);
	}
	public void addMap(Map<String,List<String>> map) {
		maps.add(map);
	}
	
	public List<String> getExecActs() {
		return execActs;
	}
	public void setExecActs(List<String> execActs) {
		this.execActs = execActs;
	}
	public List<Integer> getAvailPlans() {
		return availPlans;
	}
	public void setAvailPlans(List<Integer> availPlans) {
		this.availPlans = availPlans;
	}
	public List<Map<String, List<String>>> getMaps() {
		return maps;
	}
	public void setMaps(List<Map<String, List<String>>> maps) {
		this.maps = maps;
	}
	

}
