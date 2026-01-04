package pipe.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Moqi
 * 定义迁移包,用于Petri网合并
 */
public class Bag {
	
	private List<String> trans;
	
	public Bag() {
		trans = new ArrayList<>();
	}
	
	public void addTran(String tran) {
		if (!trans.contains(tran)) {
			trans.add(tran);
		}
	}
	public void addTrans(List<String> tempTrans) {
		for (String tempTran : tempTrans) {
			if (!trans.contains(tempTran)) {
				trans.add(tempTran);
			}
		}
	}
	public List<String> getTrans() {
		return trans;
	}
	public void setTrans(List<String> trans) {
		this.trans = trans;
	}
	
	

}
