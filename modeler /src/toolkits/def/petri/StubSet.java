package toolkits.def.petri;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Moqi
 * ¶¨ÒåÎÈÌ¬¼¯
 */
public class StubSet {
	
	private List<String> trans;
	
	public StubSet() {
		trans = new ArrayList<String>();
	}

	public List<String> getTrans() {
		return trans;
	}

	public void setTrans(List<String> trans) {
		this.trans = trans;
	}
	
}
