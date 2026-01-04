package toolkits.utils.plan;

import java.util.List;

/**
 * @author Moqi
 * 定义消息库所的前后集
 */
public class MsgPlaceBag {
	
	private List<String> preSet;
	private List<String> postSet;
	
	public List<String> getPreSet() {
		return preSet;
	}
	public void setPreSet(List<String> preSet) {
		this.preSet = preSet;
	}
	public List<String> getPostSet() {
		return postSet;
	}
	public void setPostSet(List<String> postSet) {
		this.postSet = postSet;
	}
	

}
