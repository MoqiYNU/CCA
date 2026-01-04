package toolkits.utils.block;

import java.util.ArrayList;
import java.util.List;

/**
 * *@author Moqi
 * 定义过程网中的基本块
 */
public class Block {

	// 基本块入口
	private String entry;
	// 基本块出口
	private String exit;
	// 入口后关联库所/变迁集
	private List<String> entryPost;
	// 出口前关联库所/变迁集
	private List<String> exitPre;
	// 顺序块中变迁集
	private List<String> seqActs;
	// 基本块类型
	private String type;

	public Block() {
		entryPost = new ArrayList<>();
		exitPre = new ArrayList<>();
		seqActs = new ArrayList<>();
	}

	/**************************** Utils方法 ******************************/

	public void addEntryPost(String act) {
		if (!entryPost.contains(act)) {
			entryPost.add(act);
		}
	}

	public void addExitPre(String act) {
		if (!exitPre.contains(act)) {
			exitPre.add(act);
		}
	}

	public void addSeqActs(String act) {
		if (!seqActs.contains(act)) {
			seqActs.add(act);
		}
	}

	/************************* Get和Set方法 ****************************/

	public String getEntry() {
		return entry;
	}

	public void setEntry(String entry) {
		this.entry = entry;
	}

	public String getExit() {
		return exit;
	}

	public void setExit(String exit) {
		this.exit = exit;
	}

	public List<String> getEntryPost() {
		return entryPost;
	}

	public void setEntryPost(List<String> entryActs) {
		this.entryPost = entryActs;
	}

	public List<String> getExitPre() {
		return exitPre;
	}

	public void setExitPre(List<String> exitActs) {
		this.exitPre = exitActs;
	}

	public List<String> getSeqActs() {
		return seqActs;
	}

	public void setSeqActs(List<String> seqActs) {
		this.seqActs = seqActs;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
