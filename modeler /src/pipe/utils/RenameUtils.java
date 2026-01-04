package pipe.utils;

import pipe.gui.CreateGui;

/**
 * @author Moqi
 * 判断Tab名字是否重复
 */
public class RenameUtils {
	
	//判断Tab名字是否重复
	public static boolean isDuplicatedTabName(String name){
		int size = CreateGui.getTab().getTabCount();
        for (int i = 0; i < size; i++) {
			String tempName = CreateGui.getTab().getTitleAt(i);
			if (name.equals(tempName)) {
				return true;
			}
	    }
        return false;
	}

}
