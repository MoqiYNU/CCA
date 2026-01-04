import static javax.swing.UIManager.get;
import static javax.swing.UIManager.getDefaults;
import static javax.swing.UIManager.put;

import java.awt.Font;
import java.util.Enumeration;

import javax.swing.plaf.FontUIResource;

import pipe.gui.CreateGui;

/**
 * -@author Qi Mo
 * 利用pipe2.5建模跨组织过程的Petri网(涉及同步,异步,资源及时间)
 */

public class RunGui {

    public static void main(String args[]) {
        // 界面字体设为简体苹方"PingFang SC"
        InitGlobalFont(new Font("PingFang SC", Font.PLAIN, 12));
        CreateGui.init();
    }

    // 统一设置字体
    private static void InitGlobalFont(Font font) {
        FontUIResource fontRes = new FontUIResource(font);
        for (Enumeration<Object> keys = getDefaults().keys(); keys.hasMoreElements();) {
            Object key = keys.nextElement();
            Object value = get(key);
            if (value instanceof FontUIResource) {
                put(key, fontRes);
            }
        }
    }
}
