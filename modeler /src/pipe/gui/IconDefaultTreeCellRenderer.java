package pipe.gui;

import java.awt.Component;
import java.awt.Image;

import javax.swing.ImageIcon;
import javax.swing.JTree;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;

/**
 *设置模块树中的图标
 */
@SuppressWarnings("serial")
public class IconDefaultTreeCellRenderer extends DefaultTreeCellRenderer {

    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected,
                                                  boolean expanded, boolean leaf,
                                                  int row, boolean hasFocus){
        super.getTreeCellRendererComponent(tree,value,selected,expanded,leaf,row,hasFocus);
        DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
        String tmp = node.toString();
        if (tmp.equals("Module Manager")){
        	ImageIcon imageIcon = new ImageIcon("src/Images/root.png");
            Image image = imageIcon.getImage();
            //将32px*32px图片压缩为16px*16px(不失真)
            image = image.getScaledInstance(16, 16, Image.SCALE_DEFAULT);
            imageIcon.setImage(image);
            this.setIcon(imageIcon);
        }else if (tmp.equals("Modules")){
        	ImageIcon imageIcon = new ImageIcon("src/Images/root.png");
            Image image = imageIcon.getImage();
            //将32px*32px图片压缩为16px*16px(不失真)
            image = image.getScaledInstance(16, 16, Image.SCALE_DEFAULT);
            imageIcon.setImage(image);
            this.setIcon(imageIcon);
        }else if (tmp.equals("Find Module")){
        	ImageIcon imageIcon = new ImageIcon("src/Images/Find Module.png");
            Image image = imageIcon.getImage();
            //将32px*32px图片压缩为16px*16px(不失真)
            image = image.getScaledInstance(16, 16, Image.SCALE_DEFAULT);
            imageIcon.setImage(image);
            this.setIcon(imageIcon);
        }else {
            ImageIcon imageIcon = new ImageIcon("src/Images/module.png");
            Image image = imageIcon.getImage();
            //将32px*32px图片压缩为16px*16px(不失真)
            image = image.getScaledInstance(16, 16, Image.SCALE_DEFAULT);
            imageIcon.setImage(image);
            this.setIcon(imageIcon);
        }
        return this;
    }


}
