package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;

import pipe.dataLayer.Place;
import pipe.gui.CreateGui;

@SuppressWarnings("serial")
public class SetResPlaceAction extends AbstractAction {

	@SuppressWarnings("unused")
	private Container contentPane;
	private Place selected;

	public SetResPlaceAction(Container contentPane, Place place) {
		this.contentPane = contentPane;
		selected = place;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		
		// 1. 若selected是消息库所,则先取消其属性
		if (selected.isMsgPlace() == true) {
			selected.setMsgPlace(false);
			// 移除选择库所的Id
			CreateGui.getModel().removeMsgPlace(selected.getId());
		}
		// 2. 设置selected为资源库所
		selected.setResPlace(true);
		// 添加选择库所的Id
		CreateGui.getModel().addResPlace(selected.getId());
		// 重绘当前库所
		selected.repaint();
		
	}

}
