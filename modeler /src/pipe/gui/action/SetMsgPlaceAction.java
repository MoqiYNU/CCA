package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;

import pipe.dataLayer.Place;
import pipe.gui.CreateGui;

@SuppressWarnings("serial")
public class SetMsgPlaceAction extends AbstractAction {

	@SuppressWarnings("unused")
	private Container contentPane;
	private Place selected;

	public SetMsgPlaceAction(Container contentPane, Place place) {
		this.contentPane = contentPane;
		selected = place;
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		
		// 1. 若selected是资源库所,则先取消其属性
		if (selected.isResPlace() == true) {
			selected.setResPlace(false);
			// 添加选择库所的Id
			CreateGui.getModel().removeResPlace(selected.getId());
		}
		// 2. 设置selected为消息库所
		selected.setMsgPlace(true);
		// 添加选择库所的Id
		CreateGui.getModel().addMsgPlace(selected.getId());
		// 重绘当前库所
		selected.repaint();
		
	}

}
