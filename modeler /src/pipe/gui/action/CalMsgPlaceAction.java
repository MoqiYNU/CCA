package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;
import javax.swing.AbstractAction;
import pipe.dataLayer.Place;
import pipe.gui.CreateGui;

public class CalMsgPlaceAction extends AbstractAction {

	@SuppressWarnings("unused")
	private Container contentPane;
	private Place selected;

	public CalMsgPlaceAction(Container contentPane, Place place) {
		this.contentPane = contentPane;
		selected = place;
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		selected.setMsgPlace(false);
		// 添加选择库所的Id
		CreateGui.getModel().removeMsgPlace(selected.getId());
		// 重绘当前库所
		selected.repaint();

	}

}
