package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;
import javax.swing.AbstractAction;
import pipe.dataLayer.Place;
import pipe.gui.CreateGui;

public class CalResPlaceAction extends AbstractAction {

	@SuppressWarnings("unused")
	private Container contentPane;
	private Place selected;

	public CalResPlaceAction(Container contentPane, Place place) {
		this.contentPane = contentPane;
		selected = place;
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		selected.setResPlace(false);
		// 添加选择库所的Id
		CreateGui.getModel().removeResPlace(selected.getId());
		// 重绘当前库所
		selected.repaint();

	}

}
