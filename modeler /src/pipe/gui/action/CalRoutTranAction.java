package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;

import pipe.dataLayer.Transition;
import pipe.gui.CreateGui;

@SuppressWarnings("serial")
public class CalRoutTranAction extends AbstractAction {

	@SuppressWarnings("unused")
	private Container contentPane;
	private Transition selected;

	public CalRoutTranAction(Container contentPane, Transition transition) {
		this.contentPane = contentPane;
		selected = transition;
	}

	@Override
	public void actionPerformed(ActionEvent arg0) {
		selected.setRoutTran(false);
		// 移除选择路由变迁的Id
		CreateGui.getModel().removeRoutTran(selected.getId());
		// 重绘当前变迁
		selected.repaint();
	}

}
