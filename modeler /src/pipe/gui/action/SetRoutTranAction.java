package pipe.gui.action;

import java.awt.Container;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;

import pipe.dataLayer.Transition;
import pipe.gui.CreateGui;

@SuppressWarnings("serial")
public class SetRoutTranAction extends AbstractAction{

	 @SuppressWarnings("unused")
	 private Container contentPane;
	 private Transition selected;


	 public SetRoutTranAction(Container contentPane, Transition transition) {
	    this.contentPane = contentPane;
	    selected = transition;
	 }
	
	@Override
	public void actionPerformed(ActionEvent e) {
		
		//设置selected为路由变迁
		selected.setRoutTran(true);
		//添加选择变迁的Id
		CreateGui.getModel().addRoutTran(selected.getId());
		//重绘当前库所
		selected.repaint();
		
	}

 }
