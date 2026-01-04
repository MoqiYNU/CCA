/*
 * PIPESwingToolTipListener.java
 */

package pipe.jpowergraph;

import java.awt.BorderLayout;
import java.awt.Color;
import java.util.List;
import javax.swing.JComponent;
import javax.swing.JEditorPane;
import net.sourceforge.jpowergraph.Node;
import net.sourceforge.jpowergraph.swing.manipulator.DefaultSwingToolTipListener;


//REMARK: this class extends a jpowergraph's class which is LGPL

/**
 * This class displays information about a node in its tooltip
 * @author Moqi
 */
public class RGSwingToolTipListener 
        extends DefaultSwingToolTipListener {
   
   public boolean addNodeToolTipItems(Node theNode, JComponent theJComponent,
           Color backgroundColor) {
      
      //消息字符串
      String messageString = "";
      
      if (theNode instanceof RGNode) {
    	  
    	  List<String> messages = ((RGNode) theNode).getMarking().getPlaces();
    	  if (messages.size() == 0) {
    		  messageString = "null";
		  }else {
			messageString = "{";
			for (int i = 0; i < messages.size(); i++) {
				if (i != messages.size() - 1) {
					messageString = messageString + messages.get(i) + "," + " ";
				}else {
					messageString = messageString + messages.get(i) + "}";
				}
			  }
		  }
    	  
	  }
      
      //System.out.println("messages:" + messageString);
      
      /*//标识字符串
      String markingString = "";
      
      if (theNode instanceof RGNode) {
    	  
    	  List<IdentifiedMarking> identifiedMarkings = ((RGNode) theNode).getCompState().getIdentifiedMarkings();
    	  
    	  for (IdentifiedMarking identifiedMarking : identifiedMarkings) {
    		  
			int index = identifiedMarking.getIndex();
			String bizProName = identifiedMarking.getBizPro();
			
			markingString = markingString + "</font><hr size=1><font size=3><b>" + bizProName + "[" + index + "]" + ":" + "</b>" ; 
			
			List<String> conditions = identifiedMarking.getConditions();
			
			markingString = markingString + "{";
			
			for (int i = 0; i < conditions.size(); i++) {
				if (i != conditions.size() - 1) {
					markingString = markingString + conditions.get(i) + "," + " "; 
				}else {
					markingString = markingString +conditions.get(i) + "}"; 
				}
			}
		}
      }*/
      
      //System.out.println("markingString:" + markingString);
      
      theJComponent.setLayout(new BorderLayout());
      JEditorPane editor = new JEditorPane("text/html", 
    		  "<font size=3><b>" + theNode.getLabel().replaceAll("\n", "<br>") +
              "</b> [" + theNode.getNodeType() + "]"
              + "</font><hr size=1><font size=3><b>Messages: </b>" + messageString
              + "</font>");
      editor.setBackground(new Color(255, 255, 204));
      editor.setEditable(false);
      theJComponent.add(editor);
      
      return true;
   }
   
}
