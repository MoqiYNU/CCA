/*
 * PIPENode.java
 */

package pipe.jpowergraph;

import toolkits.def.petri.Marking;
import net.sourceforge.jpowergraph.defaults.DefaultNode;
import net.sourceforge.jpowergraph.painters.node.ShapeNodePainter;
import net.sourceforge.jpowergraph.swtswinginteraction.color.JPowerGraphColor;


/**
 * This class defines the default node for LTS
 * @author moqi
 */
public abstract class RGNode 
        extends DefaultNode {
   
   // the state id, used in the graph's legend
   private String label = "";
   
   // marking used in the ToolTip
   private Marking marking = null;
   
   // gray
   private static JPowerGraphColor bgColor = new JPowerGraphColor(128, 128, 128);
   // black
   protected static JPowerGraphColor fgColor = new JPowerGraphColor(0, 0, 0);
   
   // the ShapeNodePainter for this node
   private static ShapeNodePainter shapeNodePainter = new ShapeNodePainter(
           ShapeNodePainter.ELLIPSE, bgColor, bgColor, fgColor);
   
   /**
    * Creates a new node instance.
    * @param _label    the node id.
    * @param _marking  the marking
    */
   public RGNode(String _label, Marking _marking){
      this.label = _label;
      this.marking = _marking;
   }
   
   
   public String getLabel() {
      return label;
   }
   
   
   public String getNodeType(){
      return "RGNode";
   }

   public Marking getMarking() {
	return marking;
}


public void setMarking(Marking marking) {
	this.marking = marking;
}


public static ShapeNodePainter getShapeNodePainter(){
      return shapeNodePainter;
   }
   
}
