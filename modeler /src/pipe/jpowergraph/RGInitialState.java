
package pipe.jpowergraph;

import toolkits.def.petri.Marking;
import net.sourceforge.jpowergraph.painters.node.ShapeNodePainter;
import net.sourceforge.jpowergraph.swtswinginteraction.color.JPowerGraphColor;


//REMARK: this class extends a jpowergraph's class which is LGPL

/**
 * The node that represents the initial marking in the LTS.
 * @author Moqi
 */
public class RGInitialState 
        extends RGNode {
   
   // light_blue
   static JPowerGraphColor bgColor = new JPowerGraphColor(182, 220, 255);
   
   // black
   static JPowerGraphColor fgColor = new JPowerGraphColor(0, 0, 0);
   
   // a rectangle 
   static ShapeNodePainter shapeNodePainter = new ShapeNodePainter(
           ShapeNodePainter.RECTANGLE, bgColor, JPowerGraphColor.LIGHT_GRAY, 
           fgColor);   
   
   /**
    * Creates the initial state node.
    * @param label    the node id.
    * @param marking  the marking
    */   
   public RGInitialState(String label, Marking marking){
      super(label, marking);   
   }
   

   public static ShapeNodePainter getShapeNodePainter(){
      return shapeNodePainter;
   }
   
   
   public String getNodeType(){
      return "Initial State";
   }      
   
}
