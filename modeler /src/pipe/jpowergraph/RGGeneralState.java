package pipe.jpowergraph;

import toolkits.def.petri.Marking;
import net.sourceforge.jpowergraph.painters.node.ShapeNodePainter;
import net.sourceforge.jpowergraph.swtswinginteraction.color.JPowerGraphColor;


//REMARK: this class extends a jpowergraph's class which is LGPL


/**
 * This class defines the general node used when LTS is generated
 * @author Pere Bonet
 */
public class RGGeneralState 
        extends RGNode {
   
   // light_red
   static JPowerGraphColor bgColor = new JPowerGraphColor(255, 102, 102);
   // black
   static JPowerGraphColor fgColor = JPowerGraphColor.BLACK;
    
   private static ShapeNodePainter shapeNodePainter = new ShapeNodePainter(
           ShapeNodePainter.ELLIPSE, bgColor, JPowerGraphColor.LIGHT_GRAY,
           fgColor);   

   /**
    * Creates the initial state node.
    * @param label    the node id.
    * @param marking  the marking
    */   
   public RGGeneralState(String label, Marking marking){
      super(label, marking);
   }

   
   public static ShapeNodePainter getShapeNodePainter(){
      return shapeNodePainter;
   }
   
   
   public String getNodeType(){
      return "State";
   }   

}
