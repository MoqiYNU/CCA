
package pipe.jpowergraph;

import net.sourceforge.jpowergraph.Node;
import net.sourceforge.jpowergraph.defaults.LoopEdge;


//REMARK: this class extends a jpowergraph's class which is LGPL

/**
 * This class defines the loop edges with text used in LTS
 * @author Moqi
 */
public class RGLoopWithTextEdge
        extends LoopEdge {
   
   private String text;
   
   
   /**
    * Creates a new instance of PIPELoopWithTextEdge
    */
   public RGLoopWithTextEdge(Node theNode, String _text) {
      super(theNode);
      text = _text;
   }
   
   public String getText(){
      return text;
   }
   
}
