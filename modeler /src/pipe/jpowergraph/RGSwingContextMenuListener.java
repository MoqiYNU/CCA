
package pipe.jpowergraph;

import javax.swing.JPopupMenu;

import net.sourceforge.jpowergraph.Edge;
import net.sourceforge.jpowergraph.Graph;
import net.sourceforge.jpowergraph.Node;
import net.sourceforge.jpowergraph.lens.LensSet;
import net.sourceforge.jpowergraph.swing.manipulator.DefaultSwingContextMenuListener;


//REMARK: this class extends a jpowergraph's class which is LGPL


/**
 * This class prevents from showing context menus in order to make the LTS 
 * "uneditable" (there's no "delete node" or "delete edge")
 * @author Moqi
 */
public class RGSwingContextMenuListener 
        extends DefaultSwingContextMenuListener {
   
   /** Creates a new instance of NewClass */
   public RGSwingContextMenuListener(Graph theGraph, LensSet theLensSet, 
           Integer[] theZoomLevels, Integer[] theRotateAngles) {
      super(theGraph, theLensSet, theZoomLevels, theRotateAngles);
   }
   
   
   public void fillNodeContextMenu(final Node theNode, JPopupMenu theMenu) {
      ;//
   }
   
   
   public void fillEdgeContextMenu(final Edge theEdge, JPopupMenu theMenu) {
      ;
   }
   
}
