/*
 * Created on 04-Mar-2004
 * Author is Michael Camacho
 *
 */
package pipe.gui.action;

import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;

import pipe.dataLayer.PetriNetObject;
import pipe.dataLayer.Place;
import pipe.gui.CreateGui;

public class DeletePetriNetObjectAction
      extends AbstractAction {

   private PetriNetObject selected;

   public DeletePetriNetObjectAction(PetriNetObject component) {
      selected = component;
   }

   public void actionPerformed(ActionEvent e) {
      CreateGui.getView().getUndoManager().newEdit(); // new "transaction""
      CreateGui.getView().getUndoManager().deleteSelection(selected);
      // ÒÆ³ýÏûÏ¢¿âËùId
      if (selected instanceof Place) {
         if (((Place) selected).isMsgPlace()) {
            CreateGui.getModel().removeMsgPlace(selected.getId());
            // System.out.println("Rvm msgPlace: " + selected.getId());
         }
      }
      // System.out.println("curMsgPlaces: " + CreateGui.getModel().getMsgPlaces());
      selected.delete();
   }

}
