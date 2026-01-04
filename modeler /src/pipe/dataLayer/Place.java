package pipe.dataLayer;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Container;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Insets;
import java.awt.RenderingHints;
import java.awt.Shape;
import java.awt.geom.Ellipse2D;

import javax.swing.BoxLayout;

import pipe.gui.CreateGui;
import pipe.gui.Grid;
import pipe.gui.Pipe;
import pipe.gui.Zoomer;
import pipe.gui.undo.ChangeMarkingParameterEdit;
import pipe.gui.undo.ClearMarkingParameterEdit;
import pipe.gui.undo.PlaceCapacityEdit;
import pipe.gui.undo.PlaceMarkingEdit;
import pipe.gui.undo.SetMarkingParameterEdit;
import pipe.gui.undo.UndoableEdit;
import pipe.gui.widgets.EscapableDialog;
import pipe.gui.widgets.PlaceEditorPanel;

/**
 * <b>Place</b> - Petri-Net Place Class
 *
 * @see
 *      <p>
 *      <a href="..\PNMLSchema\index.html">PNML - Petri-Net XMLSchema
 *      (stNet.xsd)</a>
 * @see
 *      </p>
 *      <p>
 *      <a href="..\..\..\UML\dataLayer.html">UML - PNML Package </a>
 *      </p>
 * @version 1.0
 * @author James D Bloom
 * 
 * @author Edwin Chung corresponding states of matrixes has been set
 *         to change when markings are altered. Users will be prompted to save
 *         their
 *         work when the markings of places are altered. (6th Feb 2007)
 * 
 * @author Edwin Chung 16 Mar 2007: modified the constructor and several
 *         other functions so that DataLayer objects can be created outside the
 *         GUI
 */
public class Place extends PlaceTransitionObject {

   public final static String type = "Place";
   /** Initial Marking */
   private Integer initialMarking = 0;

   /** Current Marking */
   private Integer currentMarking = 0;

   /** Initial Marking X-axis Offset */
   private Double markingOffsetX = 0d;

   /** Initial Marking Y-axis Offset */
   private Double markingOffsetY = 0d;

   /** Value of the capacity restriction; 0 means no capacity restriction */
   private Integer capacity = 0;
   /*
    * private boolean strongCapacity = false;
    */
   // 是否为消息库所
   private boolean isMsgPlace = false;

   // 是否为资源库所
   private boolean isResPlace = false;

   public static final int DIAMETER = Pipe.PLACE_TRANSITION_HEIGHT;

   /** Token Width */
   public static int tWidth = 4;

   /** Token Height */
   public static int tHeight = 4;

   /** Ellipse2D.Double place */
   private static Ellipse2D.Double place = new Ellipse2D.Double(0, 0, DIAMETER, DIAMETER);
   private static Shape proximityPlace = (new BasicStroke(Pipe.PLACE_TRANSITION_PROXIMITY_RADIUS))
         .createStrokedShape(place);

   private MarkingParameter markingParameter = null;

   /**
    * Create Petri-Net Place object
    * 
    * @param positionXInput      X-axis Position
    * @param positionYInput      Y-axis Position
    * @param idInput             Place id
    * @param nameInput           Name
    * @param nameOffsetXInput    Name X-axis Position
    * @param nameOffsetYInput    Name Y-axis Position
    * @param initialMarkingInput Initial Marking
    * @param markingOffsetXInput Marking X-axis Position
    * @param markingOffsetYInput Marking Y-axis Position
    * @param capacityInput       Capacity
    */
   public Place(double positionXInput, double positionYInput,
         String idInput,
         String nameInput,
         Double nameOffsetXInput, Double nameOffsetYInput,
         int initialMarkingInput,
         double markingOffsetXInput, double markingOffsetYInput,
         int capacityInput) {
      super(positionXInput, positionYInput,
            idInput,
            nameInput,
            nameOffsetXInput, nameOffsetYInput);
      initialMarking = new Integer(initialMarkingInput);
      currentMarking = new Integer(initialMarkingInput);
      markingOffsetX = new Double(markingOffsetXInput);
      markingOffsetY = new Double(markingOffsetYInput);
      componentWidth = DIAMETER;
      componentHeight = DIAMETER;
      setCapacity(capacityInput);
      setCentre((int) positionX, (int) positionY);
      // updateBounds();
   }

   /**
    * Create Petri-Net Place object
    * 
    * @param positionXInput X-axis Position
    * @param positionYInput Y-axis Position
    */
   public Place(double positionXInput, double positionYInput) {
      super(positionXInput, positionYInput);
      componentWidth = DIAMETER;
      componentHeight = DIAMETER;
      setCentre((int) positionX, (int) positionY);
      // updateBounds();
   }

   public Place paste(double x, double y, boolean fromAnotherView) {

      Place copy = new Place(
            Grid.getModifiedX(x + this.getX() + Pipe.PLACE_TRANSITION_HEIGHT / 2),
            Grid.getModifiedY(y + this.getY() + Pipe.PLACE_TRANSITION_HEIGHT / 2));

      // 1.获取复制数.Note:必须赋值否则copyNum递增
      int curCopyNum = this.getCopyNumber();

      // 2.利用curCopyNum设置变迁id
      // copy.id = this.getId() + "." + (curCopyNum - 1);
      // copy.pnName.setName(this.pnName.getName() + "." + (curCopyNum - 1));

      copy.id = this.getId() + "(" + (curCopyNum - 1) + ")";
      copy.pnName.setName(this.pnName.getName()
            + "(" + (curCopyNum - 1) + ")");

      // System.out.println("curr copyNum: " + curCopyNum);

      // 添加复制消息库所的Id
      if (this.isMsgPlace) {
         CreateGui.getModel().addMsgPlace(copy.getId());
      }
      // 添加复制资源库所的Id
      if (this.isResPlace) {
         CreateGui.getModel().addResPlace(copy.getId());
      }


      this.newCopy(copy);

      copy.nameOffsetX = this.nameOffsetX;
      copy.nameOffsetY = this.nameOffsetY;
      copy.capacity = this.capacity;
      copy.attributesVisible = this.attributesVisible;
      copy.initialMarking = this.initialMarking;
      copy.currentMarking = this.currentMarking;
      copy.markingOffsetX = this.markingOffsetX;
      copy.markingOffsetY = this.markingOffsetY;
      copy.markingParameter = this.markingParameter;

      // 更新是否为消息库所属性
      copy.isMsgPlace = this.isMsgPlace;
      // 更新是否为资源库所属性
      copy.isResPlace = this.isResPlace;
      copy.update();

      return copy;

   }

   public Place copy() {

      Place copy = new Place(Zoomer.getUnzoomedValue(this.getX(), zoom),
            Zoomer.getUnzoomedValue(this.getY(), zoom));

      // 复制时设置库所id和消息,资源,链接和数据库所
      copy.setId(this.getId());
      copy.setMsgPlace(this.isMsgPlace);
      copy.setResPlace(this.isResPlace);

      // 添加消息库所的Id
      if (this.isMsgPlace) {
         CreateGui.getModel().addMsgPlace(this.getId());
      }

      // 添加资源库所的Id
      if (this.isResPlace) {
         CreateGui.getModel().addResPlace(this.getId());
      }


      copy.pnName.setName(this.getName());
      copy.nameOffsetX = this.nameOffsetX;
      copy.nameOffsetY = this.nameOffsetY;
      copy.capacity = this.capacity;
      copy.attributesVisible = this.attributesVisible;
      copy.initialMarking = this.initialMarking;
      copy.currentMarking = this.currentMarking;
      copy.markingOffsetX = this.markingOffsetX;
      copy.markingOffsetY = this.markingOffsetY;
      copy.markingParameter = this.markingParameter;
      copy.setOriginal(this);

      return copy;

   }

   /**
    * Paints the Place component taking into account the number of tokens from
    * the currentMarking
    * 
    * @param g The Graphics object onto which the Place is drawn.
    */
   public void paintComponent(Graphics g) {
      super.paintComponent(g);
      Graphics2D g2 = (Graphics2D) g;

      Insets insets = getInsets();
      int x = insets.left;
      int y = insets.top;

      if (hasCapacity()) {
         g2.setStroke(new BasicStroke(2.0f));
      } else {
         g2.setStroke(new BasicStroke(1.0f));
      }
      g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING,
            RenderingHints.VALUE_ANTIALIAS_ON);

      if (selected && !ignoreSelection) {
         g2.setColor(Pipe.SELECTION_FILL_COLOUR);
      } else {
         g2.setColor(Pipe.ELEMENT_FILL_COLOUR);
      }
      g2.fill(place);

      if (selected && !ignoreSelection) {
         g2.setPaint(Pipe.SELECTION_LINE_COLOUR);
      } else {
         g2.setPaint(Pipe.ELEMENT_LINE_COLOUR);
      }
      g2.draw(place);

      if (isMsgPlace) {// 若为消息库所,则设置为暗红色
         Graphics2D g3 = (Graphics2D) g;
         float lineWidth = 2.0f;
         ((Graphics2D) g3).setStroke(new BasicStroke(lineWidth));
         g3.setColor(new Color(87, 136, 38));
         g3.fill(place);
         g3.draw(place);
      }

      if (isResPlace) {// 若为资源库所,则设置为蓝色双环
         Graphics2D g3 = (Graphics2D) g;
         float lineWidth = 1.0f;
         ((Graphics2D) g3).setStroke(new BasicStroke(lineWidth));
         g3.setColor(new Color(46, 67, 246));
         g3.draw(place);
         Ellipse2D.Double circle = new Ellipse2D.Double(-2, -2, DIAMETER+4, DIAMETER+4);
         g3.draw(circle);
      }



      /*
       * if (isSource) {//若为源库所,设置黑色
       * Graphics2D g3 = (Graphics2D)g;
       * float lineWidth = 3.0f;
       * ((Graphics2D)g3).setStroke(new BasicStroke(lineWidth));
       * g3.setColor(Color.BLACK);
       * g3.draw(place);
       * }
       * 
       * if (isSink) {//若为终止库所,则设置为青色
       * Graphics2D g3 = (Graphics2D)g;
       * float lineWidth = 3.0f;
       * ((Graphics2D)g3).setStroke(new BasicStroke(lineWidth));
       * g3.setColor(Color.GREEN);
       * g3.draw(place);
       * }
       */

      g2.setStroke(new BasicStroke(1.0f));

      int marking = getCurrentMarking();

      // structure sees how many markings there are and fills the place in with
      // the appropriate number.
      switch (marking) {
         case 5:
            g.drawOval(x + 4, y + 5, tWidth, tHeight);
            g.fillOval(x + 4, y + 5, tWidth, tHeight);
            /* falls through */
         case 4:
            g.drawOval(x + 16, y + 16, tWidth, tHeight);
            g.fillOval(x + 16, y + 16, tWidth, tHeight);
            /* falls through */
         case 3:
            g.drawOval(x + 4, y + 16, tWidth, tHeight);
            g.fillOval(x + 4, y + 16, tWidth, tHeight);
            /* falls through */
         case 2:
            g.drawOval(x + 16, y + 5, tWidth, tHeight);
            g.fillOval(x + 16, y + 5, tWidth, tHeight);
            /* falls through */
         case 1:
            g.drawOval(x + 10, y + 11, tWidth, tHeight);
            g.fillOval(x + 10, y + 11, tWidth, tHeight);
            break;
         case 0:
            break;
         default:
            g.drawString(String.valueOf(marking), x + 8, y + 17);
            break;
      }
   }

   /**
    * Set initial marking
    * 
    * @param initialMarkingInput Integer value for initial marking
    */
   public void setInitialMarking(int initialMarkingInput) {
      initialMarking = new Integer(initialMarkingInput);
   }

   /**
    * Set current marking
    * 
    * @param currentMarkingInput Integer value for current marking
    */
   public UndoableEdit setCurrentMarking(int currentMarkingInput) {
      int oldMarking = currentMarking;

      if (capacity == 0) {
         currentMarking = currentMarkingInput;
      } else {
         if (currentMarkingInput > capacity) {
            currentMarking = capacity;
         } else {
            currentMarking = currentMarkingInput;
         }
      }
      repaint();
      return new PlaceMarkingEdit(this, oldMarking, currentMarking);
   }

   /**
    * Set capacity
    * This method doesn't check if marking fulfilles current capacity restriction
    * 
    * @param newCapacity Integer value for capacity restriction
    */
   public UndoableEdit setCapacity(int newCapacity) {
      int oldCapacity = capacity;

      if (capacity != newCapacity) {
         capacity = newCapacity;
         update();
      }
      return new PlaceCapacityEdit(this, oldCapacity, newCapacity);
   }

   /**
    * Get initial marking
    * 
    * @return Integer value for initial marking
    */
   public int getInitialMarking() {
      return ((initialMarking == null) ? 0 : initialMarking.intValue());
   }

   /**
    * Get current marking
    * 
    * @return Integer value for current marking
    */
   public int getCurrentMarking() {
      return ((currentMarking == null) ? 0 : currentMarking.intValue());
   }

   /**
    * Get current capacity
    * 
    * @return Integer value for current capacity
    */
   public int getCapacity() {
      return ((capacity == null) ? 0 : capacity.intValue());
   }

   /**
    * Get current marking
    * 
    * @return Integer value for current marking
    */
   public Integer getCurrentMarkingObject() {
      return currentMarking;
   }

   /**
    * Get X-axis offset for initial marking
    * 
    * @return Double value for X-axis offset of initial marking
    */
   public Double getMarkingOffsetXObject() {
      return markingOffsetX;
   }

   /**
    * Get Y-axis offset for initial marking
    * 
    * @return Double value for X-axis offset of initial marking
    */
   public Double getMarkingOffsetYObject() {
      return markingOffsetY;
   }

   /**
    * Returns the diameter of this Place at the current zoom
    */
   private int getDiameter() {
      return (int) (Zoomer.getZoomedValue(DIAMETER, zoom));
   }

   public boolean contains(int x, int y) {
      double unZoomedX = Zoomer.getUnzoomedValue(x - COMPONENT_DRAW_OFFSET, zoom);
      double unZoomedY = Zoomer.getUnzoomedValue(y - COMPONENT_DRAW_OFFSET, zoom);

      someArc = CreateGui.getView().createArc;
      if (someArc != null) { // Must be drawing a new Arc if non-NULL.
         if ((proximityPlace.contains((int) unZoomedX, (int) unZoomedY)
               || place.contains((int) unZoomedX, (int) unZoomedY))
               && areNotSameType(someArc.getSource())) {
            // assume we are only snapping the target...
            if (someArc.getTarget() != this) {
               someArc.setTarget(this);
            }
            someArc.updateArcPosition();
            return true;
         } else {
            if (someArc.getTarget() == this) {
               someArc.setTarget(null);
               updateConnected();
            }
            return false;
         }
      } else {
         return place.contains((int) unZoomedX, (int) unZoomedY);
      }
   }

   /*
    * (non-Javadoc)
    * 
    * @see pipe.dataLayer.PlaceTransitionObject#updateEndPoint(pipe.dataLayer.Arc)
    */
   public void updateEndPoint(Arc arc) {
      if (arc.getSource() == this) {
         // Make it calculate the angle from the centre of the place rather than
         // the current start point
         arc.setSourceLocation(positionX + (getDiameter() * 0.5),
               positionY + (getDiameter() * 0.5));
         double angle = arc.getArcPath().getStartAngle();
         arc.setSourceLocation(positionX + centreOffsetLeft()
               - (0.5 * getDiameter() * (Math.sin(angle))),
               positionY + centreOffsetTop()
                     + (0.5 * getDiameter() * (Math.cos(angle))));
      } else {
         // Make it calculate the angle from the centre of the place rather than the
         // current target point
         arc.setTargetLocation(positionX + (getDiameter() * 0.5),
               positionY + (getDiameter() * 0.5));
         double angle = arc.getArcPath().getEndAngle();
         arc.setTargetLocation(positionX + centreOffsetLeft()
               - (0.5 * getDiameter() * (Math.sin(angle))),
               positionY + centreOffsetTop()
                     + (0.5 * getDiameter() * (Math.cos(angle))));
      }
   }

   public void toggleAttributesVisible() {
      attributesVisible = !attributesVisible;
      update();
   }

   public boolean hasCapacity() {
      return capacity > 0;
   }

   public void addedToGui() {
      super.addedToGui();
      update();
   }

   public void showEditor() {
      // Build interface
      EscapableDialog guiDialog = new EscapableDialog(CreateGui.getApp(), "PIPE2", true);

      Container contentPane = guiDialog.getContentPane();

      // 1 Set layout
      contentPane.setLayout(new BoxLayout(contentPane, BoxLayout.PAGE_AXIS));

      // 2 Add Place editor
      contentPane.add(new PlaceEditorPanel(guiDialog.getRootPane(),
            this, CreateGui.getModel(), CreateGui.getView()));

      guiDialog.setResizable(false);

      // Make window fit contents' preferred size
      guiDialog.pack();

      // Move window to the middle of the screen
      guiDialog.setLocationRelativeTo(null);
      guiDialog.setVisible(true);
   }

   public void update() {
      if (attributesVisible == true) {
         pnName.setText("\nk=" + (capacity > 0 ? capacity : "\u221E") +
               (markingParameter != null ? "\nm=" + markingParameter.toString() : ""));
      } else {
         pnName.setText("");
      }
      pnName.zoomUpdate(zoom);
      super.update();
      repaint();
   }

   public void delete() {
      if (markingParameter != null) {
         markingParameter.remove(this);
         markingParameter = null;
      }
      super.delete();
   }

   public UndoableEdit setMarkingParameter(MarkingParameter _markingParameter) {
      int oldMarking = currentMarking;

      markingParameter = _markingParameter;
      markingParameter.add(this);
      currentMarking = markingParameter.getValue();
      update();
      return new SetMarkingParameterEdit(this, oldMarking, markingParameter);
   }

   public UndoableEdit clearMarkingParameter() {
      MarkingParameter oldMarkingParameter = markingParameter;

      markingParameter.remove(this);
      markingParameter = null;
      update();
      return new ClearMarkingParameterEdit(this, oldMarkingParameter);
   }

   public UndoableEdit changeMarkingParameter(MarkingParameter _markingParameter) {
      MarkingParameter oldMarkingParameter = markingParameter;

      markingParameter.remove(this);
      markingParameter = _markingParameter;
      markingParameter.add(this);
      currentMarking = markingParameter.getValue();
      update();
      return new ChangeMarkingParameterEdit(
            this, oldMarkingParameter, markingParameter);
   }

   public MarkingParameter getMarkingParameter() {
      return markingParameter;
   }

   public boolean isMsgPlace() {
      return isMsgPlace;
   }

   public void setMsgPlace(boolean isMsgPlace) {
      this.isMsgPlace = isMsgPlace;
   }

   public boolean isResPlace() {
      return isResPlace;
   }

   public void setResPlace(boolean isResPlace) {
      this.isResPlace = isResPlace;
   }



}
