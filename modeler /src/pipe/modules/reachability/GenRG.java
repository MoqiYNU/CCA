package pipe.modules.reachability;

import java.util.List;

import pipe.dataLayer.DataLayer;
import pipe.gui.CreateGui;
import pipe.modules.Module;
import pipe.utils.DataLayerUtils;
import toolkits.def.lts.LTS;
import toolkits.def.petri.Composition;
import toolkits.def.petri.ProNet;
import toolkits.def.petri.RG;
import toolkits.utils.petri.PetriUtils;
import toolkits.utils.petri.RGUtils;

public class GenRG implements Module {

	private static final String MODULE_NAME = "GenRG";
	private PetriUtils petriUtils;
	private DataLayerUtils dataLayerUtils;
	private Composition composition;
	private RG genRG;
	
	public GenRG() {
		petriUtils = new PetriUtils();
		dataLayerUtils = new DataLayerUtils();
		composition = new Composition();
	}
   
	@Override
	public String getName() {
		return MODULE_NAME;
	}
   
   @SuppressWarnings("static-access")
   public void run(DataLayer pnmldata) {
	   
	   if((pnmldata == null) || !pnmldata.getPetriNetObjects().hasNext()){
			
			String legend = "No Petri net objects defined!";
			CreateGui.appGui.getStatusBar().changeText(legend);
			  
		}else {
			  
			List<ProNet> proNets = dataLayerUtils.genProNetsFromDL(pnmldata);//从pnml中获取过程网
			
			//组合过程网
			composition.setProNets(proNets);
			ProNet compNet = composition.compose();
			
			//组合网中交互数
			int inters = compNet.getMsgPlaces().size();
			System.out.println("Net Infor: " + "orgNum: " + proNets.size() + 
					           "; placeNum: " + compNet.getPlaces().size() + 
					           "; tranNum: " + compNet.getTrans().size() + 
					           "; inters: " + inters);
			
			try {
				  genRG = petriUtils.genRG(compNet);
				  LTS lts = RGUtils.rg2lts(genRG);
//				  GetDiameter getDiameter = new GetDiameter();
//				  System.out.println("Depth:" + getDiameter.getDepth(lts));
				  System.out.println("Num of states in RG: " + 
				                      genRG.getVertexs().size() + 
				                     ", Num of trans in RG: " + 
				                      genRG.getEdges().size() + 
				                      "\n");
			    } catch (Exception e) {
				    e.printStackTrace();
			    }
			
			
			
//			 RGUtils.genGraphFromRG(genRG, proNets.size(), pnmldata.getPlacesCount(), pnmldata.getTransitionsCount(), inters);
					 
		}
   }
   
}
