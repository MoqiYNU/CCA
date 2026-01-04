package pipe.modules.reachability;

import java.util.List;

import pipe.dataLayer.DataLayer;
import pipe.gui.CreateGui;
import pipe.modules.Module;
import pipe.utils.DataLayerUtils;
import toolkits.def.petri.Composition;
import toolkits.def.petri.Marking;
import toolkits.def.petri.ProNet;
import toolkits.def.petri.RG;
import toolkits.utils.petri.PetriUtils;
import toolkits.utils.petri.RGUtils;

public class GenRRG implements Module {

	private static final String MODULE_NAME = "GenRRG";
	private PetriUtils petriUtils;
	private DataLayerUtils dataLayerUtils;
	private RG genRG;
	private Composition composition;
	private long timeElapsed;
	
	public GenRRG() {
		petriUtils = new PetriUtils();
		dataLayerUtils = new DataLayerUtils();
		composition = new Composition();
	}
   
	@Override
	public String getName() {
		return MODULE_NAME;
	}
   
   public void run(DataLayer pnmldata) {
	   
	   if((pnmldata == null) || !pnmldata.getPetriNetObjects().hasNext()){
			
			String legend = "No Petri net objects defined!";
			CreateGui.appGui.getStatusBar().changeText(legend);
			  
		}else {
			  
            List<ProNet> proNets = dataLayerUtils.genProNetsFromDL(pnmldata);
			
			long startTime = System.currentTimeMillis(); //产生RRG开始时间
			
			//组合过程网
			composition.setProNets(proNets);
			ProNet compNet = composition.compose();
			
			List<Marking> fMarings = compNet.getSinks();
			for (Marking fmarking : fMarings) {
				System.out.println("final markings: " + fmarking.getPlaces());
			}
			
			//原过程中交互数
			int inters = compNet.getMsgPlaces().size();
			
			System.out.println("Net Infor: " + "orgNum: " + proNets.size() + 
					           "; placeNum: " + pnmldata.getPlacesCount() + 
					           "; tranNum: " + pnmldata.getTransitionsCount() + 
					           "; inters: " + inters);
			
			try {
				  genRG = petriUtils.genRGWithStubSet(compNet);
			    } catch (Exception e) {
				    e.printStackTrace();
			    }
			
			System.out.println("\n");
			System.out.println("legalPaths: " + RGUtils.getLegalPaths(genRG).size() + "\n");
			
			 //产生RG结束时间
			 long endTime = System.currentTimeMillis();
			 //产生RG耗费时间
			 timeElapsed = endTime - startTime;
			 System.out.println("The number of states and trans in RG: " + 
				      genRG.getVertexs().size() + ", " + genRG.getEdges().size() + 
				      ", The time elapsed is: " + timeElapsed + " ms" + "\n");
			 RGUtils.genGraphFromRG(genRG, proNets.size(), pnmldata.getPlacesCount(), pnmldata.getTransitionsCount(), inters);
					 
		}
   }
   
}
