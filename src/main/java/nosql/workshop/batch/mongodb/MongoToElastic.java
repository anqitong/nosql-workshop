package nosql.workshop.batch.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import org.jongo.MongoCollection;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Town;
import nosql.workshop.services.InstallationService;
import nosql.workshop.services.MongoDB;

public class MongoToElastic {
	
	
	
	public static void main(String[] args) {
		MongoDB mongoDB = new MongoDB();
		try {
			InstallationService installationService = new InstallationService(mongoDB);
			List<Installation> installations = installationService.getAllInstallations();

			
			System.out.println(installations.get(0));
			System.out.println("id = " + installations.get(0).get_id());
			System.out.println("installation name = " + installations.get(0).getNom());
			//System.out.println("equip = " + installations.get(0).getEquipements().get(0).);

			JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
	                .multiThreaded(true)
	                .build());
	        JestClient client = factory.getObject();

			
			readAndWrite(installations, client);
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static void readAndWrite(List<Installation> list, JestClient client){
		
		for(Installation ins:list){
			Installation i = new Installation();
			i.setNom(ins.getNom());
			System.out.println(ins.getNom());
			i.set_id(ins.get_id());
			System.out.println(ins.get_id());
			i.setAdresse(ins.getAdresse());
			ins.getAdresse().getCommune();
			i.setLocation(ins.getLocation());
			System.out.println(ins.getLocation().getCoordinates());
			i.setMultiCommune(ins.isMultiCommune());
			System.out.println(ins.isMultiCommune());
			i.setNbPlacesParking(ins.getNbPlacesParking());
			i.setNbPlacesParkingHandicapes(ins.getNbPlacesParkingHandicapes());
			
			Index index = new Index.Builder(i).index("installations").type("installation").id(ins.get_id()).build();
			/*try {
				client.execute(index);
				System.out.println("ID: "+i.get_id());
			} catch (IOException e) {
				e.printStackTrace();
			}*/
		}
		
	}
}
