package nosql.workshop.batch.mongodb;

import java.net.UnknownHostException;
import java.util.List;

import org.jongo.MongoCollection;

import nosql.workshop.model.Installation;
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
			
			
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static void readAndWrite(){
		
	}
}
