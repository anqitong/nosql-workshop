package nosql.workshop.services;


import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import nosql.workshop.model.Equipement;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Installation.Adresse;
import nosql.workshop.model.Installation.Location;

/**
 * Service permettant de manipuler les installations sportives.
 */
@Singleton
public class InstallationService {
    /**
     * Nom de la collection MongoDB.
     */
    public static final String COLLECTION_NAME = "installations";

    private final MongoCollection installations;

    @Inject
    public InstallationService(MongoDB mongoDB) throws UnknownHostException {
        this.installations = mongoDB.getJongo().getCollection(COLLECTION_NAME);
    }

    public Installation random() {
        // FIXME : bien sûr ce code n'est pas le bon ... peut être quelque chose comme installations.findOne()
        Installation installation = new Installation();
        installation.setNom("Mon Installation");
        installation.setEquipements(Arrays.asList(new Equipement()));
        installation.setAdresse(new Adresse());
        Location location = new Location();
        location.setCoordinates(new double[]{3.4, 3.2});
        installation.setLocation(location);
        return installation;
    }
    
    public Installation getInstallationByNumero(String numero) {    	
    	//System.out.println("check find: " + String.format("\"{\"_id\":\"%s\"}\"", numero));
    	//Installation inst = installations.findOne(String.format("\"{\"_id\":\"%s\"}\"", numero)).as(Installation.class);
    	//System.out.println("check find: " + "{'_id':'440030016'}");
    	//Installation inst = installations.findOne("{'_id':'440030016'}").as(Installation.class);
    	Installation inst = installations.findOne(String.format("{'_id':'%s'}", numero)).as(Installation.class);
    	if(inst == null) {
    		System.out.println("toto");
    	}
    	return inst;    	
    }
    
    public List<Installation> getAllInstallations() {
    	MongoCursor<Installation> installs = installations.find().as(Installation.class);
    	ArrayList<Installation> installationsList = new ArrayList<Installation>();
    	for(Installation inst : installs) {
    		installationsList.add(inst);
    	}
    	return installationsList;
    }
}
