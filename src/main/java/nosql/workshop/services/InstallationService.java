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
import nosql.workshop.model.Town;
import nosql.workshop.model.stats.CountByActivity;
import nosql.workshop.model.stats.InstallationsStats;

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
    
    public List<Installation> search() {
    	
    	return null;
    }
    
    public InstallationsStats getInstallationsStat() {
    	InstallationsStats stats = new InstallationsStats();
    	//set total    	
    	stats.setTotalCount(installations.count());
    	//set total by activity
    	List<CountByActivity> list = (List<CountByActivity>) installations.aggregate(""
    			+ "["
    			+ "{ $unwind: {$equipements.activites} },"
    			+ " {$group: {_id : $equipements.activites, total : { $sum : 1 } } }"
    			+ " ]").as(CountByActivity.class);
    	//System.out.println("count by activity = " + list.get(0).getTotalCount());

    	return stats;
    }
    
}
