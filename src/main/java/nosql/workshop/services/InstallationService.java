package nosql.workshop.services;


import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jongo.MongoCollection;
import org.jongo.MongoCursor;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
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
    
    public List<Installation> search(String query) throws IOException {
    	JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
       
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryString(query));
        Search search = (Search) new Search.Builder(searchSourceBuilder.toString())
                                        .addIndex("installations")
                                        //.addType("menu")
                                        .build();
        //System.out.println("hai");
			JestResult result = client.execute(search);
			
			List<Installation> res = result.getSourceAsObjectList(Installation.class);
    	return res ;
    }
    
    public List<Installation> geosearch(String lat, String lng, String distance) throws IOException{
    	JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        JestClient client = factory.getObject();
       
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryString(lat))
        .query(QueryBuilders.queryString(lng))
        .query(QueryBuilders.queryString(distance));
        Search search = (Search) new Search.Builder(searchSourceBuilder.toString())
                                        .addIndex("installations")
                                        //.addType("menu")
                                        .build();
        //System.out.println("hai");
			JestResult result = client.execute(search);
			
			List<Installation> res = result.getSourceAsObjectList(Installation.class);
    	return res ;
    	
    }
    
    public InstallationsStats getInstallationsStat() {
    	/*InstallationsStats stats = new InstallationsStats();
    	//set total    	
    	stats.setTotalCount(installations.count());
    	//set total by activity
    	List<CountByActivity> list = (List<CountByActivity>) installations.aggregate(""
    			+ "["
    			+ "{ $unwind: {$equipements.activites} },"
    			+ " {$group: {_id : $equipements.activites, "
    			+ "total : { $sum : 1 } } }"
    			+ " ]").as(CountByActivity.class);
    	//System.out.println("count by activity = " + list.get(0).getTotalCount());

    	return stats;*/
    	
    	long total = installations.count();//nb d'installations
    	List<Installation> liste = new ArrayList();
        for (int i=0; i<installations.count();i++){
        	liste.add(installations.find().skip(i).as(Installation.class).next());
        }
        List<String> listeActivite= new ArrayList();
        int k=0;
        int equipementtotal=0;
        for (int i=0;i<liste.size();i++){
        	if(liste.get(i).getEquipements()!=null){
        	for(int j=0;j<liste.get(i).getEquipements().size();j++){
        		equipementtotal=equipementtotal+liste.get(i).getEquipements().size();
        		if(liste.get(i).getEquipements().size()>k){
        			k=i;}
        		if(liste.get(i).getEquipements().get(j).getActivites()!=null){
        			for(int h=0;h<liste.get(i).getEquipements().get(j).getActivites().size();h++){
            			listeActivite.add(liste.get(i).getEquipements().get(j).getActivites().get(h));
            		}
        		}
        		
        	}}
        }
        List<CountByActivity> listCount = new ArrayList();
        CountByActivity count;
        	while(listeActivite.size()>0){
        		int j=1;
        		for(int i=1;i<listeActivite.size();i++){
        			
        			if(listeActivite.get(i).matches(listeActivite.get(0))){
        				j++;
        				listeActivite.remove(i);
        			}
        		
        		}
        		count = new CountByActivity();
    			count.setActivite(listeActivite.get(0));
    			count.setTotal(j);
    			listeActivite.remove(0);
    			listCount.add(count);
        	}
        double average = (double) equipementtotal/total;
        InstallationsStats stats = new InstallationsStats();
        stats.setAverageEquipmentsPerInstallation(average);
        stats.setCountByActivity(listCount);
        stats.setInstallationWithMaxEquipments(liste.get(k));
        stats.setTotalCount(total);
        return stats;
    }
    
}
