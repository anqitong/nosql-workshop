package nosql.workshop.batch.mongodb;

import java.io.*;
import java.util.Arrays;
import java.util.stream.Collectors;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
    
	public static void main(String[] args) {
		/*try{   
			
	         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	         DB db = mongoClient.getDB( "sportDB" );
	         System.out.println("Connect to database successfully");
	        // boolean auth = db.authenticate(myUserName, myPassword);
	         //System.out.println("Authentication: "+auth);
				
	         //DBCollection coll = db.createCollection("mycol");
	         //System.out.println("Collection created successfully");
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }*/
        readInstallations();
    }
    
    //insert into MongoDB(DBobject)
    public static void readInstallations(){
    	//DBCollection coll = db.createCollection("mycol");
    	try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split(","))
                       .forEach(columns -> {
                           System.out.println("Une ligne");
                           System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
                           String nom = columns[1].matches("\".*\"")?columns[1].substring(1,columns[1].length()-1):columns[1];
                           String numeroInstall = columns[1].matches("\".*\"")?columns[1].substring(1,columns[1].length()-1):columns[1];
                           String nomCommune = columns[2].matches("\".*\"")?columns[2].substring(1,columns[2].length()-1):columns[2];
                           String codeInsee = columns[3].matches("\".*\"")?columns[3].substring(1,columns[3].length()-1):columns[3];
                           String codePostal;
                           String lieuDit;
                           String numeroVoie;
                           String nomVoie;
                           String location;
                           String longitude;
                           String latitude;
                           String accessibilite;
                           String mobiliteReduite;
                           String handicapesSensoriels;
                           String empriseFonciere;
                           String logementGardien;
                           String multiCommune;
                           String placeParking;
                           String parkingHandicapes;
                           String installPart;
                           String metro;
                           String bus;
                           String tram;
                           String train;
                           String bateau;
                           String autre;
                           String nbEquip;
                           String nbFichesEquip;
                           String dateMaj;
                           
                           
                       });
           } catch (IOException e) {
               throw new UncheckedIOException(e);
           }
    }
    
    //update mongo
    public void readEquip(){
    	
    }
    //update mongo
    public void readAct(){
    	
    }
}
