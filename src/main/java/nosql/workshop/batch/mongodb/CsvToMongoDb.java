package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * Created by chriswoodrow on 09/02/2016.
 */
public class CsvToMongoDb {
    
	public static void main(String[] args) {
		DB db = null;
		try{   
			
	         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	         db = mongoClient.getDB( "sportDB" );
	         System.out.println("Connect to database successfully");
	        // boolean auth = db.authenticate(myUserName, myPassword);
	         //System.out.println("Authentication: "+auth);
				
	         //DBCollection coll = db.createCollection("mycol");
	         //System.out.println("Collection created successfully");
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
        readInstallations(db);
    }
    
    //insert into MongoDB(DBobject)
    public static void readInstallations(DB db){
    	DBCollection coll = db.getCollection("installations");
    	try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split("\",\""))
                       .forEach(columns -> {
                           System.out.println("Une ligne");
                           
                           //String nom = columns[0].matches("\".*\"")?columns[0].substring(2,columns[0].length()-1):columns[0];
                           
                           //String nom = columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0];
                           //System.out.println(nom);
                           String nom = columns[0].substring(1, columns[0].length());
                           System.out.println("nom = " + nom);
                           String numeroInstall = columns[1].matches("\".*\"")?columns[1].substring(1,columns[1].length()-1):columns[1];                           
                           System.out.println("numeroInstall = " + numeroInstall);
                           String nomCommune = columns[2].matches("\".*\"")?columns[2].substring(1,columns[2].length()-1):columns[2];
                           String codeInsee = columns[3].matches("\".*\"")?columns[3].substring(1,columns[3].length()-1):columns[3];
                           String codePostal = columns[4].matches("\".*\"")?columns[4].substring(1,columns[4].length()-1):columns[4];
                           String lieuDit = columns[5].matches("\".*\"")?columns[5].substring(1,columns[5].length()-1):columns[5];
                           String numeroVoie = columns[6].matches("\".*\"")?columns[6].substring(1,columns[6].length()-1):columns[6];
                           String nomVoie = columns[7].matches("\".*\"")?columns[7].substring(1,columns[7].length()-1):columns[7];
                           String location = columns[8].matches("\".*\"")?columns[8].substring(1,columns[8].length()-1):columns[8];
                           String longitude = columns[9].matches("\".*\"")?columns[9].substring(1,columns[9].length()-1):columns[9];
                           String latitude = columns[10].matches("\".*\"")?columns[10].substring(1,columns[10].length()-1):columns[10];
                           String accessibilite = columns[11].matches("\".*\"")?columns[11].substring(1,columns[11].length()-1):columns[11];
                           String mobiliteReduite = columns[12].matches("\".*\"")?columns[12].substring(1,columns[12].length()-1):columns[12];
                           String handicapesSensoriels = columns[13].matches("\".*\"")?columns[13].substring(1,columns[13].length()-1):columns[13];
                           String empriseFonciere = columns[14].matches("\".*\"")?columns[14].substring(1,columns[14].length()-1):columns[14];
                           String logementGardien = columns[15].matches("\".*\"")?columns[15].substring(1,columns[15].length()-1):columns[15];
                           String multiCommune = columns[16].matches("\".*\"")?columns[16].substring(1,columns[16].length()-1):columns[16];
                           String placeParking = columns[17].matches("\".*\"")?columns[17].substring(1,columns[17].length()-1):columns[17];
                           String parkingHandicapes = columns[18].matches("\".*\"")?columns[18].substring(1,columns[18].length()-1):columns[18];
                           String installPart = columns[19].matches("\".*\"")?columns[19].substring(1,columns[19].length()-1):columns[19];
                           String metro = columns[20].matches("\".*\"")?columns[20].substring(1,columns[20].length()-1):columns[20];
                           String bus = columns[21].matches("\".*\"")?columns[21].substring(1,columns[21].length()-1):columns[21];
                           String tram = columns[22].matches("\".*\"")?columns[22].substring(1,columns[22].length()-1):columns[22];
                           String train = columns[23].matches("\".*\"")?columns[23].substring(1,columns[23].length()-1):columns[23];
                           String bateau = columns[24].matches("\".*\"")?columns[24].substring(1,columns[24].length()-1):columns[24];
                           String autre = columns[25].matches("\".*\"")?columns[25].substring(1,columns[25].length()-1):columns[25];
                           String nbEquip = columns[26].matches("\".*\"")?columns[26].substring(1,columns[26].length()-1):columns[26];
                           String nbFichesEquip = columns[27].matches("\".*\"")?columns[27].substring(1,columns[27].length()-1):columns[27];
                           String dateMaj = columns[28].matches("\".*\"")?columns[28].substring(1,columns[28].length()-1):columns[28];
                           
                           coll.insert( new BasicDBObject("_id", numeroInstall).
                                   append("nom", nom).
                                   append("address", new Document().
                                		   append("nomCommune", nomCommune).
                                		   append("numeroVoie", numeroVoie).
                                		   append("nomVoie", nomVoie).
                                		   append("lieuDit", lieuDit).
                                		   append("codePostal", codePostal)
                                   ).
                                   append("location", new Document()
                                		   .append("latitude", latitude).
                                		   append("longitude", longitude)
                                   ).
                                   append("multiCommune", multiCommune).
                                   append("placeParking", placeParking).
                                   append("parkingHandicapes", parkingHandicapes));                     
                           
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
