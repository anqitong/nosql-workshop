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
		try{   
			
	         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	         DB db = mongoClient.getDB( "sportDB" );
	         System.out.println("Connect to database successfully");
	        // boolean auth = db.authenticate(myUserName, myPassword);
	         //System.out.println("Authentication: "+auth);
				
	         DBCollection coll = db.createCollection("mycol");
	         System.out.println("Collection created successfully");
	      }catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
        
    }
    
    //insert into MongoDB(DBobject)
    public void readInstallations(){
    	try (InputStream inputStream = CsvToMongoDb.class.getResourceAsStream("/batch/csv/installations.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
               reader.lines()
                       .skip(1)
                       .filter(line -> line.length() > 0)
                       .map(line -> line.split(","))
                       .forEach(columns -> {
                           System.out.println("Une ligne");
                           System.out.println(columns[0].matches("\".*\"")?columns[0].substring(1,columns[0].length()-1):columns[0]);
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
