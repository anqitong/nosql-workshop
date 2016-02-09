package nosql.workshop.batch.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class MongoToElastic {
	public static void main(String[] args) {
		MongoDatabase db = null;
		try{   

			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );					
			db = mongoClient.getDatabase("sportDB");
			System.out.println("Connect to database successfully");
			//READ from MongoDB and Write to Elastic
		}catch(Exception e){
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		}
		readAndWrite();
	}
	
	public static void readAndWrite(){
		
	}
}
