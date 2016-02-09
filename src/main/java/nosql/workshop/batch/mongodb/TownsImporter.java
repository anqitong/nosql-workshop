package nosql.workshop.batch.mongodb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import static org.elasticsearch.node.NodeBuilder.*;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

public class TownsImporter {
	
	
	public static void main(String[] args) {
		Node node = nodeBuilder().clusterName("sportDB").node();
		/*Client client = new TransportClient().addTransportAddress(
				new InetSocketTransportAddress("host", 9300));*/
		Client client = node.client();
		
		write(client);
		client.close();
		node.close();
				
	}
	
	public static void write(Client client){
		try (InputStream inputStream = TownsImporter.class.getResourceAsStream("/batch/csv/towns_paysdeloire.csv");
				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
			reader.lines()
			.skip(1)
			.filter(line -> line.length() > 0)
			.map(line -> line.split(","))
			.forEach(columns -> {
				String objectID = columns[0];
				String townName = columns[1];
				String townNameSuggest = columns[2];
				String postcode = columns[3];
				String pays = columns[4];
				String region = columns[5];
				double x = Double.parseDouble(columns[6].trim());
				double y = Double.parseDouble(columns[7].trim());
				
				XContentBuilder builder;
				try {
					builder = jsonBuilder()
							 .startObject()
						        .field("objectID", objectID)
						        .field("townName", townName)
						        .field("townNameSuggest", townNameSuggest)
						        .field("postcode", postcode)
						        .field("pays", pays)
						        .field("region", region)
						        .field("x", x)
						        .field("y", y)
						    .endObject();
					
					IndexResponse response = client.prepareIndex("towns","town")
						    .setSource(builder)
						    .execute()
						    .actionGet();
					System.out.println("Is created : "+response.isCreated());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				
			});
		}
		catch (IOException e) {
			throw new UncheckedIOException(e);
		}



	}

	
}
