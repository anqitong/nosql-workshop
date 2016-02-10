package nosql.workshop.services;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;
import nosql.workshop.connection.ESConnectionUtil;
import nosql.workshop.model.Installation;
import nosql.workshop.model.Town;
import nosql.workshop.model.suggest.TownSuggest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jongo.MongoCollection;

import com.google.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Search service permet d'encapsuler les appels vers ElasticSearch
 */
public class SearchService {

	
	public Double[] getLocation(String townName) throws IOException{
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryString(townName));
        Search search = (Search) new Search.Builder(searchSourceBuilder.toString())
                                        .addIndex("towns")
                                        .addType("location")
                                        .build();
        //System.out.println("hai");
			JestResult result = client.execute(search);
			//Double[] res = result
			List<Installation> res = result.getSourceAsObjectList(Installation.class);
		return null;
		
	}

	public List<TownSuggest> suggest(String text) throws IOException {
		JestClientFactory factory = new JestClientFactory();
		factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
				.multiThreaded(true)
				.build());
		JestClient client = factory.getObject();
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.queryString(text));
        Search search = (Search) new Search.Builder(searchSourceBuilder.toString())
                                        .addIndex("towns")
                                        .addType("location")
                                        .build();
        //System.out.println("hai");
			JestResult result = client.execute(search);
			//Double[] res = result
			List<Town> res = result.getSourceAsObjectList(Town.class);
			List<TownSuggest> r = new ArrayList<TownSuggest>();
			for(Town t:res){
				TownSuggest s = new TownSuggest();
				s.setTownName(t.getTownName());
				Double [] tab = {Double.parseDouble(t.getX().trim()), Double.parseDouble(t.getY().trim())};
				s.setLocation(tab);
			}
		return r;
	}

}
