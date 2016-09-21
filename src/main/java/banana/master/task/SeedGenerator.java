package banana.master.task;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.math3.ode.ExpandableStatefulODE;

import com.github.jknack.handlebars.Template;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.util.JSON;

import banana.core.ExpandHandlebars;
import banana.core.request.PageRequest;
import banana.core.request.RequestBuilder;
import banana.master.impl.CrawlerMasterServer;

public class SeedGenerator {
	
	public banana.core.protocol.Task.SeedGenerator seed_generator;
	
	private String collection;
	
	private DBObject ref;
	
	private DBObject keys;
	
	private int limit = 10000;
	
	private boolean canQuery = true;
	
	public SeedGenerator(String collection,banana.core.protocol.Task.SeedGenerator seed_generator){
		this.collection = collection;
		this.seed_generator = seed_generator;
		HashMap<String,Object> querys = (HashMap<String, Object>) seed_generator.find.get("ref");
		ref = new BasicDBObject(querys);
		if (seed_generator.find.containsKey("keys")){
			keys = new BasicDBObject((HashMap<String, Object>)seed_generator.find.get("keys"));
		}
		if (seed_generator.find.containsKey("limit")){
			limit = (int) seed_generator.find.get("limit");
		}
		
	}
	
	public List<PageRequest> query() throws IOException{
		List<PageRequest> result = new ArrayList<PageRequest>();
		if (!canQuery){
			return result;
		}
		DBCursor cursor = CrawlerMasterServer.getInstance().db.getCollection(collection).find(ref, keys).limit(limit);
		if (cursor.count() == 0){
			canQuery = false;
			return result;
		}
		ExpandHandlebars handlebar = new ExpandHandlebars();
		Template template = null;
		if (seed_generator.url.contains("{{")){
			template = handlebar.compileInline(seed_generator.url);
		}
		while(cursor.hasNext()){
			Map<String,Object> dbObject = cursor.next().toMap();
			String url = template != null? template.apply(dbObject):seed_generator.url;
			PageRequest request = RequestBuilder.createPageRequest(url, seed_generator.processor);
			dbObject.remove("_id");
			for(Entry<String,Object> entry : dbObject.entrySet()){
				request.addAttribute(entry.getKey(), entry.getValue());
			}
			result.add(request);
		}
		if (!seed_generator.keep){
			canQuery = false;
		}
		return result;
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException {
		String mongoAddress = "localhost,27017,crawler,crawler,crawler";
		String[] split = mongoAddress.split(",");
		MongoClient client = null;
		ServerAddress serverAddress = new ServerAddress(split[0], Integer.parseInt(split[1]));
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();
		seeds.add(serverAddress);
		String userName = split[3];
		String dataBase = split[2];
		String password = split[4];
		MongoCredential credentials = MongoCredential.createCredential(userName, dataBase,
				password.toCharArray());
		client = new MongoClient(seeds, Arrays.asList(credentials));
		DB db = client.getDB(split[2]);
		BasicDBObject query = (BasicDBObject) JSON.parse("{isQiye:\"1\"}");
		//query.append(key, val)
		DBCursor cursor = db.getCollection("taobaoshoplist").find(query,(DBObject) JSON.parse("{userid:1}")).limit(2);
		while(cursor.hasNext()){
			System.out.println(cursor.next());
		}
	}
	
}
