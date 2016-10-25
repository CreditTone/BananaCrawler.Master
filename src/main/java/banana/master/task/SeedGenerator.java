package banana.master.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import banana.core.ExpandHandlebars;
import banana.core.request.HttpRequest;
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
		Map<String,Object> querys = (Map<String, Object>) seed_generator.find.get("ref");
		ref = new BasicDBObject(querys);
		if (seed_generator.find.containsKey("keys")){
			keys = new BasicDBObject((Map<String, Object>)seed_generator.find.get("keys"));
		}
		if (seed_generator.find.containsKey("limit")){
			limit = (int) seed_generator.find.get("limit");
		}
		
	}
	
	public List<HttpRequest> query() throws Exception{
		Thread.sleep(10 * 1000);
		List<HttpRequest> result = new ArrayList<HttpRequest>();
		if (!canQuery){
			return result;
		}
		DBCursor cursor = CrawlerMasterServer.getInstance().db.getCollection(collection).find(ref, keys).limit(limit);
		if (cursor.count() == 0){
			canQuery = false;
			return result;
		}
		ExpandHandlebars handlebar = new ExpandHandlebars();
		while(cursor.hasNext()){
			Map<String,Object> dbObject = cursor.next().toMap();
			HttpRequest request = null;
			String url = null;
			if (seed_generator.url != null){
				url = handlebar.escapeParse(seed_generator.url, dbObject);
				request = RequestBuilder.createPageRequest(url, seed_generator.processor);
			}else if(seed_generator.download != null){
				url = handlebar.escapeParse(seed_generator.download, dbObject);
				request = RequestBuilder.createBinaryRequest(url, seed_generator.processor);
			}
			dbObject.remove("_id");
			for(Entry<String,Object> entry : dbObject.entrySet()){
				request.addAttribute(entry.getKey(), entry.getValue());
			}
			request.setMethod(request.getMethod());
			if (seed_generator.headers != null){
				for (Entry<String,String> entry : seed_generator.headers.entrySet()) {
					String value = handlebar.escapeParse(entry.getValue(), dbObject);
					request.putHeader(entry.getKey(), value);
				}
			}
			if (seed_generator.params != null){
				for (Entry<String,String> entry : seed_generator.params.entrySet()) {
					String value = handlebar.escapeParse(entry.getValue(), dbObject);
					request.putParams(entry.getKey(), value);
				}
			}
			result.add(request);
		}
		if (!seed_generator.keep){
			canQuery = false;
		}
		return result;
	}
	
	public final boolean canQuery(){
		return canQuery;
	}
	
}
