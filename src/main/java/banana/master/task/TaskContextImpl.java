package banana.master.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import com.github.jknack.handlebars.Template;

import banana.core.BytesWritable;
import banana.core.ExpandHandlebars;
import banana.core.modle.ContextModle;
import banana.core.request.HttpRequest;
import banana.core.request.RequestBuilder;
import banana.master.BasicContext;
import banana.master.MasterServer;
import banana.core.request.PageRequest.PageEncoding;

/**
 *  TaskContext是注入时所有seed的上下文信息如果爬虫在抓取过程当中需要共享一些变量。那么可使用StartContext作为容器。
 *
 */
public final class TaskContextImpl extends BytesWritable implements ContextModle{
	
	private static final ExpandHandlebars handlebars = new ExpandHandlebars();
	
	static{
		handlebars.registerHelper("existKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				TaskContextImpl taskContext = (TaskContextImpl) options.context.model();
				return taskContext.existPath(path);
			}
		});
		handlebars.registerHelper("notEmpty", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				TaskContextImpl taskContext = (TaskContextImpl) options.context.model();
				return taskContext.existPath(path);
			}
		});
		handlebars.registerHelper("isEmpty", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				TaskContextImpl taskContext = (TaskContextImpl) options.context.model();
				return !taskContext.existPath(path);
			}
		});
	}
	
	private final BasicContext contextAttribute = new BasicContext();
	/**
	 * 定义根url
	 */
	private List<HttpRequest> seeds = new ArrayList<HttpRequest>();
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * 构造一个StartContext。通常用来充当seedRequest的容器
	 */
	public TaskContextImpl(){}
	
	/**
	 * 构造一个StartContext。并且加入一个种子URL
	 * @param url
	 * @param processorCls
	 */
	public TaskContextImpl(String url,String processor) {
		this(url, processor, null);
	}
	
	
	/**
	 * 构造一个StartContext。并且加入一个种子URL
	 * @param url 
	 * @param processorCls 
	 * @param pageEncoding  URL对应网页的编码
	 */
	public TaskContextImpl(String url,String processor,PageEncoding pageEncoding) {
		HttpRequest seed = RequestBuilder.custom().setUrl(url).setProcessor(processor).setPriority(0).setPageEncoding(pageEncoding).build();
		seeds.add(seed);
	}
	
	
	/**
	 * 注入种子
	 * @param request
	 */
	public void injectSeed(HttpRequest request){
		this.seeds.add(request);
	}
	
	/**
	 * 返回该StartContext所包含的所有种子URL
	 * @return
	 */
	public List<HttpRequest> getSeedRequests(){
		return this.seeds;
	}
	
	/**
	 * 返回该StartContext所包含的所有种子URL
	 * @return
	 */
	public List<HttpRequest> getSeedRequestsAndClear(){
		List<HttpRequest> s = this.seeds;
		this.seeds = new ArrayList<>();
		return s;
	}

	@Override
	public  Object get(Object attribute){
		Object value = contextAttribute.get(attribute);
		if (value == null){
			value = MasterServer.getInstance().getGlobalContext().get(attribute);
		}
		return value;
	}
	
	@Override
	public Object put(String attribute, Object value) {
		contextAttribute.put(attribute, value);
		return value;
	}
	
	/**
	 * 返回种子URL的个数
	 * @return
	 */
	public int getSeedSize(){
		return seeds.size();
	}
	
	@Override
	public boolean isEmpty(){
		return seeds.isEmpty() && contextAttribute.isEmpty();
	}
	

	@Override
	public void write(DataOutput out) throws IOException {
		String contextAttributeJson = JSON.toJSONString(contextAttribute);
		out.writeUTF(contextAttributeJson);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		String contextAttributeJson = in.readUTF();
		HashMap<String, Object> attribute = JSON.parseObject(contextAttributeJson, HashMap.class);
		contextAttribute.putAll(attribute);
	}

	@Override
	public int size() {
		return contextAttribute.size();
	}

	@Override
	public boolean containsKey(Object key) {
		return contextAttribute.containsKey(contextAttribute);
	}

	@Override
	public boolean containsValue(Object value) {
		return contextAttribute.containsValue(value);
	}


	@Override
	public Object remove(Object key) {
		return contextAttribute.remove(key);
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		contextAttribute.putAll(m);
	}

	@Override
	public void clear() {
		contextAttribute.clear();
	}

	@Override
	public Set<String> keySet() {
		return contextAttribute.keySet();
	}

	@Override
	public Collection<Object> values() {
		return contextAttribute.values();
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		return contextAttribute.entrySet();
	}

	@Override
	public Object parseObject(String line) throws IOException {
		if (line.startsWith("{{") && line.endsWith("}}") && !line.contains(" ")){
			return get(line.substring(2, line.length() -2));
		}
		return parseString(line);
	}

	@Override
	public String parseString(String line) throws IOException {
		if (!line.contains("{{")){
			return line;
		}
		Template template = handlebars.compileEscapeInline(line);
		return template.apply(this);
	}

	@Override
	public void copyTo(Map<String, Object> dst) {
		contextAttribute.copyTo(dst);
	}

	@Override
	public boolean existPath(String path) {
		if (contextAttribute.existPath(path)){
			return true;
		}
		return MasterServer.getInstance().getGlobalContext().existPath(path);
	}
	
}
