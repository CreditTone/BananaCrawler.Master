package banana.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.github.jknack.handlebars.Template;

import banana.core.ExpandHandlebars;
import banana.core.modle.ContextModle;

public class AutoOverdueContext implements ContextModle {
	
	private static final ExpandHandlebars handlebars = new ExpandHandlebars();
	
	static{
	}
	
	private LinkedList<ContextModle> allContextModle ;
	
	private int capacity ;
	
	private int passTime ;
	
	private Timer timer;
	

	public AutoOverdueContext(int capacity, int passTime) {
		this.capacity = capacity;
		this.passTime = passTime;
		init(capacity, passTime);
	}



	private void init(int capacity, int passTime) {
		this.allContextModle = new LinkedList<>();
		this.allContextModle.add(new BasicContext());
		if (this.timer != null){
			this.timer.cancel();
		}
		this.timer = new Timer(true);
		this.timer.schedule(new TimerTask() {
			
			@Override
			public void run() {
				synchronized(AutoOverdueContext.this){
					if (allContextModle.size() >= capacity){
						allContextModle.removeFirst();
					}
					allContextModle.add(new BasicContext());
				}
			}
		}, passTime, passTime);
	}
	
	

	@Override
	public int size() {
		int size = 0;
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			size += iter.next().size();
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			if (!iter.next().isEmpty()){
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean containsKey(Object key) {
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			if (iter.next().containsKey(key)){
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			if (iter.next().containsValue(value)){
				return true;
			}
		}
		return false;
	}

	@Override
	public synchronized Object get(Object key) {
		Iterator<ContextModle> iter = allContextModle.iterator();
		Object value = null;
		while(iter.hasNext()){
			value = iter.next().get(key);
			if (value != null){
				return value;
			}
		}
		return null;
	}

	@Override
	public Object put(String key, Object value) {
		return allContextModle.getLast().put(key, value);
	}

	@Override
	public Object remove(Object key) {
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			iter.next().remove(key);
		}
		return null;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		allContextModle.getLast().putAll(m);
	}

	@Override
	public void clear() {
		init(capacity, passTime);
	}

	@Override
	public Set<String> keySet() {
		HashSet<String> keys = new HashSet<String>();
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			keys.addAll(iter.next().keySet());
		}
		return keys;
	}

	@Override
	public Collection<Object> values() {
		List<Object> values = new ArrayList<>();
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			values.addAll(iter.next().values());
		}
		return values;
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		HashSet<java.util.Map.Entry<String, Object>> entrySet = new HashSet<java.util.Map.Entry<String, Object>>();
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			entrySet.addAll(iter.next().entrySet());
		}
		return entrySet;
	}

	@Override
	public Object parseObject(String line) throws IOException {
		if (line.startsWith("{{") && line.endsWith("}}")){
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
		Iterator<ContextModle> iter = allContextModle.iterator();
		while(iter.hasNext()){
			iter.next().copyTo(dst);
		}
	}
	
}
