package javaclz.persist.accessor;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import javaclz.persist.opt.PersistenceOpt;
import javaclz.persist.data.PersistenceData;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MongoAccessor implements PersistenceAccessor{
	
	private static final Logger log = Logger.getLogger(MongoAccessor.class);
	
	private MongoClient mc;
	
	public MongoAccessor(MongoClient mc) {
		this.mc = mc;
	}
	
	@Override
	public void persistenceOne(PersistenceOpt opt, PersistenceData data)  throws Exception{
		List<PersistenceData> dataList = new ArrayList<PersistenceData>(1);
		dataList.add(data);
		persistenceBatch(opt, dataList);
	}

	@Override
	public void persistenceBatch(PersistenceOpt opt, Collection<PersistenceData> data)  throws Exception{
		if (data.isEmpty()) {
			log.info("Batch empty return directly");
			return;
		}

		String dbName = opt.getStr1();
		String tblName = opt.getStr2();
		if (dbName == null || tblName == null) {
			log.warn("Unspecify database name and table name, data cannot be persisisted to mongodb but all droped");
			return;
		}
		MongoDatabase md = mc.getDatabase(dbName);
		log.info(dbName);
		MongoCollection<Document> collection = md.getCollection(tblName);
		log.info(tblName);
		List<Document> listDocuments = new ArrayList<Document>();
		for (PersistenceData pd: data) {
			Document d = getDocument(pd.toJson());
			listDocuments.add(d);
		}
		
		try {
			collection.insertMany(listDocuments);
		} finally {
			listDocuments.clear();
		}
	}
	
	private Document getDocument(JSONObject jo) {
		if (jo == null) {
			return null;
		}
//		Document result = new Document();
//		@SuppressWarnings("unchecked")
//		Set<Entry<String, Object>> entries = jo.entrySet();
//		for (Entry<String, Object> entry : entries) {
//			String key = entry.getKey();
//			Object value = entry.getValue();
//			if (value instanceof JSONObject) {
//				value = getDocument((JSONObject) value);
//			}
//			result.append(key, value);
//		}
		return Document.parse(jo.toString());
	}
	
	public void close() throws Exception{
		if (mc != null) {
			mc.close();
		}
		mc = null;
	}

}
