/** filename:MongodbTemplate.java */
package maki.commons.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.QueryBuilder;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * <pre>
 * mongodb基本操作类
 * mongodb 时间操作是用默认标准时区，存入bson时不会保存本地时区，所以存时间时要转换
 *  加上当前服务器时区
        TimeZone tz = TimeZone.getTimeZone("Asia/Shenzhen");
        TimeZone.setDefault(tz);
 * </pre>
 * 
 */
public class MongodbTemplate {

    public static final int DEFAULT_LIMIT = 100;
    
    public static final int DEFAULT_COUNT_SKIP = 10000;
    
    public static final int DEFAULT_NUM = 300; // 地理位置查询默认返回条数

    private static final DBObject DB_OBJECT = new BasicDBObject();

    private MongoClient mongoClient;

    public MongodbTemplate() {
    }

    public MongodbTemplate(MongoClient mongoClient) {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectionsPerHost(200); // 客户端最大链接数
        builder.threadsAllowedToBlockForConnectionMultiplier(5); // 5* 200 允许最大线程请求链接
        // builder.socketKeepAlive(false);
        MongoClientOptions options = builder.build();
        mongoClient = new MongoClient(mongoClient.getAllAddress(), options);
        this.mongoClient = mongoClient;
    }

    /**
     * @param isSlaveOk true : secondary 节点可读
     */
    public MongodbTemplate(MongoClient mongoClient, boolean isSlaveOk) {
        MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
        builder.connectionsPerHost(200);
        builder.threadsAllowedToBlockForConnectionMultiplier(5);
        // builder.socketKeepAlive(false);
        MongoClientOptions options = builder.build();
        mongoClient = new MongoClient(mongoClient.getAllAddress(), options);
        this.mongoClient = mongoClient;
        if (isSlaveOk) {
            mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
        }
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public void setMongoClient(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    /**
     * 如果是多db，多collection 必需每次返回新的实例
     * 
     * @param dbname
     * @param collectionName
     * @return
     */
    private DBCollection getDBCollection(String dbname, String collectionName) {
        return mongoClient.getDB(dbname).getCollection(collectionName); // 如果不存在库表会自动创建
    }

    // **************************************************************修改相关************************************************************
    /**
     * <pre>
     * 查询并更新数据 更新map,如果是设置某个字段的值，必须要设置$set 
     * 如果让某个数字incr,可以 new BasicDBObject("$incr",new BasicDBOject(key,int))
     * 只能更新最近一条
     * </pre>
     * 
     * @param dbName mongodb 数据库空间
     * @param collectionName 表空间
     * @param qryMap 查询字段
     * @param updateMap 更新时如果更新的字段在原来记录里不存在，会新加这个字段
     * @param field 查询 返回 新/旧 结果集里的指定字段 1 显示 0 不显示
     * @param returnNew 是否要返回更新后的数据
     * @param upInsert 不存在是否插入一条，如果为true , 插入,字段为qry和update里的所有字段
     * @param remove 如果qry 和update 的字段存在，会删除这条记录，然后返回被删除的记录
     */
    // 验证完
    public DBObject findAndModify(String dbName, String collectionName, DBObject qry, DBObject update, DBObject field, DBObject sort, boolean remove, boolean returnNew, boolean upInsert) {
        if (checkEmpty(qry, update)) {
            return null;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return dbCollection.findAndModify(qry, field == null ? DB_OBJECT : field, sort == null ? DB_OBJECT : sort, remove, new BasicDBObject("$set", update), returnNew, upInsert);
    }
    
    /**
     * 查看更新或者插入 不存在则插入
     * @param dbName
     * @param collectionName
     * @param qry
     * @param update
     */
    public void saveOrUpdate(String dbName, String collectionName, DBObject qry, DBObject update){
    	if (checkEmpty(qry, update)) {
            return ;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.findAndModify(qry,  DB_OBJECT, DB_OBJECT, false, new BasicDBObject("$set", update), false, true);
    }
    
    /**
     * 查询qry条件数据 修改update 字段
     * @param dbName
     * @param collectionName
     * @param qry
     * @param update
     * @return
     */
    public DBObject findAndModify(String dbName, String collectionName, DBObject qry, DBObject update) {
        if (checkEmpty(qry, update)) {
            return null;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return dbCollection.findAndModify(qry, update);
    }
    
    // 验证完
    public void update(String dbName, String collectionName, DBObject qry, DBObject update) {
        if (checkEmpty(qry, update)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.update(qry, new BasicDBObject("$set", update), false, false);
    }

    // 验证完
    public void updateAndInc(String dbName, String collectionName, DBObject qry, DBObject update, DBObject inc) {
        if (checkEmpty(qry, update, inc)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        DBObject dbObject = new BasicDBObject();
        dbObject.put("$set", update);
        dbObject.put("$inc", inc);
        dbCollection.update(qry, dbObject, false, false);
    }

    // 验证完
    /**
     * @param dbName
     * @param collectionName
     * @param qry
     * @param inc 自增字段，必须要加 $inc符号
     */
    public void updateInc(String dbName, String collectionName, DBObject qry, DBObject inc) {
        if (checkEmpty(qry, inc)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.update(qry, new BasicDBObject("$inc", inc), false, false);
    }

    /**
     * @param dbName
     * @param collectionName
     * @param qry
     * @param update
     * @param upsert 如果 qry 不存在，就插入，插入的值是update里的值
     * @param multi false :更新 最先插入的一条 true: 更新所有符合记录的
     */
    // 验证完
    public void update(String dbName, String collectionName, DBObject qry, DBObject update, boolean upsert, boolean multi) {
        if (checkEmpty(qry, update)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.update(qry, new BasicDBObject("$set", update), upsert, multi);
    }

    /**
     * 不覆盖原来的，只做插入操作
     * 
     * @param dbName 数据库命名空间
     * @param collectionName 表空间
     * @param updateMap
     */
    // 验证
    public void insert(String dbName, String collectionName, DBObject insert) {
        if (checkEmpty(insert)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.insert(insert);
    }

    /**
     * <pre>
     * 批量插入
     * mongodb一次插入数据大小有限制,不能超过getMaxBsonObjectSize()的四倍，也就是60M
     * 如果list的size 大于一定值(不确定)，就要加WriteConcern.SAFE保证所有数据都被插入
     * 
     * 关于WriteConcern的定义
     * http://www.cnblogs.com/xinghebuluo/archive/2011/12/01/2270896.html
     * 
     *     WriteConcern.NONE:没有异常抛出
     *     WriteConcern.NORMAL:仅抛出网络错误异常，没有服务器错误异常
     *     WriteConcern.SAFE:抛出网络错误异常、服务器错误异常；并等待服务器完成写操作。
     *     WriteConcern.MAJORITY: 抛出网络错误异常、服务器错误异常；并等待一个主服务器完成写操作。
     *     WriteConcern.FSYNC_SAFE: 抛出网络错误异常、服务器错误异常；写操作等待服务器将数据刷新到磁盘。
     *     WriteConcern.JOURNAL_SAFE:抛出网络错误异常、服务器错误异常；写操作等待服务器提交到磁盘的日志文件。
     *     WriteConcern.REPLICAS_SAFE:抛出网络错误异常、服务器错误异常；等待至少2台服务器完成写操作。
     * 
     * </pre>
     * 
     * @param dbName
     * @param collectionName
     * @param updateMaps
     */
    // 验证
    public void batchInsert(String dbName, String collectionName, List<DBObject> inserts) {
        if (inserts == null || inserts.size() == 0) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.insert(inserts, WriteConcern.SAFE);
    }

    /**
     * 删掉所有符合记录的数据
     * 
     * @param dbName
     * @param collectionName
     * @param remove
     */
    public void remove(String dbName, String collectionName, DBObject remove) {
        if (checkEmpty(remove)) {
            return;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.remove(remove, WriteConcern.SAFE);
    }

    public void remove(String dbName, String collectionName) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.remove(new BasicDBObject());
    }

    // *******************************************************查询相关*********************************************************

    /**
     * 如果符合记录的有多条，只返回最先插入的一条
     * 
     * @param dbName
     * @param collectionName
     * @param qry
     * @return
     */
    public DBObject findOne(String dbName, String collectionName, DBObject qry) {
        if (checkEmpty(qry)) {
            return null;
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return dbCollection.findOne(qry);
    }

    /**
     * <pre>
     *  根据搜索条件查询,此处要注意排序
     *  qryMap 放入时如果是排除某个字段用
     *  new BasicDBObject("$ne", focususer.getUserId()) //$ne 表示不等于，还有其他符号，详细参见官方文档
     *  $gt 大于 $it 小于
     * mongodb是严格类型定义的，如果放进去是int,查询时就一定要用int
     * 如果文档添加字段，如果查询时使用了新添加字段，就要对整个集合的所有文档更新
     * mongodb 查出的int 类型数字会转换成 double ，需要注意
     * mongodb sort时有对记录数大小限制,所以一定要保证limit > 0,不能超过内存限制
     * field 字段  {fieldname:1} 表示这个字段要放进查询结果里 {fieldname:0} 表示这个字段不放进查询结果里
     * </pre>
     * 
     * @return
     */
    // 验证
    public List<DBObject> findList(String dbName, String collectionName, DBObject qry, DBObject sort, DBObject field, int skip, int limit) {
        DBCursor dbCursor = null;
        try {
            if (checkEmpty(qry)) {
                return null;
            }
            if (limit <= 0) {
                limit = DEFAULT_LIMIT;
            }
            DBCollection dbCollection = getDBCollection(dbName, collectionName);
            if (skip > 0) {
                dbCursor = dbCollection.find(qry, field == null ? DB_OBJECT : field).sort(sort == null ? DB_OBJECT : sort).skip(skip).limit(limit);
            } else {
                dbCursor = dbCollection.find(qry, field == null ? DB_OBJECT : field).sort(sort == null ? DB_OBJECT : sort).limit(limit);
            }
            return dbCursor.toArray();
        } finally {
            if (dbCursor != null) {
                dbCursor.close();
            }
        }
    }

    /**
     * <pre>
     * 类似mysql field like %A% 查询
     * mongodb模糊查询，在记录数很大时效率还是很低，譬如1200w数据，如果查询的字符串不存在，全表扫描的话差不多要17s
     * </pre>
     * 
     * @param likeMap : key 为 要模糊查询的字段 值为 模糊查询的字符串
     */
    public List<DBObject> like(String dbName, String collectionName, DBObject qry, DBObject sort, DBObject fields, Map<String, String> likeMap, int skip, int limit) {
        DBCursor dbCursor = null;
        try {
            if (likeMap == null || likeMap.size() == 0) {
                return null;
            }
            if (limit <= 0) {
                limit = DEFAULT_LIMIT;
            }
            DBCollection dbCollection = getDBCollection(dbName, collectionName);
            String key = "";
            String value = "";
            for (Entry<String, String> entry : likeMap.entrySet()) {
                key = entry.getKey();
                value = entry.getValue();
            }
            Pattern pattern = Pattern.compile(value);
            qry.put(key, pattern);
            if (skip > 0) {
                dbCursor = dbCollection.find(qry, fields == null ? DB_OBJECT : fields).sort(sort == null ? DB_OBJECT : sort).skip(skip).limit(limit);
            } else {
                dbCursor = dbCollection.find(qry, fields == null ? DB_OBJECT : fields).sort(sort == null ? DB_OBJECT : sort).limit(limit);
            }
            return dbCursor.toArray();
        } finally {
            if (dbCursor != null) {
                dbCursor.close();
            }
        }
    }

    /**
     * 根据查询条件查总记录数
     * 
     * @param dbName
     * @param collectionName
     * @param qryMap
     * @return
     */
    public int count(String dbName, String collectionName, DBObject qry) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return (int) dbCollection.count(qry == null ? DB_OBJECT : qry);
    }

    // ******************************************************索引相关*****************************************************************************
    /**
     * 删除索引
     * 
     * @param dbName
     * @param collectionName
     * @param idxName
     */
    public void dropIndex(String dbName, String collectionName, String idxName) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.dropIndex(idxName);
    }

    /**
     * 删除全部索引
     * 
     * @param dbName
     * @param collectionName
     * @param idxName
     */
    public void dropIndexAll(String dbName, String collectionName) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        dbCollection.dropIndexes();
    }

    /**
     * <pre>
     * 创建索引
     * 索引类似mysql,遵循最左索引原则
     * 要注意顺序，过滤字段查询记录数少的尽量放到最左边
     * 索引只创建一次
     * 三个字段 
     * a b c 如果对abc 建立复合索引
     * 最优查询只能是
     * a  ab abc三种
     * </pre>
     */
    public void ensureIndex(String dbName, String collectionName, String indexName, DBObject index, boolean isUnique) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        if (checkEmpty(index)) {
            return;
        }
        dbCollection.ensureIndex(index, indexName, isUnique);
    }

    /**
     * 后台创建索引
     * 
     * @param dbName
     * @param collectionName
     * @param nameSpace 索引命名空间
     * @param indexName 索引名称
     * @param index 索引字段
     * @param backGround true 后台 运行
     * @param isUnique 是否唯一索引
     */
    public void createIndexBackGround(String dbName, String collectionName, String nameSpace, String indexName, DBObject index, boolean backGround, boolean isUnique) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        if (checkEmpty(index)) {
            return;
        }
        DBObject options = new BasicDBObject();
        options.put("ns", nameSpace);
        options.put("name", indexName);
        options.put("background", backGround);
        options.put("unique", isUnique);
        dbCollection.createIndex(index, options);
    }

    /**
     * 聚合查询
     * 
     * @param dbName
     * @param collectionName
     * @param firstOp
     * @param additionalOps
     * @return
     */
    public List<BasicDBObject> aggregate(String dbName, String collectionName, DBObject firstOp, DBObject... additionalOps) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        AggregationOutput outPut = dbCollection.aggregate(firstOp, additionalOps);
        List<BasicDBObject> list = new ArrayList<BasicDBObject>();
        CommandResult result = outPut.getCommandResult();
        if (result.containsField("result")) {
            Object o1 = result.get("result");
            if (o1 != null && o1 instanceof BasicDBList) {
                BasicDBList list1 = (BasicDBList) o1;
                if (list1.size() <= 0) {
                    return list;
                }
                for (Object dbObject : list1.subList(0, list1.size())) {
                    if (dbObject instanceof DBObject) {
                        list.add((BasicDBObject) dbObject);
                    }
                }
                return list;
            }
        }
        return list;
    }

    /**
     * <pre>
     * 
     * TODO 如果用pipeline查询，是先完成$geoNear 函数，然后对结果 $limit $sort 这样就会有问题，
     * 因为$geoNear必须有一个num参数，所以首先 会限制查出结果数，如果additionalOps 加了 $sort会把查询出的结果
     * $sort重新排，导致结果不能按距离由近及远返回
     * 如果 GeoJSON point
     * db.collection_near_male.find({ gps : { $near :{ $geometry :{ type : "Point" ,coordinates: [ 121.431481, 31.1822952 ] } ,$maxDistance : 100 } }})
     * 创建索引时要用2dsphere，maxDistance 单位meters
     * 
     * 如果legacy coordinate pairs，要使用2d索引
     * db.collection_near_male.find({"gps":{$near:[121.0,31.0],$maxDistance:0.1}}) ，maxDistance角度值
     * 
     * 如果2dsphere,默认返回dis是 meters,maxDistance单位也是meters
     * db.runCommand( { geoNear : "collection_near_male" ,near : { type : "Point" ,coordinates: [121,31 ] } ,spherical : true, "maxDistance" :100000} ) 
     * 聚合查询
     * [{$geoNear: 
     *    {near: [40.724, -73.997],
     *    distanceField: "dist.calculated", 查询结果要展示距离的字段描述，
     *    includeLocs: "dist.location",
     *    e.g   "dist" : {
     *                                 "calculated" : 0.3891332360426221,
     *                                 "location" : [
     *                                         -130.232132,
     *                                         30.312313
     *                                 ]
     *                         }
     *               distanceField:"a" ,那就只显示a: 0.3891332360426221
     *    maxDistance: 默认是角度值
     *    distanceMultiplier：[miles or kilometers] 指定maxDistance 的单位,以radius of the Earth  e.g 公里 6378.137
     *    query: { type: "public" }, 过滤其他字段
     *    uniqueDocs: false, true: the document will only return once even if the document has multiple locations that meet the criteria.
     *    num: 5
     *    }}]
     *    [{$geoNear:},{$limit:1},{$skip:1}]，还可以这样加分页参数
     *    
     *    如果near 为 GeoJSON {type : "Point" ,coordinates: [121,31 ]} 这样的，maxDistance 默认值转换为 meters,如果需要转换成公里,distanceMultiplier ：0.001
     * </pre>
     * 
     * @param dbName
     * @param collectionName
     * @param qry
     */
    @Deprecated
    public List<BasicDBObject> near(String dbName, String collectionName, DBObject qry, double lng, double lat, String distanceField, double maxDistance, int skip, int limit, DBObject sort) {
        DBObject near = new BasicDBObject();
        near.put("type", "Point");
        near.put("coordinates", new double[] { lng, lat });

        // 拼接参数
        BasicDBObject tmp = new BasicDBObject("near", near).append("query", qry == null ? new BasicDBObject() : qry).append("spherical", true).append("distanceField", distanceField);
        if (maxDistance > 0) {
            tmp.append("maxDistance", maxDistance * 1000);
        }
        tmp.put("distanceMultiplier", 1); // 当查询是geogson时默认就是 1 米
        tmp.append("num", DEFAULT_NUM);

        DBObject dbObject = QueryBuilder.start("$geoNear").is(tmp).get();
        return aggregate(dbName, collectionName, dbObject);
        // DBObject skipDBObject = QueryBuilder.start("$skip").is(skip).get();
        // DBObject limitDBObject = QueryBuilder.start("$limit").is(limit).get();
        // if (sort != null && sort.toMap().size() > 0) {
        // DBObject sortDBObject = QueryBuilder.start("$sort").is(sort).get();
        // return aggregate(dbName, collectionName, dbObject, sortDBObject, skipDBObject, limitDBObject);
        // } else {
        // return aggregate(dbName, collectionName, dbObject, skipDBObject, limitDBObject);
        // }
    }

    /**
     * 返回符合条件的总条数
     * 
     * @param dbName
     * @param collectionName
     * @param qry
     * @param lng
     * @param lat
     * @param distanceField
     * @param maxDistance
     * @return
     */
    @Deprecated
    public int nearCount(String dbName, String collectionName, DBObject qry, double lng, double lat, String distanceField, double maxDistance) {
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        DBObject near = new BasicDBObject();
        near.put("type", "Point");
        near.put("coordinates", new double[] { lng, lat });

        // 拼接参数
        BasicDBObject tmp = new BasicDBObject("near", near).append("query", qry == null ? new BasicDBObject() : qry).append("spherical", true).append("distanceField", distanceField);
        if (maxDistance > 0) {
            tmp.append("maxDistance", maxDistance * 1000);
        }
        tmp.put("distanceMultiplier", 1);
        tmp.append("num", DEFAULT_NUM);
        DBObject dbObject = QueryBuilder.start("$geoNear").is(tmp).get();
        AggregationOutput outPut = dbCollection.aggregate(dbObject);
        CommandResult result = outPut.getCommandResult();
        if (result.containsField("result")) {
            Object o1 = result.get("result");
            if (o1 != null && o1 instanceof BasicDBList) {
                BasicDBList list = (BasicDBList) o1;
                return list.size();
            }
        }
        return 0;
    }

    /**http://www.2cto.com/database/201309/243885.html
     * 分页统计总数目 DBCursor cursor = collection.find(query).skip((currentPage-1) * PAGESIZE).sort(new BasicDBObject("starttime", -1)).limit(PAGESIZE);//PAGESIZE=10
     * 采取加分页状态 50000条内查询统计速度可以接受 模拟5000页数据 大于5000则记为5000 小于5000 则为实际
     * 
     * db.places.ensureIndex({'coordinate':'2d'})
 	 * db.places.ensureIndex({'coordinate':'2dsphere'})
 	 * db.coll_user.ensureIndex({'userId':-1})
 	 * db.coll_group.ensureIndex({'groupId':-1})
     * @param dbName
     * @param collectionName
     * @param qry
     * @param lng
     * @param lat
     * @param maxDistance
     * @param sort
     * @return
     */
    public int nearCount(String dbName, String collectionName, DBObject qry, double lng, double lat, double maxDistance, DBObject sort){
    	DBCursor dbCursor = null;
        try {
            BasicDBObject gps = new BasicDBObject();
            gps.append("type", "Point");
            gps.append("coordinates", new double[] { lng, lat });
            BasicDBObject geometry = new BasicDBObject();
            geometry.put("$geometry", gps);
            if (maxDistance > 0) {
            	geometry.put("$maxDistance", maxDistance);
            }
            DBObject near = QueryBuilder.start("$near").is(geometry).get();
            DBObject dbObject = QueryBuilder.start("coordinate").is(near).get();
            if (qry != null) {
                dbObject.putAll(qry);
            }
            DBCollection dbCollection = getDBCollection(dbName, collectionName);
            dbCursor = dbCollection.find(dbObject).sort(sort == null ? new BasicDBObject() : sort).skip(DEFAULT_COUNT_SKIP).limit(1);
            if(dbCursor.hasNext()){
            	return DEFAULT_COUNT_SKIP;
            }else{
            	return dbCursor.count();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbCursor != null) {
                dbCursor.close();
            }
        }
    	return 0;
    }
    
    /**
     * 
     * > db.places.ensureIndex({'coordinate':'2d'})
	 * > db.places.ensureIndex({'coordinate':'2dsphere'})
     * 通过find 命令查询，可以控制sort,limit,skip. 但是查询结果不返回距离,maxDistance单位要求是米 TODO 有bug
     */
    public List<BasicDBObject> near(String dbName, String collectionName, DBObject qry, double lng, double lat, double maxDistance, int skip, int limit, DBObject sort) {
        DBCursor dbCursor = null;
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
        try {
            BasicDBObject gps = new BasicDBObject();
            gps.append("type", "Point");
            gps.append("coordinates", new double[] { lng, lat });
            BasicDBObject geometry = new BasicDBObject();
            geometry.put("$geometry", gps);
            if (maxDistance > 0) {
            	geometry.put("$maxDistance", maxDistance);
            }
            DBObject near = QueryBuilder.start("$near").is(geometry).get();
            DBObject dbObject = QueryBuilder.start("coordinate").is(near).get();
            if (qry != null) {
                dbObject.putAll(qry);
            }
            DBCollection dbCollection = getDBCollection(dbName, collectionName);
            dbCursor = dbCollection.find(dbObject).sort(sort == null ? new BasicDBObject() : sort).skip(skip).limit(limit);
            List<DBObject> result = dbCursor.toArray();
            List<BasicDBObject> list = new ArrayList<BasicDBObject>();
            for (DBObject db : result) {
                list.add((BasicDBObject) db);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbCursor != null) {
                dbCursor.close();
            }
        }
        return null;
    }

    /**
     * <pre>
     * maxDistance单位要求是米  
     * TODO
     * 当数据超过一定时有bug ，靠近应用发现返回50个就报 
     * Tue Sep 24 11:14:16.067 JavaScript execution failed: count failed: { "ok" : 0, "errmsg" : "13111 wrong type for field () 10 != 2" } at
     * src/mongo/shell/query.js:L180
     * 这个错误是没记录时用 dbCursor.toArray() 转报错
     * 
     * $near 会报错和 $nearSphere 是按近到远 ，是有序的
     * $near是度 ，$nearSphere是弧度
     * </pre>
     * db.coll_user.find({coordinate:{$near:{$geometry:{type:"Point",coordinates:[113.947455,22.548958]},$maxDistance:1000000}}})
     * @param dbName
     * @param collectionName
     * @param qry
     * @param lng
     * @param lat
     * @param maxDistance
     * @return
     */
    public int nearCount(String dbName, String collectionName, DBObject qry, double lng, double lat, double maxDistance) {
        BasicDBObject gps = new BasicDBObject();
        gps.append("type", "Point");
        gps.append("coordinates", new double[] { lng, lat });
        BasicDBObject geometry = new BasicDBObject();
        geometry.put("$geometry", gps);
        if (maxDistance > 0) {
        	geometry.put("$maxDistance", maxDistance);
        }
        DBObject near = QueryBuilder.start("$near").is(geometry).get();
        DBObject dbObject = QueryBuilder.start("coordinate").is(near).get();
        if (qry != null) {
            dbObject.putAll(qry);
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return (int) dbCollection.count(dbObject);
    }

    // 弧度是公里数/6378 ，度是公里数/111，具体参考http://www.infoq.com/cn/articles/depth-study-of-Symfony2
    private static final double EARTH = 6378.137;

    /**
     * 通过find 命令查询，可以控制sort,limit,skip. 但是查询结果不返回距离,maxDistance单位要求是米 geoWithin 是包含关系，不会对结果排序
     */
    public List<BasicDBObject> nearCenter(String dbName, String collectionName, DBObject qry, double lng, double lat, double maxDistance, int skip, int limit, DBObject sort) {
        DBCursor dbCursor = null;
        if (limit <= 0) {
            limit = DEFAULT_LIMIT;
        }
        try {
            BasicDBObject gps = new BasicDBObject();
            List<Object> centerSphere = new ArrayList<Object>();
            centerSphere.add(new double[] { lng, lat });
            centerSphere.add(maxDistance / EARTH);
            gps.put("$centerSphere", centerSphere);

            DBObject near = QueryBuilder.start("$geoWithin").is(gps).get();
            DBObject dbObject = QueryBuilder.start("gps").is(near).get();
            if (qry != null) {
                dbObject.putAll(qry);
            }
            DBCollection dbCollection = getDBCollection(dbName, collectionName);
            dbCursor = dbCollection.find(dbObject).sort(sort == null ? new BasicDBObject() : sort).skip(skip).limit(limit);
            List<DBObject> result = dbCursor.toArray();
            List<BasicDBObject> list = new ArrayList<BasicDBObject>();
            for (DBObject db : result) {
                list.add((BasicDBObject) db);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (dbCursor != null) {
                dbCursor.close();
            }
        }
        return null;
    }

    /**
     * <pre>
     * maxDistance单位要求是米  
     * 当数据超过一定时有bug ，靠近应用发现返回50个就报 
     * Tue Sep 24 11:14:16.067 JavaScript execution failed: count failed: { "ok" : 0, "errmsg" : "13111 wrong type for field () 10 != 2" } at
     * src/mongo/shell/query.js:L180
     * </pre>
     * 
     * @param dbName
     * @param collectionName
     * @param qry
     * @param lng
     * @param lat
     * @param maxDistance
     * @return
     */
    public int nearCenterCount(String dbName, String collectionName, DBObject qry, double lng, double lat, double maxDistance) {
        BasicDBObject gps = new BasicDBObject();
        List<Object> centerSphere = new ArrayList<Object>();
        centerSphere.add(new double[] { lng, lat });
        centerSphere.add(maxDistance / EARTH);
        gps.put("$centerSphere", centerSphere);

        DBObject near = QueryBuilder.start("$geoWithin").is(gps).get();
        DBObject dbObject = QueryBuilder.start("gps").is(near).get();
        if (qry != null) {
            dbObject.putAll(qry);
        }
        DBCollection dbCollection = getDBCollection(dbName, collectionName);
        return (int) dbCollection.count(dbObject);
    }

    /**
     * 检查参数是否为null，或 map的size ==0
     * 
     * @param args
     * @return
     */
    private boolean checkEmpty(DBObject... args) {
        for (DBObject dbObject : args) {
            if (dbObject == null || dbObject.toMap().size() == 0) {
                return true;
            }
        }
        return false;
    }
}
