package javaclz;

/**
 * Created by xiaoke on 17-5-16.
 */
public class JavaV {

    // used for para info
    public static final String LOG_PATH = "file/log4j.properties";
    public static final String VAR_FILE_PATH = "file/var.xml";

    // used for base
    public static final Mark<String> MASTER_HOST = Mark.makeMark("sjtumaster.master.host", "192.168.31.110");
    public static final Mark<Long> STREAMING_TIME_SEC_INTERVAL = Mark.makeMark("sjtumaster.streaming.time.interal", 10L);

    // used for kafka
    public static final Mark<Integer> KAFKA_PARALLELISM_NUM = Mark.makeMark("sjtumaster.kafka.parallelism.num", 2);
    public static final Mark<String> KAFKA_TOPIC = Mark.makeMark("sjtumaster.kafka.topic", "devicegate-topic");
    public static final Mark<String> KAFKA_SERVER_URL = Mark.makeMark("sjtumaster.kafka.zk.url", "192.168.31.110:9092");
    public static final Mark<String> KAFKA_ZK_URL = Mark.makeMark("sjtumaster.kafka.zk.url", "192.168.31.110:2181");
    public static final Mark<String> KAFKA_GROUP_ID = Mark.makeMark("sjtumaster.kafka.group.id", "devicegate-group-id");
    public static final Mark<String> KAFKA_ZK_SESSION_TIMEOUT = Mark.makeMark("sjtumaster.kafka.zk.session.timeout", "4000");
    public static final Mark<String> KAFKA_ZK_SYNC_TIME = Mark.makeMark("sjtumaster.kafka.zk.sync.time", "200");
    public static final Mark<String> KAFKA_REBALANCE_MAX_RETRIES = Mark.makeMark("sjtumaster.kafka.rebalance.max.retries", "5");
    public static final Mark<String> KAFKA_REBALANCE_BACKOFF = Mark.makeMark("sjtumaster.kafka.rebalance.backoff", "1200");
    public static final Mark<String> KAFKA_AUTO_COMMIT_INTERVAL = Mark.makeMark("sjtumaster.auto.commit.interval", "1000");
    public static final Mark<String> KAFKA_AUTO_OFFSET_RESET = Mark.makeMark("sjtumaster.auto.offset.reset", "largest");

    public static final Mark<String> SPARKSTREAMING_CLEANED_TOPIC = Mark.makeMark("sjtumaster.sparkstreaming.cleaned.topic", "cleaned-data-topic");
    public static final Mark<String> SPARKSTREAMING_AVG_TOPIC = Mark.makeMark("sjtumaster.sparkstreaming.cleaned.topic", "cleaned-avg-topic");

    public static final Mark<String> DEVICE_CONFIG_PATH = Mark.makeMark("sjtumaster.device.conf.deviceconfig.path");
    public static final Mark<String> DC_ZK_HOST = Mark.makeMark("sjtumaster.dc.zk.host", "192.168.31.110:2181");
    public static final Mark<String> DC_ZK_PORT = Mark.makeMark("sjtumaster.dc.zk.port");
    public static final Mark<String> DC_ZK_TIMEOUT = Mark.makeMark("sjtumaster.dc.zk.timout", "10000");
    public static final Mark<String> DC_ZK_APP_PATH = Mark.makeMark("sjtumaster.dc.zk.app.path", "/application");
    public static final Mark<String> DC_ZK_DEVICE_PATH = Mark.makeMark("sjtumaster.dc.zk.device.path", "/deviceconf");
    public static final Mark<String> DC_MYSQL_HOST = Mark.makeMark("sjtumaster.dc.mysql.host", "192.168.31.110");
    public static final Mark<String> DC_MYSQL_PORT = Mark.makeMark("sjtumaster.dc.mysql.port", "3306");
    public static final Mark<String> DC_MYSQL_DBNAME = Mark.makeMark("sjtumaster.dc.mysql.dbname", "device");
    public static final Mark<String> DC_MYSQL_USER = Mark.makeMark("sjtumaster.dc.mysql.user", "root");
    public static final Mark<String> DC_MYSQL_PASSWD = Mark.makeMark("sjtumaster.dc.mysql.passwd", "heheave");

    public static final Mark<String> HDFS_BASE_PATH = Mark.makeMark("sjtumaster.hdfs.base.path");
    public static final Mark<String> HDFS_FILE_APPENDED = Mark.makeMark("sjtumaster.hdfs.file.appended");


    public static final Mark<String> PERSIST_MONGODB_HOST = Mark.makeMark("sjtumaster.persist.mongo.host", "192.168.31.110");
    public static final Mark<String> PERSIST_MONGODB_PORT = Mark.makeMark("sjtumaster.persist.mongo.port", "27017");
    public static final Mark<String> PERSIST_MONGODB_DBNAME = Mark.makeMark("sjtumaster.persist.mongo.dbname", "device");
    public static final Mark<String> PERSIST_MONGODB_TIMEOUT = Mark.makeMark("sjtumaser.persist.mongo.timeout", "1000");
    public static final Mark<String> PERSIST_MONGODB_REALTIME_TBLNAME = Mark.makeMark("sjtumaster.persist.mongo.realtime.tblname", "realtime");
    public static final Mark<String> PERSIST_MONGODB_AVG_TBLNAME = Mark.makeMark("sjtumaster.persist.mongo.avg.tblname", "avgdata");
    public static final Mark<String> PERSIST_FILE_DATE_FORMAT = Mark.makeMark("sjtumaster.persist.file.date.format", "yyyy-MM-dd_HH");
    public static final Mark<String> PERSIST_FILE_REALTIME_BASEPATH = Mark.makeMark("sjtumaster.persist.file.realtime.basepath", "file:///tmp/rdd/realtime/");
    public static final Mark<String> PERSIST_FILE_AVG_BASEPATH = Mark.makeMark("sjtumaster.persist.file.avg.basepath", "file:///tmp/rdd/avg/");

    public static final Mark<String> RPC_BIND_NAME = Mark.makeMark("sjtumaster.rpc.bind.name", "RPC-JOB-SERVER");
    public static final Mark<Integer> RPC_BIND_PORT = Mark.makeMark("sjtumaster.rpc.bind.port", 8099);

    public static final Mark<String> SPARK_SQL_HDFS_BASEPATH = Mark.makeMark("sjtumaster.spark.sql.hdfs.basepath", "/tmp/rdd/realtime/");
    public static final Mark<String> SPARK_SQL_MONGO_BASEPATH = Mark.makeMark("sjtumaster.spark.sql.mongo.basepath", "mongodb://192.168.1.110:27017/device");
    public static final Mark<Integer> SPARK_SQL_MIN_PARTITION = Mark.makeMark("sjtumaster.spark.sql.min.partition", -1);
    public static final Mark<String> SPARK_SQL_TEMP_TBLNAME = Mark.makeMark("sjtumaster.spark.sql.temp.tblname", "deviceValue");
    public static final Mark<Integer> SPARK_SQL_LOCK_WAIT = Mark.makeMark("sjtumaster.spark.sql.lock.wait", 2000);
    public static final Mark<Integer> SPARK_SQL_TASK_TIME = Mark.makeMark("sjtumaster.spark.sql.task.time", 10000);

}
