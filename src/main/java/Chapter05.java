import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.csv.CSVParser;
import org.javatuples.Pair;
import redis.clients.jedis.*;

import java.io.File;
import java.io.FileReader;
import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.*;

public class Chapter05 {
    public static final String DEBUG = "debug";
    public static final String INFO = "info";
    public static final String WARNING = "warning";
    public static final String ERROR = "error";
    public static final String CRITICAL = "critical";

    public static final Collator COLLATOR = Collator.getInstance();

    public static final SimpleDateFormat TIMESTAMP =
            new SimpleDateFormat("EEE MMM dd HH:00:00 yyyy");
    private static final SimpleDateFormat ISO_FORMAT =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00");
    static{
        ISO_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static final void main(String[] args)
            throws InterruptedException
    {
        new Chapter05().run();
    }

    public void run()
            throws InterruptedException
    {
        Jedis conn = new Jedis("localhost");
        conn.select(15);
        //测试logRecent功能
        testLogRecent(conn);
        //保存当前小时和上个小时常见日志，以及最近99条日志信息
        testLogCommon(conn);
        //记录时间点的计数器，按照精度记录
        testCounters(conn);
        testStats(conn);
        testAccessTime(conn);
        testIpLookup(conn);
        testIsUnderMaintenance(conn);
        testConfig(conn);
    }

    /**
     * 记录5条数据，然后打印结果
     * 存储结构为数组，key为：recent:名称:日志级别
     */
    public void testLogRecent(Jedis conn) {
        System.out.println("\n----- testLogRecent -----");
        System.out.println("Let's write a few logs to the recent log");
        for (int i = 0; i < 5; i++) {
            logRecent(conn, "test", "this is message " + i);
        }
        List<String> recent = conn.lrange("recent:test:info", 0, -1);
        System.out.println(
                "The current recent message log has this many messages: " +
                        recent.size());
        System.out.println("Those messages include:");
        for (String message : recent){
            System.out.println(message);
        }
        assert recent.size() >= 5;
    }

    /**
     * 写入日志，并打印结果
     */
    public void testLogCommon(Jedis conn) {
        System.out.println("\n----- testLogCommon -----");
        System.out.println("Let's write some items to the common log");
        for (int count = 1; count < 6; count++) {
            for (int i = 0; i < count; i ++) {
                logCommon(conn, "test", "message-" + count);
            }
        }
        //common:test:info存放的是当前小时出现的日志，按出现的次数排序
        Set<Tuple> common = conn.zrevrangeWithScores("common:test:info", 0, -1);
        System.out.println("The current number of common messages is: " + common.size());
        System.out.println("Those common messages are:");
        for (Tuple tuple : common){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert common.size() >= 5;
    }

    public void testCounters(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testCounters -----");
        System.out.println("Let's update some counters for now and a little in the future");
        long now = System.currentTimeMillis() / 1000;
        //循环模拟时间流逝
        for (int i = 0; i < 10; i++) {
            //随机生成数据
            int count = (int)(Math.random() * 5) + 1;
            updateCounter(conn, "test", count, now + i);
        }
        //获取指定精度的计数器并打印时间以及计数值
        List<Pair<Integer,Integer>> counter = getCounter(conn, "test", 1);
        System.out.println("We have some per-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer,Integer> count : counter){
            System.out.println("  " + count);
        }
        assert counter.size() >= 10;

        counter = getCounter(conn, "test", 5);
        System.out.println("We have some per-5-second counters: " + counter.size());
        System.out.println("These counters include:");
        for (Pair<Integer,Integer> count : counter){
            System.out.println("  " + count);
        }
        assert counter.size() >= 2;
        System.out.println();

        System.out.println("Let's clean out some counters by setting our sample count to 0");
        CleanCountersThread thread = new CleanCountersThread(0, 2 * 86400000);
        thread.start();
        Thread.sleep(1000);
        thread.quit();
        thread.interrupt();
        counter = getCounter(conn, "test", 86400);
        System.out.println("Did we clean out all of the counters? " + (counter.size() == 0));
        assert counter.size() == 0;
    }

    public void testStats(Jedis conn) {
        System.out.println("\n----- testStats -----");
        System.out.println("Let's add some data for our statistics!");
        List<Object> r = null;
        for (int i = 0; i < 5; i++){
            double value = (Math.random() * 11) + 5;
            r = updateStats(conn, "temp", "example", value);
        }
        System.out.println("We have some aggregate statistics: " + r);
        Map<String,Double> stats = getStats(conn, "temp", "example");
        System.out.println("Which we can also fetch manually:");
        System.out.println(stats);
        assert stats.get("count") >= 5;
    }

    public void testAccessTime(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testAccessTime -----");
        System.out.println("Let's calculate some access times...");
        AccessTimer timer = new AccessTimer(conn);
        for (int i = 0; i < 10; i++){
            timer.start();
            Thread.sleep((int)((.5 + Math.random()) * 1000));
            timer.stop("req-" + i);
        }
        System.out.println("The slowest access times are:");
        Set<Tuple> atimes = conn.zrevrangeWithScores("slowest:AccessTime", 0, -1);
        for (Tuple tuple : atimes){
            System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
        }
        assert atimes.size() >= 10;
        System.out.println();
    }

    public void testIpLookup(Jedis conn) {
        System.out.println("\n----- testIpLookup -----");
        String cwd = System.getProperty("user.dir");
        File blocks = new File(cwd + "/GeoLiteCity-Blocks.csv");
        File locations = new File(cwd + "/GeoLiteCity-Location.csv");
        if (!blocks.exists()){
            System.out.println("********");
            System.out.println("GeoLiteCity-Blocks.csv not found at: " + blocks);
            System.out.println("********");
            return;
        }
        if (!locations.exists()){
            System.out.println("********");
            System.out.println("GeoLiteCity-Location.csv not found at: " + locations);
            System.out.println("********");
            return;
        }

        System.out.println("Importing IP addresses to Redis... (this may take a while)");
        importIpsToRedis(conn, blocks);
        long ranges = conn.zcard("ip2cityid:");
        System.out.println("Loaded ranges into Redis: " + ranges);
        assert ranges > 1000;
        System.out.println();

        System.out.println("Importing Location lookups to Redis... (this may take a while)");
        importCitiesToRedis(conn, locations);
        long cities = conn.hlen("cityid2city:");
        System.out.println("Loaded city lookups into Redis:" + cities);
        assert cities > 1000;
        System.out.println();

        System.out.println("Let's lookup some locations!");
        for (int i = 0; i < 5; i++){
            String ip =
                    randomOctet(255) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256) + '.' +
                            randomOctet(256);
            System.out.println(Arrays.toString(findCityByIp(conn, ip)));
        }
    }

    public void testIsUnderMaintenance(Jedis conn)
            throws InterruptedException
    {
        System.out.println("\n----- testIsUnderMaintenance -----");
        System.out.println("Are we under maintenance (we shouldn't be)? " + isUnderMaintenance(conn));
        conn.set("is-under-maintenance", "yes");
        System.out.println("We cached this, so it should be the same: " + isUnderMaintenance(conn));
        Thread.sleep(1000);
        System.out.println("But after a sleep, it should change: " + isUnderMaintenance(conn));
        System.out.println("Cleaning up...");
        conn.del("is-under-maintenance");
        Thread.sleep(1000);
        System.out.println("Should be False again: " + isUnderMaintenance(conn));
    }

    public void testConfig(Jedis conn) {
        System.out.println("\n----- testConfig -----");
        System.out.println("Let's set a config and then get a connection from that config...");
        Map<String,Object> config = new HashMap<String,Object>();
        config.put("db", 15);
        setConfig(conn, "redis", "test", config);

        Jedis conn2 = redisConnection("test");
        System.out.println(
                "We can run commands from the configured connection: " + (conn2.info() != null));
    }

    public void logRecent(Jedis conn, String name, String message) {
        logRecent(conn, name, message, INFO);
    }

    public void logRecent(Jedis conn, String name, String message, String severity) {
        String destination = "recent:" + name + ':' + severity;
        Pipeline pipe = conn.pipelined();
        //日志存储的格式为：日志+消息
        pipe.lpush(destination, TIMESTAMP.format(new Date()) + ' ' + message);
        //只保留最近的消息
        pipe.ltrim(destination, 0, 99);
        pipe.sync();
    }

    public void logCommon(Jedis conn, String name, String message) {
        logCommon(conn, name, message, INFO, 5000);
    }

    public void logCommon(
            Jedis conn, String name, String message, String severity, int timeout) {
        String commonDest = "common:" + name + ':' + severity;
        String startKey = commonDest + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            //监视当前小时小时
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());
            String existing = conn.get(startKey);

            Transaction trans = conn.multi();
            //对过去的记录重命名，也就是每小时进行消息轮换
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0){
                trans.rename(commonDest, commonDest + ":last");
                trans.rename(startKey, commonDest + ":pstart");
                trans.set(startKey, hourStart);
            }
            //更新message频率
            trans.zincrby(commonDest, 1, message);

            String recentDest = "recent:" + name + ':' + severity;
            //存放最近的日志，并且只保留99条
            trans.lpush(recentDest, TIMESTAMP.format(new Date()) + ' ' + message);
            trans.ltrim(recentDest, 0, 99);
            List<Object> results = trans.exec();
            // null response indicates that the transaction was aborted due to
            // the watched key changing.
            if (results == null){
                continue;
            }
            return;
        }
    }

    public void updateCounter(Jedis conn, String name, int count) {
        updateCounter(conn, name, count, System.currentTimeMillis() / 1000);
    }

    public static final int[] PRECISION = new int[]{1, 5, 60, 300, 3600, 18000, 86400};

    public void updateCounter(Jedis conn, String name, int count, long now){
        Transaction trans = conn.multi();
        //对每个精度的计数器都进行计数
        for (int prec : PRECISION) {
            long pnow = (now / prec) * prec;
            String hash = String.valueOf(prec) + ':' + name;
            //当分数为0时，默认按照key的字典序排序
            //键：known，key:精度+名字，值0
            trans.zadd("known:", 0, hash);
            //key:count+精度+name，pnow是当前时间，count是当前时间的计数
            trans.hincrBy("count:" + hash, String.valueOf(pnow), count);
        }
        trans.exec();
    }

    //根据时间先后对转换后的数据进行排序
    public List<Pair<Integer,Integer>> getCounter(
            Jedis conn, String name, int precision)
    {
        String hash = String.valueOf(precision) + ':' + name;
        Map<String,String> data = conn.hgetAll("count:" + hash);
        ArrayList<Pair<Integer,Integer>> results =
                new ArrayList<Pair<Integer,Integer>>();
        for (Map.Entry<String,String> entry : data.entrySet()) {
            results.add(new Pair<Integer,Integer>(
                    Integer.parseInt(entry.getKey()),
                    Integer.parseInt(entry.getValue())));
        }
        Collections.sort(results);
        return results;
    }

    public List<Object> updateStats(Jedis conn, String context, String type, double value){
        int timeout = 5000;
        String destination = "stats:" + context + ':' + type;
        String startKey = destination + ":start";
        long end = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < end){
            conn.watch(startKey);
            String hourStart = ISO_FORMAT.format(new Date());

            String existing = conn.get(startKey);
            Transaction trans = conn.multi();
            if (existing != null && COLLATOR.compare(existing, hourStart) < 0){
                trans.rename(destination, destination + ":last");
                trans.rename(startKey, destination + ":pstart");
                trans.set(startKey, hourStart);
            }

            String tkey1 = UUID.randomUUID().toString();
            String tkey2 = UUID.randomUUID().toString();
            trans.zadd(tkey1, value, "min");
            trans.zadd(tkey2, value, "max");

            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MIN),
                    destination, tkey1);
            trans.zunionstore(
                    destination,
                    new ZParams().aggregate(ZParams.Aggregate.MAX),
                    destination, tkey2);

            trans.del(tkey1, tkey2);
            trans.zincrby(destination, 1, "count");
            trans.zincrby(destination, value, "sum");
            trans.zincrby(destination, value * value, "sumsq");

            List<Object> results = trans.exec();
            if (results == null){
                continue;
            }
            return results.subList(results.size() - 3, results.size());
        }
        return null;
    }

    public Map<String,Double> getStats(Jedis conn, String context, String type){
        String key = "stats:" + context + ':' + type;
        Map<String,Double> stats = new HashMap<String,Double>();
        Set<Tuple> data = conn.zrangeWithScores(key, 0, -1);
        for (Tuple tuple : data){
            stats.put(tuple.getElement(), tuple.getScore());
        }
        stats.put("average", stats.get("sum") / stats.get("count"));
        double numerator = stats.get("sumsq") - Math.pow(stats.get("sum"), 2) / stats.get("count");
        double count = stats.get("count");
        stats.put("stddev", Math.pow(numerator / (count > 1 ? count - 1 : 1), .5));
        return stats;
    }

    private long lastChecked;
    private boolean underMaintenance;
    public boolean isUnderMaintenance(Jedis conn) {
        if (lastChecked < System.currentTimeMillis() - 1000){
            lastChecked = System.currentTimeMillis();
            String flag = conn.get("is-under-maintenance");
            underMaintenance = "yes".equals(flag);
        }

        return underMaintenance;
    }

    public void setConfig(
            Jedis conn, String type, String component, Map<String,Object> config) {
        Gson gson = new Gson();
        conn.set("config:" + type + ':' + component, gson.toJson(config));
    }

    private static final Map<String,Map<String,Object>> CONFIGS =
            new HashMap<String,Map<String,Object>>();
    private static final Map<String,Long> CHECKED = new HashMap<String,Long>();

    @SuppressWarnings("unchecked")
    public Map<String,Object> getConfig(Jedis conn, String type, String component) {
        int wait = 1000;
        String key = "config:" + type + ':' + component;

        Long lastChecked = CHECKED.get(key);
        if (lastChecked == null || lastChecked < System.currentTimeMillis() - wait){
            CHECKED.put(key, System.currentTimeMillis());

            String value = conn.get(key);
            Map<String,Object> config = null;
            if (value != null){
                Gson gson = new Gson();
                config = (Map<String,Object>)gson.fromJson(
                        value, new TypeToken<Map<String,Object>>(){}.getType());
            }else{
                config = new HashMap<String,Object>();
            }

            CONFIGS.put(key, config);
        }

        return CONFIGS.get(key);
    }

    public static final Map<String,Jedis> REDIS_CONNECTIONS =
            new HashMap<String,Jedis>();
    public Jedis redisConnection(String component){
        Jedis configConn = REDIS_CONNECTIONS.get("config");
        if (configConn == null){
            configConn = new Jedis("localhost");
            configConn.select(15);
            REDIS_CONNECTIONS.put("config", configConn);
        }

        String key = "config:redis:" + component;
        Map<String,Object> oldConfig = CONFIGS.get(key);
        Map<String,Object> config = getConfig(configConn, "redis", component);

        if (!config.equals(oldConfig)){
            Jedis conn = new Jedis("localhost");
            if (config.containsKey("db")){
                conn.select(((Double)config.get("db")).intValue());
            }
            REDIS_CONNECTIONS.put(key, conn);
        }

        return REDIS_CONNECTIONS.get(key);
    }

    public void importIpsToRedis(Jedis conn, File file) {
        FileReader reader = null;
        try{
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            int count = 0;
            String[] line = null;
            while ((line = parser.getLine()) != null){
                String startIp = line.length > 1 ? line[0] : "";
                if (startIp.toLowerCase().indexOf('i') != -1){
                    continue;
                }
                int score = 0;
                if (startIp.indexOf('.') != -1){
                    score = ipToScore(startIp);
                }else{
                    try{
                        score = Integer.parseInt(startIp, 10);
                    }catch(NumberFormatException nfe){
                        continue;
                    }
                }

                String cityId = line[2] + '_' + count;
                conn.zadd("ip2cityid:", score, cityId);
                count++;
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try{
                reader.close();
            }catch(Exception e){
                // ignore
            }
        }
    }

    public void importCitiesToRedis(Jedis conn, File file) {
        Gson gson = new Gson();
        FileReader reader = null;
        try{
            reader = new FileReader(file);
            CSVParser parser = new CSVParser(reader);
            String[] line = null;
            while ((line = parser.getLine()) != null){
                if (line.length < 4 || !Character.isDigit(line[0].charAt(0))){
                    continue;
                }
                String cityId = line[0];
                String country = line[1];
                String region = line[2];
                String city = line[3];
                String json = gson.toJson(new String[]{city, region, country});
                conn.hset("cityid2city:", cityId, json);
            }
        }catch(Exception e){
            throw new RuntimeException(e);
        }finally{
            try{
                reader.close();
            }catch(Exception e){
                // ignore
            }
        }
    }

    public int ipToScore(String ipAddress) {
        int score = 0;
        for (String v : ipAddress.split("\\.")){
            score = score * 256 + Integer.parseInt(v, 10);
        }
        return score;
    }

    public String randomOctet(int max) {
        return String.valueOf((int)(Math.random() * max));
    }

    public String[] findCityByIp(Jedis conn, String ipAddress) {
        int score = ipToScore(ipAddress);
        Set<String> results = conn.zrevrangeByScore("ip2cityid:", score, 0, 0, 1);
        if (results.size() == 0) {
            return null;
        }

        String cityId = results.iterator().next();
        cityId = cityId.substring(0, cityId.indexOf('_'));
        return new Gson().fromJson(conn.hget("cityid2city:", cityId), String[].class);
    }

    public class CleanCountersThread
            extends Thread
    {
        private Jedis conn;
        private int sampleCount = 100;
        private boolean quit;
        private long timeOffset; // used to mimic a time in the future.

        public CleanCo untersThread(int sampleCount, long timeOffset){
            this.conn = new Jedis("localhost");
            this.conn.select(15);
            this.sampleCount = sampleCount;
            this.timeOffset = timeOffset;
        }

        public void quit(){
            quit = true;
        }

        public void run(){
            int passes = 0;
            //遍历每一个计数器
            while (!quit){
                long start = System.currentTimeMillis() + timeOffset;
                int index = 0;
                //遍历精度计数器
                while (index < conn.zcard("known:")){
                    Set<String> hashSet = conn.zrange("known:", index, index);
                    index++;
                    if (hashSet.size() == 0) {
                        break;
                    }
                    String hash = hashSet.iterator().next();
                    //获取精度
                    int prec = Integer.parseInt(hash.substring(0, hash.indexOf(':')));
                    //取分钟数
                    int bprec = (int)Math.floor(prec / 60);
                    //小于1分钟的按1分钟算
                    if (bprec == 0){
                        bprec = 1;
                    }

                    if ((passes % bprec) != 0){
                        continue;
                    }

                    String hkey = "count:" + hash;
                    String cutoff = String.valueOf(
                            ((System.currentTimeMillis() + timeOffset) / 1000) - sampleCount * prec);
                    ArrayList<String> samples = new ArrayList<String>(conn.hkeys(hkey));
                    Collections.sort(samples);
                    int remove = bisectRight(samples, cutoff);

                    if (remove != 0){
                        conn.hdel(hkey, samples.subList(0, remove).toArray(new String[0]));
                        if (remove == samples.size()){
                            conn.watch(hkey);
                            if (conn.hlen(hkey) == 0) {
                                Transaction trans = conn.multi();
                                trans.zrem("known:", hash);
                                trans.exec();
                                index--;
                            }else{
                                conn.unwatch();
                            }
                        }
                    }
                }

                passes++;
                //每60s一次循环，60s没用完就就休眠
                long duration = Math.min(
                        (System.currentTimeMillis() + timeOffset) - start + 1000, 60000);
                try {
                    sleep(Math.max(60000 - duration, 1000));
                }catch(InterruptedException ie){
                    Thread.currentThread().interrupt();
                }
            }
        }

        // mimic python's bisect.bisect_right
        public int bisectRight(List<String> values, String key) {
            int index = Collections.binarySearch(values, key);
            return index < 0 ? Math.abs(index) - 1 : index + 1;
        }
    }

    public class AccessTimer {
        private Jedis conn;
        private long start;

        public AccessTimer(Jedis conn){
            this.conn = conn;
        }

        public void start(){
            start = System.currentTimeMillis();
        }

        public void stop(String context){
            long delta = System.currentTimeMillis() - start;
            List<Object> stats = updateStats(conn, context, "AccessTime", delta / 1000.0);
            double average = (Double)stats.get(1) / (Double)stats.get(0);

            Transaction trans = conn.multi();
            trans.zadd("slowest:AccessTime", average, context);
            trans.zremrangeByRank("slowest:AccessTime", 0, -101);
            trans.exec();
        }
    }
}