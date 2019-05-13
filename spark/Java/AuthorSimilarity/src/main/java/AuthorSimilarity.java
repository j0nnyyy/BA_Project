import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class AuthorSimilarity {

    private static final String basePath = "/scratch/wikipedia-dump/wiki_small_";
    private static final String logPath = "/home/ubuntu/BA_Project/log/jaccard_log.txt";
    private static final String plotDataPath = "/home/ubuntu/BA_Project/log/jaccard_data_log.txt";
    private static final String[] botNames = {"Bot", "Bots"};
    private ArrayList<String> fileNames = new ArrayList<String>();
    int fileCount;
    int fileNumber;
    private String mode;
    private String jaccardMethod;
    private double minVal;
    private double maxVal;
    private StructType schema;
    private SparkSession spark;
    private SparkContext sc;
    private Dataset df;

    public AuthorSimilarity(int fileCount, int fileNumber, String mode, String method, double minVal, double maxVal) {

        this.fileCount = fileCount;
        this.fileNumber = fileNumber;
        this.mode = mode;
        this.jaccardMethod = method;
        this.minVal = minVal;
        this.maxVal = maxVal;

        spark = new SparkSession.Builder().appName("Author similarity").config("spark.executor.memory", "128g")
                .config("spark.speculation", "true").getOrCreate();
        sc = spark.sparkContext();
        schema = createSchema();

        retrieveFilenames();
        createDataFrame();

    }

    private StructType createSchema() {

        ArrayList<StructField> authorList = new ArrayList<StructField>();
        authorList.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        authorList.add(DataTypes.createStructField("ip", DataTypes.StringType, true));
        authorList.add(DataTypes.createStructField("username", DataTypes.StringType, true));
        StructType authorSchema = DataTypes.createStructType(authorList);

        ArrayList<StructField> revisionList = new ArrayList<StructField>();
        revisionList.add(DataTypes.createStructField("comment", DataTypes.StringType, true));
        revisionList.add(DataTypes.createStructField("contributor", authorSchema, true));
        revisionList.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        revisionList.add(DataTypes.createStructField("parentid", DataTypes.StringType, true));
        revisionList.add(DataTypes.createStructField("timestamp", DataTypes.StringType, true));
        StructType revisionSchema = DataTypes.createStructType(revisionList);

        ArrayList<StructField> schemaList = new ArrayList<StructField>();
        schemaList.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        schemaList.add(DataTypes.createStructField("revision", DataTypes.createArrayType(revisionSchema), true));
        schemaList.add(DataTypes.createStructField("title", DataTypes.StringType, true));

        return DataTypes.createStructType(schemaList);

    }

    private void retrieveFilenames() {

        if(fileCount == 1) {
            fileNames.add(basePath + fileNumber + ".json");
        } else {
            for(int i = 1; i <= fileCount; i++) {
                fileNames.add(basePath + i + ".json");
            }
        }

    }

    private void createDataFrame() {

        df = spark.read().schema(schema).json(fileNames.toArray(new String[fileNames.size()]));
        df = df.withColumn("revision", explode(col("revision")))
                .select(col("title"), col("revision").getField("contributor").getField("username").alias("author"))
                .distinct();
        df =  df.where(col("author").isNotNull());
        df.cache();

        df = df.sample(false, 10000.0 / df.count(), System.currentTimeMillis());
        System.out.println("count: " + df.count());

    }

    public void calculateJaccard() {

        if(jaccardMethod.equals("cross")) {
            jaccardWithCrossJoin();
        }

    }

    private void jaccardWithCrossJoin() {

        Dataset dfJaccard = null;

        System.out.println("Self joining...");
        Dataset df1 = df.select(col("title").alias("title1"), col("author").alias("author1"));
        Dataset df2 = df.select(col("title").alias("title2"), col("author").alias("author2"));
        Dataset dfJoined = df1.crossJoin(df2);
        dfJoined.show();
        dfJoined = dfJoined.where(col("author1").lt(col("author2")));
        dfJoined.show();
        System.out.println("count" + dfJoined.count());
        System.out.println("Join complete");

        System.out.println("Calculating all articles per author pair");
        Dataset dfAll = dfJoined.groupBy(col("author1"), col("author2")).count()
                .select(col("author1").alias("ad1"), col("author2").alias("ad2"), col("count").alias("dis")).distinct();
        dfAll.show();
        System.out.println("count:" + dfAll.count());

        System.out.println("Calculating common articles");
        Dataset dfCommon = dfJoined.where(col("title1").equalTo(col("title2")))
                .groupBy(col("author1"), col("author2")).count()
                .select(col("author1").alias("ac1"), col("author2").alias("ac2"), col("count").alias("con")).distinct();
        dfCommon.show();
        System.out.println("count:" + dfCommon.count());

        System.out.println("Calculating rest");
        Dataset dfRest = dfJoined.where(col("title1").notEqual(col("title2")))
                .groupBy(col("author1"), col("author2")).count()
                .select(col("author1").alias("ac1"), col("author2").alias("ac2"))
                .withColumn("con", lit(0));
        System.out.println("count:" + dfRest.count());
        System.out.println("Union common and rest");
        dfCommon = dfCommon.union(dfRest).groupBy(col("ac1"), col("ac2")).sum();
        dfCommon = dfCommon.select(col("ac1"), col("ac2"), col("sum(con)").alias("con"));
        dfCommon.show();
        System.out.println("count:" + dfCommon.count());
        System.out.println("Joining over both authors");
        dfAll = dfAll.join(dfCommon, col("ad1").equalTo(col("ac1")).and(col("ad2").equalTo(col("ac2"))))
                .select(col("ad1"), col("ad2"), col("dis"), col("con"));
        dfAll.show();
        System.out.println("count:" + dfAll.count());
        System.out.println("Subtracting duplicates");
        dfAll = dfAll.withColumn("dis", col("dis").minus(col("con")));
        dfAll.show();
        System.out.println("count:" + dfAll.count());

        System.out.println("Calculating jaccard");
        if(mode.equals("sim")) {
            dfJaccard = dfAll.withColumn("jaccard", col("con").divide(col("dis")));
        } else if (mode.equals("dist")) {
            dfJaccard = dfAll.withColumn("jaccard", lit(1.0).minus(col("con").divide(col("dis"))));
        }
        System.out.println("count:" + dfJaccard.count());

        System.out.println("Selecting needed columns");
        Dataset<Row> dfHist = dfJaccard.select("jaccard").where(col("jaccard").geq(lit(minVal)).and(col("jaccard").leq(lit(maxVal))));
        System.out.println("Selected needed columns");

        System.out.println("Selecting similar authors");
        Dataset dfSimilar = dfJaccard.where(col("jaccard").leq(lit(0.1))).select("ad1");
        dfSimilar.count();
        System.out.println("Joining over author");
        Dataset dfAuthors = df.join(dfSimilar, col("author").equalTo(col("ad1"))).select("author", "title");
        dfSimilar.count();
        System.out.println("counting");
        Dataset dfCounts = dfAuthors.groupBy("author").count().select(col("count"));
        dfSimilar.count();
        System.out.println("Calculating average");
        Dataset<Row> dfAvg = dfCounts.select(avg(col("count")).alias("avg"));
        dfSimilar.count();
        System.out.println("Calculating standard deviation");
        Dataset<Row> dfStddev = dfCounts.select(stddev(col("count")).alias("stddev"));
        dfSimilar.count();
        double avg = dfAvg.first().getDouble(0);
        double stddev = dfStddev.first().getDouble(0);

        System.out.println("Creating java rdd");
        JavaRDD<Double> jdf = dfHist.toJavaRDD().map(row -> row.getDouble(0));

        System.out.println("Creating doublerdd");
        JavaDoubleRDD tmp = jdf.mapToDouble(y -> y);
        long[] data = tmp.histogram(new double[] {0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 1.0,});

        try {
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(plotDataPath, true)));
            String output = fileCount + ">>>" + fileNumber + ">>>" + Arrays.toString(data) + ">>>" + avg + ">>>" + stddev;

            writer.println(output);
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {

        int filecount = 1;
        int filenumber = 1;
        String mode = "sim";
        String method = "cross";
        double minVal = 0.0;
        double maxVal = 1.0;

        for(int i = 0; i < args.length; i += 2) {
            if(args[i].equals("--filecount")) {
                filecount = Integer.parseInt(args[i + 1]);
            } else if(args[i].equals("--filenumber")) {
                filenumber = Integer.parseInt(args[i + 1]);
            } else if(args[i].equals("--mode")) {
                mode = args[i + 1];
            } else if(args[i].equals("--method")) {
                method = args[i + 1];
            } else if(args[i].equals("--minval")) {
                minVal = Double.parseDouble(args[i + 1]);
            } else if(args[i].equals("--maxval")) {
                maxVal = Double.parseDouble(args[i + 1]);
            }
        }

        AuthorSimilarity authorSimilarity = new AuthorSimilarity(filecount, filenumber, mode, method, minVal, maxVal);
        authorSimilarity.calculateJaccard();

    }

}
