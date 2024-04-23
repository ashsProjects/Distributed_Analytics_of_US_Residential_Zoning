import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class Detroit {
    public static void calculateZoning() throws IOException {
        SparkSession spark = SparkSession.builder().appName("Detroit").master("local[*]").getOrCreate();
        String filePath = "hdfs://salem.cs.colostate.edu:31190/zoning/Detroit/detroit-parcels.csv";

        JavaRDD<Row> df = spark.read().option("header", false).option("delimeter", ",")
            .option("multiline", true).csv(filePath).javaRDD();

        Row header = df.first();
        JavaRDD<String> zones = df.filter(line -> !line.equals(header))
            .map(x -> {
                String zoningType = x.getString(14);
                if (zoningType == null) zoningType = "null";
                return zoningType.toLowerCase();
            });
        
        FileWriter writer = new FileWriter("./outputs/detroit/countOfAllZonesInDetroit.txt", true);
        zones.countByValue().forEach((key, value) -> {
            try {
                writer.write(key + ": " + value + "\n");
            } catch (IOException e) {}
        }); 
        
        JavaRDD<String> residential = zones.filter(x -> (x.contains("family") || x.contains("apt")
            || x.contains("condo") || x.contains("duplex") || x.contains("house")));

        Map<String, Long> countResidential = residential.countByValue();
        FileWriter writer2 = new FileWriter("./outputs/detroit/countOfAllResidentialInDetroit.txt", true);
        writer2.write("Count of all types of Residential in Detroit\n");
        long total = 0;
        for (Map.Entry<String, Long> entry: countResidential.entrySet()) {
            String line = entry.getKey() + ": " + entry.getValue() + "\n";
            total += entry.getValue();
            writer2.write(line);
        }
        writer2.write("Total: " + total);

        writer.close();
        writer2.close();
        spark.close();;
        spark.stop();
    }

    public static void calculateTransport(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Detroit/transportation_means.csv");
        JavaPairRDD<String, String> meanCommute = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String county = x.split(",")[1];
                String commuteTime = x.split(",")[787];

                return new Tuple2<>(county, commuteTime);
            });
        meanCommute.saveAsTextFile("./outputs/detroit/meanCommute");

        JavaRDD<String> typeFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Detroit/transportation_type.csv");
        JavaPairRDD<String, String> commuteTypeByCounty = typeFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();
                sb.append("total:" + desc[3]);
                sb.append("car/truck:" + desc[5]);
                sb.append("public:" + desc[17]);
                sb.append("bicycle:" + desc[29]);
                sb.append("walk:" + desc[31]);
                sb.append("wfh:" + desc[35]);

                return new Tuple2<>(desc[1], sb.toString());
            });
        commuteTypeByCounty.sortByKey().saveAsTextFile("./outputs/detroit/commuteTypes");
    }

    public static void calculatePoverty(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Detroit/poverty.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();

                sb.append("below_poverty_owner:" + desc[183] + "±" + desc[184]);
                sb.append("below_poverty_renter:" + desc[185] + "±" + desc[186]);

                return new Tuple2<>(desc[1], sb.toString());
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/detroit/percentPoverty");
    }

    public static void calculateOccupancy(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Detroit/occupancy.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();

                int total = Integer.parseInt(desc[3].replace("\"", ""));
                int owner = Integer.parseInt(desc[4].replace("\"", ""));
                int vacant = Integer.parseInt(desc[5].replace("\"", ""));

                double ownerPercent = owner * 1.00 / total;
                double vacantPercent = vacant * 1.00 / total;

                sb.append("occupied:" + ownerPercent + ",vacant:" + vacantPercent);

                return new Tuple2<>(desc[1], sb.toString());
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/detroit/occupancy");
    }

    public static void calculateQOL(JavaSparkContext sc) {
        JavaRDD<String> allQualities = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/QOL.csv");

        String header = allQualities.first();
        JavaPairRDD<String, String> qualitiesByCounty = allQualities.filter(line -> !line.equals(header))
            .filter(x -> x.startsWith("MIWayne ")).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                String countyName = desc[0].substring(2).toLowerCase().replace(" county", "");
                List<String> qualityList = new ArrayList<>();
                
                qualityList.add(desc[8]);//crime rate
                qualityList.add(desc[9]);//unemployment
                qualityList.add(desc[16]);//nationalParkCount
                qualityList.add(desc[17]);//stateParkCoverage
                qualityList.add(desc[18]);//cost of living
                
                return new Tuple2<>(countyName, qualityList);
            }).mapToPair(x -> {
                StringBuilder sb = new StringBuilder();
                List<String> qualities = x._2;

                sb.append("\n\tcrimeRate:" + qualities.get(0));
                sb.append("\n\tunemployment:" + qualities.get(1));
                sb.append("\n\tnationalParkCount:" + qualities.get(2));
                sb.append("\n\tstateParkCoverage:" + qualities.get(3));
                sb.append("\n\tcostOfLiving:" + qualities.get(4));

                return new Tuple2<>(x._1, sb.toString());
            });
            qualitiesByCounty.sortByKey().saveAsTextFile("./outputs/detroit/allQualitiesPlusHealth");
    }

    public static void main(String[] args) throws IOException {
        // calculateZoning();

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Detroit Analysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // calculateTransport(sc);
        // calculatePoverty(sc);
        // calculateOccupancy(sc);
        calculateQOL(sc);

        sc.close();
        sc.stop();
    }
}
