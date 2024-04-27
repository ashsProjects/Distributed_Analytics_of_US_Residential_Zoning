import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class Houston {
    public static void calculateZoning() throws IOException {
        SparkSession spark = SparkSession.builder().appName("Houston").master("local[*]").getOrCreate();
        String filePath = "hdfs://salem.cs.colostate.edu:31190/zoning/Houston/houston-parcels.csv";

        JavaRDD<Row> df = spark.read().option("header", false).option("delimeter", ",")
            .option("multiline", true).csv(filePath).javaRDD();

        Row header = df.first();
        JavaRDD<String> residential = df.filter(line -> !line.equals(header))
            .map(x -> {
                String zoningType = x.getString(34);
                if (zoningType == null) zoningType = "null";
                return zoningType.toLowerCase();
            }).filter(x -> x.contains("residential"));

        residential.distinct().saveAsTextFile("./outputs/houston/allResidentialZoningTypes");

        Map<String, Long> countResidential = residential.countByValue();
        FileWriter writer = new FileWriter("./outputs/houston/countOfAllResidentialInHouston.txt", true);
        writer.write("Count of all types of Residential in Houston\n");
        long total = 0;
        for (Map.Entry<String, Long> entry: countResidential.entrySet()) {
            String line = entry.getKey() + ": " + entry.getValue() + "\n";
            total += entry.getValue();
            writer.write(line);
        }
        writer.write("Total: " + total);

        writer.close();
        spark.close();;
        spark.stop();
    }

    public static void calculateTransport(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Houston/transportation_means.csv");
        JavaPairRDD<String, String> meanCommute = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String county = x.split(",")[1];
                String commuteTime = x.split(",")[787];

                return new Tuple2<>(county, commuteTime);
            });
        meanCommute.saveAsTextFile("./outputs/houston/meanCommute");

        JavaRDD<String> typeFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Houston/transportation_type.csv");
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
        commuteTypeByCounty.sortByKey().saveAsTextFile("./outputs/houston/commuteTypes");
    }

    public static void calculatePoverty(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Houston/poverty.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();

                sb.append("below_poverty_owner:" + desc[183] + "±" + desc[184]);
                sb.append("below_poverty_renter:" + desc[185] + "±" + desc[186]);

                return new Tuple2<>(desc[1], sb.toString());
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/houston/percentPoverty");
    }

    public static void calculateOccupancy(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Houston/occupancy.csv");
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
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/houston/occupancy");
    }

    public static void calculateQOL(JavaSparkContext sc) {
        JavaRDD<String> allQualities = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/QOL.csv");
        JavaRDD<String> mentalHealth = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Houston/mental_health.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 2).map(pair -> pair._1())
            .filter(x -> x.startsWith("Harris"));
        
        JavaPairRDD<String, Double> mentalHealthByCounty = mentalHealth.mapToPair(x -> {
            String county = x.split(",")[0].toLowerCase().replace("\"", "");
            Double value = Double.parseDouble(x.split(",")[1].replace("\"", ""));
            return new Tuple2<>(county, value);
        });

        String header = allQualities.first();
        JavaPairRDD<String, List<String>> qualitiesByCounty = allQualities.filter(line -> !line.equals(header))
            .filter(x -> x.startsWith("TXHarris ")).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                String countyName = desc[0].substring(2).toLowerCase().replace(" county", "");
                List<String> qualityList = new ArrayList<>();
                
                qualityList.add(desc[8]);//crime rate
                qualityList.add(desc[9]);//unemployment
                qualityList.add(desc[16]);//nationalParkCount
                qualityList.add(desc[17]);//stateParkCoverage
                qualityList.add(desc[18]);//cost of living
                
                return new Tuple2<>(countyName, qualityList);
            });
        
        JavaPairRDD<String, String> allQualitiesPlusHealth = qualitiesByCounty.leftOuterJoin(mentalHealthByCounty)
            .mapToPair(x -> {
                StringBuilder sb = new StringBuilder();
                List<String> qualities = x._2._1;
                Optional<Double> health = x._2._2;

                sb.append("\n\tcrimeRate:" + qualities.get(0));
                sb.append("\n\tunemployment:" + qualities.get(1));
                sb.append("\n\tnationalParkCount:" + qualities.get(2));
                sb.append("\n\tstateParkCoverage:" + qualities.get(3));
                sb.append("\n\tcostOfLiving:" + qualities.get(4));
                sb.append("\n\tmentalHealth:" + health.or(0.0));

                return new Tuple2<>(x._1, sb.toString());
            });
        allQualitiesPlusHealth.sortByKey().saveAsTextFile("./outputs/houston/allQualitiesPlusHealth");
    }

    public static void calculateHousingCharacteristics(JavaSparkContext sc) {
        JavaRDD<String> raceFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/housing_race.csv");
        JavaPairRDD<String, String> racesByCounty = raceFile.zipWithIndex()
            .filter(pair -> (pair._2() == 47))
            .map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                String countyName = desc[1].toLowerCase().replace(" county", "");
                StringBuilder sb = new StringBuilder();

                sb.append("white:" + desc[59]);
                sb.append("black:" + desc[61]);
                sb.append("asian:" + desc[65]);
                sb.append("hispanic:" + desc[73]);

                return new Tuple2<>(countyName, sb.toString());
            });
            racesByCounty.sortByKey().saveAsTextFile("./outputs/houston/racesByCounty");

        JavaRDD<String> financialFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/housing_financial.csv");
        JavaPairRDD<String, String> financeByCounty = financialFile.zipWithIndex()
            .filter(pair -> (pair._2() == 47))
            .map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                String countyName = desc[1].toLowerCase().replace(" county", "");
                return new Tuple2<>(countyName, desc[27]);
            });
            financeByCounty.sortByKey().saveAsTextFile("./outputs/houston/financesByCounty");
    }

    public static void main(String[] args) throws IOException {
        calculateZoning();

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Houston Analysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        calculateTransport(sc);
        calculatePoverty(sc);
        calculateOccupancy(sc);
        calculateQOL(sc);
        calculateHousingCharacteristics(sc);

        sc.close();
        sc.stop();
    }
}
