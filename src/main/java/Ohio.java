import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class Ohio {
    public static void calculateZoning(JavaSparkContext sc) throws IOException {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/ohio-parcels.csv");

        String header = inputFile.first();
        JavaPairRDD<String, String> residential = inputFile.filter(line -> !line.equals(header))
            .mapToPair(x -> {
                String county = x.split(",")[0];
                String zoningType = x.split(",")[3];
                return new Tuple2<>(county, zoningType);
            }).groupByKey().flatMapValues(x -> {
                List<String> filteredValues = new ArrayList<>();
                for (String s: x) {
                    if (s.startsWith("5")) filteredValues.add(s);
                }
                return filteredValues.iterator();
            });
        residential.cache();

        JavaPairRDD<String, Iterable<String>> residentialByCounty = residential.groupByKey();
        residentialByCounty.coalesce(1).saveAsTextFile("./outputs/ohio/allResidentialTypesAllCounties");

        Map<String, Long> countOhio = residential.values().countByValue();
        FileWriter writer = new FileWriter("./outputs/ohio/countOfAllResidentialInOhio.txt", true);
        writer.write("Count of all types of Residential in Ohio\n");
        for (Map.Entry<String, Long> entry: countOhio.entrySet()) {
            String line = entry.getKey() + ": " + entry.getValue() + "\n";
            writer.write(line);
        }
        writer.write("END-----------------\n\n");
        
        residentialByCounty.mapToPair(x -> {
            Long size = x._2.spliterator().getExactSizeIfKnown();
            return new Tuple2<>(x._1, size);
        }).sortByKey().coalesce(1).saveAsTextFile("./outputs/ohio/countOfAllResidentialByCounty");
        
        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> countsByCounty = residentialByCounty.mapValues(x -> {
            Map<String, Integer> typeCounts = new HashMap<>();
            for (String type: x) {
                typeCounts.put(type, typeCounts.getOrDefault(type, 0)+1);
            }
            ArrayList<Tuple2<String, Integer>> typeCountList = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : typeCounts.entrySet()) {
                typeCountList.add(new Tuple2<>(entry.getKey(), entry.getValue()));
            }
            return typeCountList;
        });
        countsByCounty.sortByKey().coalesce(1).saveAsTextFile("./outputs/ohio/countOfEachResidentialByCounty");
        
        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> relevantCountsByCounty = countsByCounty.mapToPair(x -> {
            ArrayList<Tuple2<String, Integer>> typeCountList = new ArrayList<>();
            for (Tuple2<String, Integer> zone: x._2) {
                if (zone._1.startsWith("510") || zone._1.startsWith("520") || zone._1.startsWith("530") || zone._1.startsWith("540") || zone._1.startsWith("550") || zone._1.startsWith("560")) {
                    typeCountList.add(new Tuple2<>(zone._1, zone._2));
                }
            }
            return new Tuple2<>(x._1, typeCountList);
        });
        relevantCountsByCounty.sortByKey().coalesce(1).saveAsTextFile("./outputs/ohio/relevantResidentialByCounty");

        writer.close();
    }

    public static void calculateTransport(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/transportation_means.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String county = x.split(",")[1];
                String commuteTime = x.split(",")[787];

                return new Tuple2<>(county, commuteTime);
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/ohio/meanCommuteByCounty");

        JavaRDD<String> typeFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/transportation_type.csv");
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
        commuteTypeByCounty.sortByKey().saveAsTextFile("./outputs/ohio/commuteTypesByCounty");
    }

    public static void calculatePoverty(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/poverty.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();

                sb.append("below_poverty_owner:" + desc[183] + "±" + desc[184]);
                sb.append("below_poverty_renter:" + desc[185] + "±" + desc[186]);

                return new Tuple2<>(desc[1], sb.toString());
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/ohio/percentPovertyByCounty");
    }

    public static void calculateOccupancy(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/occupancy.csv");
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
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/ohio/occupancyByCounty");
    }

    public static void calculateQOL(JavaSparkContext sc) {
        JavaRDD<String> allQualities = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/QOL.csv");
        JavaRDD<String> mentalHealth = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Ohio/mental_health.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 2).map(pair -> pair._1());
        
        JavaPairRDD<String, Double> mentalHealthByCounty = mentalHealth.mapToPair(x -> {
            String county = x.split(",")[0].toLowerCase().replace("\"", "");
            Double value = Double.parseDouble(x.split(",")[1].replace("\"", ""));
            return new Tuple2<>(county, value);
        });

        String header = allQualities.first();
        JavaPairRDD<String, List<String>> qualitiesByCounty = allQualities.filter(line -> !line.equals(header))
            .filter(x -> x.startsWith("OH")).mapToPair(x -> {
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
            allQualitiesPlusHealth.sortByKey().saveAsTextFile("./outputs/ohio/allQualitiesPlusHealth");
    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Ohio Analysis");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        calculateZoning(sc);
        calculateTransport(sc);
        calculatePoverty(sc);
        calculateOccupancy(sc);
        calculateQOL(sc);

        sc.close();
        sc.stop();
    }
}
