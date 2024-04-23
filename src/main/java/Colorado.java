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

public class Colorado {
    public static void calculateZoning(JavaSparkContext sc) throws IOException {
        JavaPairRDD<String, String> denverFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/denver-parcels.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 1).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",0);
                return new Tuple2<>("denver", desc[51].toLowerCase());
            });
        JavaPairRDD<String, String> jeffersonFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/jefferson-parcels.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 1).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",0);
                return new Tuple2<>("jefferson", desc[0].toLowerCase());
            });
        JavaPairRDD<String, String> weldFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/weld-parcels.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 1).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",0);
                return new Tuple2<>("weld", desc[33].toLowerCase());
            });

        JavaPairRDD<String, String> vstacked = denverFile.union(jeffersonFile).union(weldFile)
            .groupByKey().flatMapValues(x -> {
                List<String> filteredValues = new ArrayList<>();
                for (String s: x) {
                    if (s.contains("family") || s.contains("home") || s.contains("condo") || s.contains("duplex") 
                        || s.contains("house") || s.contains("apt") || s.contains("apartment") || s.contains("triplex"))
                        filteredValues.add(s);
                }
                return filteredValues.iterator();
            });

        JavaPairRDD<String, Iterable<String>> residentialByCounty = vstacked.groupByKey();
        residentialByCounty.coalesce(1).saveAsTextFile("./outputs/colorado/allResidentialTypesAllCounties");

        Map<String, Long> countColorado = vstacked.values().countByValue();
        FileWriter writer = new FileWriter("./outputs/colorado/countOfAllResidentialInColorado.txt", true);
        writer.write("Count of all types of Residential in Colorado\n");
        for (Map.Entry<String, Long> entry: countColorado.entrySet()) {
            String line = entry.getKey() + ": " + entry.getValue() + "\n";
            writer.write(line);
        }
        
        residentialByCounty.mapToPair(x -> {
            Long size = x._2.spliterator().getExactSizeIfKnown();
            return new Tuple2<>(x._1, size);
        }).sortByKey().coalesce(1).saveAsTextFile("./outputs/colorado/countOfAllResidentialByCounty");
        
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
        countsByCounty.sortByKey().coalesce(1).saveAsTextFile("./outputs/colorado/countOfEachResidentialByCounty");

        writer.close();
    }

    public static void calculateTransport(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/transportation_means.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String county = x.split(",")[1];
                String commuteTime = x.split(",")[787];

                return new Tuple2<>(county, commuteTime);
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/colorado/meanCommuteByCounty");

        JavaRDD<String> typeFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/transportation_type.csv");
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
        commuteTypeByCounty.sortByKey().saveAsTextFile("./outputs/colorado/commuteTypesByCounty");
    }

    public static void calculatePoverty(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/poverty.csv");
        JavaPairRDD<String, String> meanCommuteByCounty = inputFile.zipWithIndex()
            .filter(pair -> pair._2() >= 3).map(pair -> pair._1()).mapToPair(x -> {
                String[] desc = x.split(",");
                StringBuilder sb = new StringBuilder();

                sb.append("below_poverty_owner:" + desc[183] + "±" + desc[184]);
                sb.append("below_poverty_renter:" + desc[185] + "±" + desc[186]);

                return new Tuple2<>(desc[1], sb.toString());
            });
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/colorado/percentPovertyByCounty");
    }

    public static void calculateOccupancy(JavaSparkContext sc) {
        JavaRDD<String> inputFile = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/occupancy.csv");
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
        meanCommuteByCounty.sortByKey().saveAsTextFile("./outputs/colorado/occupancyByCounty");
    }

    public static void calculateQOL(JavaSparkContext sc) {
        JavaRDD<String> allQualities = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/QOL.csv");
        JavaRDD<String> mentalHealth = sc.textFile("hdfs://salem.cs.colostate.edu:31190/zoning/Colorado/mental_health.csv")
            .zipWithIndex().filter(pair -> pair._2() >= 2).map(pair -> pair._1());
        
        JavaPairRDD<String, Double> mentalHealthByCounty = mentalHealth.mapToPair(x -> {
            String county = x.split(",")[0].toLowerCase().replace("\"", "");
            Double value = Double.parseDouble(x.split(",")[1].replace("\"", ""));
            return new Tuple2<>(county, value);
        });

        String header = allQualities.first();
        JavaPairRDD<String, List<String>> qualitiesByCounty = allQualities.filter(line -> !line.equals(header))
            .filter(x -> x.startsWith("CO")).mapToPair(x -> {
                String[] desc = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                String countyName = desc[0].substring(2).toLowerCase().replace(" county", "");
                List<String> qualityList = new ArrayList<>();
                
                qualityList.add(desc[8]);//crime rate
                qualityList.add(desc[9]);//unemployment
                qualityList.add(desc[16]);//nationalParkCount
                qualityList.add(desc[17]);//stateParkCoverage
                qualityList.add(desc[18]);//cost of living
                
                return new Tuple2<>(countyName, qualityList);
            }).filter(x -> (x._1.equals("denver") || x._1.equals("jefferson") || x._1.equals("weld")));
        
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
            allQualitiesPlusHealth.sortByKey().saveAsTextFile("./outputs/colorado/allQualitiesPlusHealth");
    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Colorado Analysis");
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
