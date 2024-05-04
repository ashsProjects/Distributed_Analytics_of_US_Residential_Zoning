# Analyzing the Impact of Single Family Zoning in the United States using Distributed Tools
### Ayush Adhikari, Brendan Verspohl
#### Originally a part of CS455: Distributed Systems class at Colorado State University
<hr>

This is a project that aims to do distributed analytics using clusters using a spatial dataset. Our goal with this project was to analyze the impact of single family rresidential zoning in the US and correlate it to quality of life measures in an effort to dissuade a segreggation of zoning types and promote inclusivity. We hoped to be able to compare the results against data from other countries that have more includive zoning laws, but this was not possible due to constraints on data availability and language barriers. For the distributed component, we are using a cluster of 10 machines that are managed by Yarn. To do the processing of data and calculations, we applied Spark using Java and Gradle. The data itself was stored using HDFS and totaled to ~3.2 GB. For more detail on our motivation, procedures, project structure, and results, please reference the latex file or the presentation. Use the former for a more detailed explanation and the latter for a summary.

### Data Directory Structure
To access dataset using HDFS
```bash
$ hadoop fs -ls /zoning/<place>/<dataset>
```

```
|--zoning  
    |--Ohio  
        |--mental_health    
        |--occupancy  
        |--ohio-parcels  
        |--poverty    
        |--transportation_means    
        |--transportation_type  
    |--Houston    
        |--houston-parcels   
        |--mental_health   
        |--occupancy    
        |--poverty     
        |--transportation_means   
        |--transportation_type       
    |--Colorado       
        |--denver-parcels   
        |--jefferson-parcels   
        |--mental_health    
        |--occupancy      
        |--poverty         
        |--transportation_means       
        |--transportation_type    
        |--weld-parcels     
    |--Placer       
        |--mental_health   
        |--occupancy      
        |--placer-parcels    
        |--poverty       
        |--transportation_means     
        |--transportation_type     
    |--Dallas
        |--dallas-parcels 
        |--mental_health   
        |--occupancy         
        |--poverty     
        |--structure    
        |--transportation_means     
        |--transportation_type    
    |--Detroit
        |--detroit-parcels   
        |--mental_health   
        |--occupancy         
        |--poverty    
        |--structure   
        |--transportation_means   
        |--transportation_type      
    |--QOL
    |--races.csv
    |--financial.csv
```

### Data Sources
```
Parcel Data
- https://koordinates.com/explore/

CENSUS DATA
- B08006 Sex of Workers by Means of Transportation to Work (Employment -> Commuting)
- S0802 Means of Transportation to Work by Selected Characteristics (Employment -> Commuting)
- H1 OCCUPANCY STATUS (Housing, Housing, 1)
- S1702 Poverty Status in the Past 12 Months of Families (Income and Poverty -> Income and Poverty)

MENTAL HEALTH
- https://www.countyhealthrankings.org/health-data/health-outcomes/quality-of-life

DATASET FOR COUNTY LEVEL QUALITY OF LIFE
- https://www.kaggle.com/datasets/zacvaughan/cityzipcountyfips-quality-of-life
```

### Dependencies
- Java 11
- Gradle 8.0
- Apache Spark SQL 3.5
- Apache Spark Core 3.5

### Compiling

To start DFS:
```bash 
$ start-dfs.sh
```
To start master:
```bash
$ start-master.sh
```
To start workers:
```bash
$ start-workers.sh
```
To use the spark shell
```bash
$ spark-shell
```
To run the Java program using Gradle:
```bash
$ gradle run
```
