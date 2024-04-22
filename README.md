# CS455 Term Project
### Ayush Adhikari, Brendan Verspohl

### Questions:
1. What are the different types of zoning?
2. How many parcels are identified as a residential type? This includes SF, Condos, MF, Apartments, etc.
3. How many Single Family homes are there and what is the percentage of SF to the total number of residential zoning?
4. What is the average commute time for the county?
5. What means of transport are used the most commonly? Describe averages.
6. What is the level of poverty in that county?
7. What is the average rent prices for different residential buildings?
8. What is the standard if living (income/wage/price of goods)?
9. What is the crime rate of the county?

### Data Directory Structure
```bash
$ hadoop fs -ls /zoning/<place>/<dataset>
```

```
|--zoning  
    |--Ohio  
        |--mental_health    
        |--occupancy  
        |--occupancy_meta  
        |--ohio-parcels  
        |--poverty  
        |--poverty_meta   
        |--transportation_means  
        |--transportation_means_meta  
        |--transportation_type  
        |--transportation_type_means  
    |--Houston    
        |--houston-parcels   
        |--mental_health   
        |--occupancy   
        |--occupancy_meta   
        |--poverty   
        |--poverty_meta     
        |--transportation_means   
        |--transportation_means_meta   
        |--transportation_type   
        |--transportation_type_means    
    |--Colorado       
        |--denver-parcels   
        |--jefferson-parcels   
        |--mental_health    
        |--occupancy    
        |--occupancy_meta     
        |--poverty   
        |--poverty_meta      
        |--transportation_means   
        |--transportation_means_meta     
        |--transportation_type   
        |--transportation_type_means   
        |--weld-parcels     
    |--Placer       
        |--mental_health   
        |--occupancy    
        |--occupancy_meta    
        |--placer-parcels    
        |--poverty   
        |--poverty_meta     
        |--transportation_means   
        |--transportation_means_meta   
        |--transportation_type    
        |--transportation_type_means  
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
```

### Data Sources
```
CENSUS DATA
- B08006 Sex of Workers by Means of Transportation to Work (Employment -> Commuting)
- S0802 Means of Transportation to Work by Selected Characteristics (Employment -> Commuting)
- H1 OCCUPANCY STATUS (Housing, Housing, 1)
- S1702 Poverty Status in the Past 12 Months of Families (Income and Poverty -> Income and Poverty)

MENTAL HEALTH
- https://www.countyhealthrankings.org/health-data/health-outcomes/quality-of-life

HAS COUNTY LEVEL GRAPHS FOR QUALITY OF LIFE
- https://www.miqols.org/toolbox/usqoli.html

DATASET FOR COUNTY LEVEL QUALITY OF LIFE
- https://www.kaggle.com/datasets/zacvaughan/cityzipcountyfips-quality-of-life
```

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
