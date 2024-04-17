# CS455 Term Project
### Ayush Adhikari, Brendan Verspohl

### Questions:
1. What are the different types of zoning?
2. How many parcels are identified as a residential type? This includes SF, Condos, MF, Apartments, etc.
3. How many Single Family homes are there and what is the percentage of SF to the total number of residential zoning?
4. 

### Data Repository
insert_here

### Data Directory Structure
```bash
$ hadoop fs -ls /data/<place>/<dataset>
```

```
|--data  
    |--ohio  
        |--[]  
        |--[]  
    |--houston    
        |--[]   
        |--[]  
    |--denver    
        |--[]   
        |--[]  
    |--jefferson    
        |--[]   
        |--[]  
    |--sonoma    
        |--[]   
        |--[]  
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