# Building the Gluten Project with Velox Project

The cost model version is using Velox as backend. The building of Velox and GlutenCPP can refer to [VeloxBuild](https://github.com/apache/incubator-gluten/blob/main/docs/get-started/Velox.md)


```
./dev/builddeps-veloxbe.sh build_arrow
./dev/builddeps-veloxbe.sh build_velox
./dev/builddeps-veloxbe.sh build_gluten_cpp
mvn clean package -Pbackends-velox -Pspark-3.3 -Puniffle -DskipTests
```

After the build, put the generated jar file in `package/target/` to the spark's `/jars` directory.

# Configuration
```
/localhdd/hza214/spark-3.3.1-bin-hadoop3-velox/bin/spark-shell    
--conf spark.gluten.enabled=true  
--conf spark.gluten.sql.debug=false  
--conf spark.gluten.sql.injectNativePlanStringToExplain=false  
--conf spark.local.dir=/localssd/hza214/sparktmp  
--conf spark.plugins=org.apache.gluten.GlutenPlugin  
--conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager  
--conf spark.memory.offHeap.enabled=true   
--conf spark.memory.offHeap.size=20g   
--conf spark.gluten.ras.enabled=true   
--conf spark.gluten.ras.costModel=rough
--executor-cores 4
--driver-memory 40g   
--executor-memory 16g   
--conf spark.executor.memoryOverhead=4g
--conf spark.sql.join.preferSortMergeJoin=false 
```
