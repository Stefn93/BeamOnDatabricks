# BeamOnDatabricks
This repository provides a demo of my walk-through about running an Apache Beam Pipeline on Azure Databricks.

1. Import Maven Project
2. Setup project's JDK to 1.8
3. Create fat-jar using maven lifecycle
4. Attach the shaded jar to a Databricks Job  
    a. Select Runtime version 6.4  
    b. Add `--runner=SparkRunner --usesProvidedSparkContext` to Job's parameters  
    c. Configure other Job parameters as you prefer  
5. Run your Databricks Job!

N.B. The same method should work on a normal HDInsight Cluster too.