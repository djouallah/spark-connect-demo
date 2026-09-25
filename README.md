# Spark Connect demo

A repo for testing Spark Connect platforms in a lakehouse scenario: open table formats only (Iceberg, or Delta on Fabric), never a platform's proprietary tables. Platforms: Snowflake, LakeSail and others. The same PySpark jobs run on each one, and the results for each platform are in [findings/](findings/). Fabric Spark is here too, as a reference Spark: it has no Spark Connect, so its jobs are submitted as Spark Job Definitions.
