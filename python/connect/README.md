# sparkless-connect

Shared Python launcher, lifecycle helpers, and optional Spark Connect client dependency.
Install `sparkless-connect[client]` to connect with the pinned `pyspark-client`. Install a
server product package to own a packaged local server process. For an existing endpoint:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://server:15002").getOrCreate()
```

The common launcher also supports explicit executable paths and keeps product selection
stable when both server products are installed.
