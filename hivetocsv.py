from pyspark.sql.functions import rand
from pyspark.sql import HiveContext
hive_context = HiveContext(sc)
flop = hive_context.table('test.flipflops')
flop2 = flop.withColumn('guest_iq', rand())
flop2.coalesce(1).write.csv('/user/rose/flops')
