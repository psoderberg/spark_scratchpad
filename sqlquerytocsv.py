#from cli, start with following command
#$pyspark2 --jars /lib/hadoop_lib/spark/elasticsearch-hadoop-5.5.1.jar

from pyspark.sql import HiveContext
from pyspark.sql.functions import to_date
from pyspark.sql.types import DataType

hive_context = HiveContext(sc)

#import hive tables
order_lines = hive_context.table('electronics_data.order_lines')
products = hive_context.table('electronics_data.products')
product_costs = hive_context.table('electronics_data.products_prices')
order_headers = hive_context.table('electronics_data.order_headers')
customers = hive_context.table('electronics_data.customers')
countries = hive_context.table('electronics_data.countries')
states_provs = hive_context.table('electronics_data.states_provs')
product_subfamily = hive_context.table('electronics_data.product_subfamily')
product_family = hive_context.table('electronics_data.product_family')

#create temp tables from hive table to query
order_lines.registerTempTable('order_lines')
products.registerTempTable('products')
product_costs.registerTempTable('product_costs')
order_headers.registerTempTable('order_headers')
customers.registerTempTable('customers')
countries.registerTempTable('countries')
states_provs.registerTempTable('states_provs')
product_subfamily.registerTempTable('product_subfamily')
product_family.registerTempTable('product_family')

#create df from sql query
view = sqlContext.sql('SELECT ol.line_id,ol.schedule_ship_date,ol.quantity, ol.discount,ol.product_id,ol.net_price,oh.order_number,oh.po_id,c.customer_name,c.city,sp.state_name,ct.country_name,p.product_number,p.product_name,p.description,p.uom,ps.product_subfamily_name,pf.product_family_name FROM order_lines ol INNER JOIN order_headers oh on ol.header_id = oh.header_id INNER JOIN customers c on oh.sold_to_id = c.customer_id INNER JOIN countries ct on c.country_id = ct.country_id INNER JOIN states_provs sp on c.state_prov_id = sp.state_prov_id INNER JOIN products p on ol.product_id = p.product_id INNER JOIN products prod on ol.product_id = prod.product_id INNER JOIN product_subfamily ps on p.subfamily_id = ps.subfamily_id inner join product_family pf on p.family_id = pf.family_id')

#must specify new "index/type", it will create one on write, and options
#because no longer using localhost, need to use rose node

view2 = view.withColumn("schedule_ship_date", view["schedule_ship_date"].cast("date"))

view2.write.format("org.elasticsearch.spark.sql").option("es.resource", "electronics2/table").option("es.nodes", "rose").save()

