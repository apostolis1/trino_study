SELECT (SELECT count(*) from tpcds.sf10.dbgen_version)=(SELECT count(*) from cassandra.trino.dbgen_version) AS dbgen_version,(SELECT count(*) from tpcds.sf10.customer_address)=(SELECT count(*) from cassandra.trino.customer_address) AS customer_address,(SELECT count(*) from tpcds.sf10.customer_demographics)=(SELECT count(*) from cassandra.trino.customer_demographics) AS customer_demographics,(SELECT count(*) from tpcds.sf10.date_dim)=(SELECT count(*) from cassandra.trino.date_dim) AS date_dim,(SELECT count(*) from tpcds.sf10.warehouse)=(SELECT count(*) from cassandra.trino.warehouse) AS warehouse,(SELECT count(*) from tpcds.sf10.ship_mode)=(SELECT count(*) from cassandra.trino.ship_mode) AS ship_mode,(SELECT count(*) from tpcds.sf10.time_dim)=(SELECT count(*) from cassandra.trino.time_dim) AS time_dim,(SELECT count(*) from tpcds.sf10.reason)=(SELECT count(*) from cassandra.trino.reason) AS reason,(SELECT count(*) from tpcds.sf10.income_band)=(SELECT count(*) from cassandra.trino.income_band) AS income_band,(SELECT count(*) from tpcds.sf10.item)=(SELECT count(*) from cassandra.trino.item) AS item,(SELECT count(*) from tpcds.sf10.store)=(SELECT count(*) from cassandra.trino.store) AS store,(SELECT count(*) from tpcds.sf10.call_center)=(SELECT count(*) from cassandra.trino.call_center) AS call_center,(SELECT count(*) from tpcds.sf10.customer)=(SELECT count(*) from cassandra.trino.customer) AS customer,(SELECT count(*) from tpcds.sf10.web_site)=(SELECT count(*) from cassandra.trino.web_site) AS web_site,(SELECT count(*) from tpcds.sf10.store_returns)=(SELECT count(*) from cassandra.trino.store_returns) AS store_returns,(SELECT count(*) from tpcds.sf10.household_demographics)=(SELECT count(*) from cassandra.trino.household_demographics) AS household_demographics,(SELECT count(*) from tpcds.sf10.web_page)=(SELECT count(*) from cassandra.trino.web_page) AS web_page,(SELECT count(*) from tpcds.sf10.promotion)=(SELECT count(*) from cassandra.trino.promotion) AS promotion,(SELECT count(*) from tpcds.sf10.catalog_page)=(SELECT count(*) from cassandra.trino.catalog_page) AS catalog_page,(SELECT count(*) from tpcds.sf10.inventory)=(SELECT count(*) from cassandra.trino.inventory) AS inventory,(SELECT count(*) from tpcds.sf10.catalog_returns)=(SELECT count(*) from cassandra.trino.catalog_returns) AS catalog_returns,(SELECT count(*) from tpcds.sf10.web_returns)=(SELECT count(*) from cassandra.trino.web_returns) AS web_returns,(SELECT count(*) from tpcds.sf10.web_sales)=(SELECT count(*) from cassandra.trino.web_sales) AS web_sales,(SELECT count(*) from tpcds.sf10.catalog_sales)=(SELECT count(*) from cassandra.trino.catalog_sales) AS catalog_sales,(SELECT count(*) from tpcds.sf10.store_sales)=(SELECT count(*) from cassandra.trino.store_sales) AS store_sales;;