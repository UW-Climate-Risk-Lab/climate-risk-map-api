Contains database setup and migrations for v1 of the Climate Risk Map API

Databases are populated initially with the PgOSM Flex ETL Tool. Databases are segmented by region. This is done because once a database is populated with data from the ETL tool for a given region, it becomes difficult to switch regions. 