"""
This module houses code relating to building SQL queries
"""

from psycopg2 import sql

from typing import List, Dict, Tuple, Optional, Any

import app.v1.config as config

from app.v1.schemas import GetDataInputParameters


class GetDataQueryBuilder:
    """Creates query for PG OSM Flex Database

    The query will return data in a GeoJSON format
    """

    def __init__(self, input_params: GetDataInputParameters) -> None:

        self.input_params = input_params

        # Primary table will be a materialized view of the given category
        self.primary_table = self.input_params.osm_category

    def _create_select_statement(self) -> Tuple[sql.SQL, List[Any]]:
        """Bulids a dynamic SQL SELECT statement for the get_osm_data method

        NOTE, we use ST_Centroid() to get lat/lon values for non-point shapes.
        The lat/lon returned is not guarenteed to be on the shape itself, and represents the
        geometric center of mass of the shape. This should be fine for polygons and points,
        but may be meaningless for long linestrings.
        Aleternative methods may be ST_PointOnSurface() or ST_LineInterpolatePoint(), however
        these are more computationally expensive and for now not worth the implementation.

        """
        params = list()

        # Initial list of fields that are always returned
        select_fields = [
            sql.Identifier(config.OSM_SCHEMA_NAME, self.primary_table, "osm_id"),
            sql.Identifier(config.OSM_SCHEMA_NAME, self.primary_table, "osm_type"),
            sql.SQL("{schema}.{table}.{column} AS osm_tags").format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                table=sql.Identifier(config.OSM_TABLE_TAGS),
                column=sql.Identifier("tags"),
            ),
            sql.SQL("ST_Transform({schema}.{table}.{column}, %s) AS geometry").format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                table=sql.Identifier(self.primary_table),
                column=sql.Identifier(config.OSM_COLUMN_GEOM),
            ),
            sql.SQL(
                "ST_AsText(ST_Transform({schema}.{table}.{column}, %s), 3) AS geometry_wkt"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                table=sql.Identifier(self.primary_table),
                column=sql.Identifier(config.OSM_COLUMN_GEOM),
            ),
            sql.SQL(
                "ST_X(ST_Centroid(ST_Transform({schema}.{table}.{column}, %s))) AS longitude"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                table=sql.Identifier(self.primary_table),
                column=sql.Identifier(config.OSM_COLUMN_GEOM),
            ),
            sql.SQL(
                "ST_Y(ST_Centroid(ST_Transform({schema}.{table}.{column}, %s))) AS latitude"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                table=sql.Identifier(self.primary_table),
                column=sql.Identifier(config.OSM_COLUMN_GEOM),
            ),
        ]
        params.extend([self.input_params.epsg_code] * 4)

        # Add extra where clause for subtypes if they are specified
        if self.input_params.osm_subtypes:
            select_fields.append(
                sql.Identifier(
                    config.OSM_SCHEMA_NAME, self.primary_table, "osm_subtype"
                )
            )

        # County and City tables are aliased in the _create_join_method()
        conditions = self._create_admin_table_conditions("county")
        county_field = sql.SQL("{admin_table_alias}.name AS county").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            admin_table_alias=sql.Identifier(conditions["alias"]),
        )
        select_fields.append(county_field)

        conditions = self._create_admin_table_conditions("city")
        city_field = sql.SQL("{admin_table_alias}.name AS city").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            admin_table_alias=sql.Identifier(conditions["alias"]),
        )
        select_fields.append(city_field)

        if (
            self.input_params.climate_variable
            and self.input_params.climate_ssp
            and self.input_params.climate_month
            and self.input_params.climate_decade
        ):
            select_fields.append(
                sql.SQL("{climate_table_alias}.ssp").format(
                    climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                    climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.month").format(
                    climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                    climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.decade").format(
                    climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                    climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.variable AS climate_variable").format(
                    climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                    climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                )
            )
            select_fields.append(
                sql.SQL("{climate_table_alias}.value AS climate_exposure").format(
                    climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                    climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                )
            )

        select_statement = sql.SQL("SELECT {columns}").format(
            columns=sql.SQL(", ").join(select_fields)
        )
        self.select_statement = select_statement
        return select_statement, params

    def _create_from_statement(self) -> sql.SQL:

        from_statement = sql.SQL("FROM {schema}.{table}").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            table=sql.Identifier(self.primary_table),
        )
        return from_statement

    def _create_join_statement(self) -> Tuple[sql.SQL, List[Any]]:
        """Builds SQL Join statement

        Returns:
            Tuple[sql.SQL, List[Any]]: Returns SQL language object and list of params
        """
        params = list()

        # the tags table contains all of the properties of the features
        join_statement = sql.SQL(
            "JOIN {schema}.{tags_table} ON {schema}.{primary_table}.osm_id = {schema}.{tags_table}.osm_id"
        ).format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            tags_table=sql.Identifier(config.OSM_TABLE_TAGS),
            primary_table=sql.SQL(self.primary_table),
        )

        # Dynamically add government administrative boundaries as necessary
        admin_conditions = []
        admin_conditions.append(self._create_admin_table_conditions("county"))
        admin_conditions.append(self._create_admin_table_conditions("city"))

        # Iterate over the admin conditions to build the joins dynamically
        for admin in admin_conditions:
            admin_join = sql.SQL(
                "LEFT JOIN {schema}.{admin_table} {alias}"
                "ON ST_Intersects({schema}.{primary_table}.{geom_column}, {alias}.{geom_column}) "
                "AND {alias}.admin_level = %s "
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                admin_table=sql.Identifier(config.OSM_TABLE_PLACES),
                primary_table=sql.Identifier(self.primary_table),
                geom_column=sql.Identifier(config.OSM_COLUMN_GEOM),
                alias=sql.Identifier(admin["alias"]),
            )
            params.append(admin["level"])
            join_statement = sql.SQL(" ").join([join_statement, admin_join])

        if (
            self.input_params.climate_variable
            and self.input_params.climate_ssp
            and self.input_params.climate_month
            and self.input_params.climate_decade
        ):
            climate_join = sql.Composed(
                [
                    sql.SQL("LEFT JOIN ("),
                    sql.SQL(
                        "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value "
                    ),
                    sql.SQL("FROM {climate_schema}.{scenariomip} s ").format(
                        climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                        scenariomip=sql.Identifier(config.SCENARIOMIP_TABLE),
                    ),
                    sql.SQL(
                        "LEFT JOIN {climate_schema}.{scenariomip_variable} v "
                    ).format(
                        climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                        scenariomip_variable=sql.Identifier(
                            config.SCENARIOMIP_VARIABLE_TABLE
                        ),
                    ),
                    sql.SQL("ON s.variable_id = v.id "),
                    sql.SQL(
                        "WHERE v.ssp = %s AND v.variable = %s AND s.decade IN %s AND s.month IN %s"
                    ),
                    sql.SQL(") AS {climate_table_alias} ").format(
                        climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS)
                    ),
                    sql.SQL(
                        "ON {schema}.{primary_table}.osm_id = {climate_table_alias}.osm_id"
                    ).format(
                        schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                        primary_table=sql.Identifier(self.primary_table),
                        climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                    ),
                ]
            )
            params += [
                self.input_params.climate_ssp,
                self.input_params.climate_variable,
                tuple(set(self.input_params.climate_decade)),
                tuple(set(self.input_params.climate_month)),
            ]

            join_statement = sql.SQL(" ").join([join_statement, climate_join])

        self.join_statement = join_statement
        return join_statement, params

    def _create_where_clause(self) -> Tuple[sql.SQL, List[Any]]:
        params = list()
        # Always filter by osm type to throttle data output!
        where_clause = sql.SQL("WHERE {schema}.{primary_table}.{column} IN %s").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            primary_table=sql.Identifier(self.primary_table),
            column=sql.Identifier("osm_type"),
        )
        params.append(tuple(self.input_params.osm_types))

        if self.input_params.osm_subtypes:
            subtype_clause = sql.SQL(
                "AND {schema}.{primary_table}.{column} IN %s"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                primary_table=sql.Identifier(self.primary_table),
                column=sql.Identifier("osm_subtype"),
            )
            params.append(tuple(self.input_params.osm_subtypes))
            where_clause = sql.SQL(" ").join([where_clause, subtype_clause])

        if self.input_params.geom_type:
            geom_type_clause = sql.SQL(
                "AND {schema}.{primary_table}.geom_type = %s"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                primary_table=sql.Identifier(self.primary_table),
            )
            params.append("ST_" + self.input_params.geom_type)
            where_clause = sql.SQL(" ").join([where_clause, geom_type_clause])

        # If a bounding box GeoJSON is passed in, use as filter
        if self.input_params.bbox:
            bbox_filter = sql.SQL("AND (")
            count = 0
            # Handles multiple bounding boxes drawn by user
            for feature in self.input_params.bbox.features:
                if count == 0:
                    pass
                else:
                    conditional = sql.SQL("OR")
                    bbox_filter = sql.SQL(" ").join([bbox_filter, conditional])
                feature_filter = sql.SQL(
                    "ST_Intersects(ST_Transform({schema}.{primary_table}.{geom_column}, %s), ST_GeomFromText(%s, %s))"
                ).format(
                    schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                    primary_table=sql.Identifier(self.primary_table),
                    geom_column=sql.Identifier(config.OSM_COLUMN_GEOM),
                )
                params.append(self.input_params.epsg_code)
                params.append(feature.geometry.wkt)
                params.append(self.input_params.epsg_code)
                bbox_filter = sql.SQL(" ").join([bbox_filter, feature_filter])
                count += 1
            bbox_filter = sql.SQL(" ").join([bbox_filter, sql.SQL(")")])

            where_clause = sql.SQL(" ").join([where_clause, bbox_filter])

        self.where_clause = where_clause
        return where_clause, params

    def _create_limit(self) -> Tuple[sql.SQL, List[Any]]:
        """Adds limit to reduce size of output, for debugging and throttling

        NOTE: This limits records, not discrete geo features.
        TODO:is to refactor to make sure features are not repeated in return data

        """
        params = list()
        if self.input_params.limit:
            limit_statement = sql.SQL("LIMIT %s")
            params.append(self.input_params.limit)
            return limit_statement, params
        return sql.SQL(""), params

    def _create_admin_table_conditions(self, condition: str) -> Dict:

        admin_conditions = {
            "condition": condition,
            "level": config.OSM_TABLE_PLACES_ADMIN_LEVELS[condition],
            "alias": condition,
        }

        return admin_conditions

    def build_query(self) -> Tuple[sql.Composable, List[Any]]:
        """
        Builds SQL query based on user input

        Returns both query and query parameters

        """

        self.query_params = list()
        self.query = sql.SQL("")

        geojson_statement = sql.SQL(
            """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(geojson.*)::json))
            
            FROM (
        """
        )

        select_statement, params = self._create_select_statement()
        self.query_params.extend(params)

        from_statement = self._create_from_statement()

        join_statement, params = self._create_join_statement()
        self.query_params.extend(params)

        where_clause, params = self._create_where_clause()
        self.query_params.extend(params)

        limit_statement, params = self._create_limit()
        self.query_params.extend(params)

        self.query = sql.SQL(" ").join(
            [
                geojson_statement,
                select_statement,
                from_statement,
                join_statement,
                where_clause,
                limit_statement,
                sql.SQL(") AS geojson;"),
            ]
        )

        self.query_params = tuple(self.query_params)

        return self.query, self.query_params
