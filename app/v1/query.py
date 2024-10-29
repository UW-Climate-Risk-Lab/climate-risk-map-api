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
        self.primary_table = self.input_params.category if self.input_params.category in config.OSM_AVAILABLE_CATEGORIES.keys() else None
        if self.primary_table is None:
            raise ValueError(f"The category {self.input_params.category} is not currently available!")
        

        if not config.OSM_AVAILABLE_CATEGORIES[self.primary_table]["has_subtypes"]:
            self.osm_subtypes = None

    def _create_select_statement(self) -> Tuple[sql.SQL, List[Any]]:
        """Bulids a dynamic SQL SELECT statement for the get_osm_data method

        NOTE, we use ST_Centroid() to get lat/lon values for non-point shapes.
        The lat/lon returned is not guarenteed to be on the shape itself, and represents the
        geometric center of mass of the shape. This should be fine for polygons and points,
        but may be meaningless for long linestrings.
        Aleternative methods may be ST_PointOnSurface() or ST_LineInterpolatePoint(), however
        these are more computationally expensive and for now not worth the implementation.

        """

        # Initial list of fields that are always returned
        select_fields = [
            sql.Identifier(config.OSM_SCHEMA_NAME, self.primary_table, "osm_id"),
            sql.Identifier(config.OSM_SCHEMA_NAME, self.primary_table, "osm_type"),
            sql.Identifier(config.OSM_SCHEMA_NAME, config.OSM_TABLE_TAGS, "tags"),
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
        self.params.extend([self.epsg_code] * 4)

        # Add extra where clause for subtypes if they are specified
        if self.osm_subtypes:
            select_fields.append(
                sql.Identifier(
                    config.OSM_SCHEMA_NAME, self.primary_table, "osm_subtype"
                )
            )

        # County and City tables are aliased in the _create_join_method()
        if self.county:
            conditions = self._create_admin_table_conditions("county")
            county_field = sql.SQL("{admin_table_alias}.name AS county_name").format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                admin_table_alias=sql.Identifier(conditions["alias"]),
            )
            select_fields.append(county_field)

        if self.city:
            conditions = self._create_admin_table_conditions("city")
            city_field = sql.SQL("{admin_table_alias}.name AS city_name").format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                admin_table_alias=sql.Identifier(conditions["alias"]),
            )
            select_fields.append(city_field)

        if (
            self.climate_variable
            and self.climate_ssp
            and self.climate_month
            and self.climate_decade
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
            if self.climate_metadata:
                select_fields.append(
                    sql.SQL("{climate_table_alias}.climate_metadata").format(
                        climate_schema=sql.Identifier(config.CLIMATE_SCHEMA_NAME),
                        climate_table_alias=sql.Identifier(config.CLIMATE_TABLE_ALIAS),
                    )
                )

        select_statement = sql.SQL("SELECT {columns}").format(
            columns=sql.SQL(", ").join(select_fields)
        )
        self.select_statement = select_statement
        return select_statement

    def _create_from_statement(self) -> sql.SQL:

        from_statement = sql.SQL("FROM {schema}.{table}").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            table=sql.Identifier(self.primary_table),
        )
        return from_statement

    def _create_join_statement(self) -> Tuple[sql.SQL, List[Any]]:

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
        if self.county:
            admin_conditions.append(self._create_admin_table_conditions("county"))
        if self.city:
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
            self.params.append(admin["level"])
            join_statement = sql.SQL(" ").join([join_statement, admin_join])

        if (
            self.climate_variable
            and self.climate_ssp
            and self.climate_month
            and self.climate_decade
        ):
            climate_join = sql.Composed(
                [
                    sql.SQL("LEFT JOIN ("),
                    sql.SQL(
                        "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata "
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
            self.params += [
                self.climate_ssp,
                self.climate_variable,
                tuple(set(self.climate_decade)),
                tuple(set(self.climate_month)),
            ]

            join_statement = sql.SQL(" ").join([join_statement, climate_join])

        self.join_statement = join_statement
        return join_statement

    def _create_where_clause(self) -> Tuple[sql.SQL, List[Any]]:

        # Always filter by osm type to throttle data output!
        where_clause = sql.SQL("WHERE {schema}.{primary_table}.{column} IN %s").format(
            schema=sql.Identifier(config.OSM_SCHEMA_NAME),
            primary_table=sql.Identifier(self.primary_table),
            column=sql.Identifier("osm_type"),
        )
        self.params.append(tuple(self.osm_types))

        if self.osm_subtypes:
            subtype_clause = sql.SQL(
                "AND {schema}.{primary_table}.{column} IN %s"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                primary_table=sql.Identifier(self.primary_table),
                column=sql.Identifier("osm_subtype"),
            )
            self.params.append(tuple(self.osm_subtypes))
            where_clause = sql.SQL(" ").join([where_clause, subtype_clause])

        if self.geom_type:
            geom_type_clause = sql.SQL(
                "AND {schema}.{primary_table}.geom_type = %s"
            ).format(
                schema=sql.Identifier(config.OSM_SCHEMA_NAME),
                primary_table=sql.Identifier(self.primary_table),
            )
            self.params.append("ST_" + self.geom_type)
            where_clause = sql.SQL(" ").join([where_clause, geom_type_clause])

        # If a bounding box GeoJSON is passed in, use as filter
        if self.bbox:
            bbox_filter = sql.SQL("AND (")
            count = 0
            # Handles multiple bounding boxes drawn by user
            for feature in self.bbox.features:
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
                self.params.append(self.epsg_code)
                self.params.append(feature.geometry.wkt)
                self.params.append(self.epsg_code)
                bbox_filter = sql.SQL(" ").join([bbox_filter, feature_filter])
                count += 1
            bbox_filter = sql.SQL(" ").join([bbox_filter, sql.SQL(")")])

            where_clause = sql.SQL(" ").join([where_clause, bbox_filter])

        self.where_clause = where_clause
        return where_clause

    def _create_admin_table_conditions(self, condition: str) -> Dict:

        admin_conditions = {
            "condition": condition,
            "level": config.OSM_TABLE_PLACES_ADMIN_LEVELS[condition],
            "alias": condition,
        }

        return admin_conditions

    def build_query(self) -> sql.Composable:

        self.params = list()

        geojson_statement = sql.SQL(
            """ 
        SELECT json_build_object(
            'type', 'FeatureCollection',
            'features', json_agg(ST_AsGeoJSON(geojson.*)::json))
            
            FROM (
        """
        )

        select_statement = self._create_select_statement()
        from_statement = self._create_from_statement()
        join_statement = self._create_join_statement()
        where_clause = self._create_where_clause()

        self.query = sql.SQL(" ").join(
            [
                geojson_statement,
                select_statement,
                from_statement,
                join_statement,
                where_clause,
                sql.SQL(") AS geojson;"),
            ]
        )

        self.params = tuple(self.params)

        return self.query



