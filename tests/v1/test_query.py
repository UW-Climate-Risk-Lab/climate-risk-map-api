from unittest.mock import MagicMock, patch

import pytest
from geojson_pydantic import FeatureCollection
from psycopg2.sql import SQL, Composed, Identifier

from app.v1 import api, query

TEST_BBOX = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "type": "rectangle",
                "_bounds": [
                    {"lat": 47.61402337357123, "lng": -119.32662963867189},
                    {"lat": 47.62651702078168, "lng": -119.27650451660158},
                ],
                "_leaflet_id": 11228,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-119.32662963867189, 47.61402337357123],
                        [-119.32662963867189, 47.62651702078168],
                        [-119.27650451660158, 47.62651702078168],
                        [-119.27650451660158, 47.61402337357123],
                        [-119.32662963867189, 47.61402337357123],
                    ]
                ],
            },
        },
        {
            "type": "Feature",
            "properties": {
                "type": "rectangle",
                "_bounds": [
                    {"lat": 47.49541671416695, "lng": -119.30191040039064},
                    {"lat": 47.50747495167563, "lng": -119.27444458007814},
                ],
                "_leaflet_id": 11242,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-119.30191040039064, 47.49541671416695],
                        [-119.30191040039064, 47.50747495167563],
                        [-119.27444458007814, 47.50747495167563],
                        [-119.27444458007814, 47.49541671416695],
                        [-119.30191040039064, 47.49541671416695],
                    ]
                ],
            },
        },
    ],
}


@pytest.mark.parametrize(
    "input_params, expected_select_statement, expected_params",
    [
        # Select with climate arguments
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            Composed(
                [
                    SQL("SELECT "),
                    Composed(
                        [
                            Identifier("osm", "infrastructure", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_type"),
                            SQL(", "),
                            Identifier("osm", "tags", "tags"),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s) AS geometry"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_AsText(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s), 3) AS geometry_wkt"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_X(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS longitude"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Y(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_subtype"),
                            SQL(", "),
                            Composed(
                                [Identifier("county"), SQL(".name AS county_name")]
                            ),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city_name")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".ssp")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".month")]),
                            SQL(", "),
                            Composed([Identifier("climate_data"), SQL(".decade")]),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".variable AS climate_variable"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("climate_data"),
                                    SQL(".value AS climate_exposure"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [Identifier("climate_data"), SQL(".climate_metadata")]
                            ),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
        # Climate query test case 1 - Any climate argument that is None will result in no climate columns returned
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=True,
            ),
            Composed(
                [
                    SQL("SELECT "),
                    Composed(
                        [
                            Identifier("osm", "infrastructure", "osm_id"),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_type"),
                            SQL(", "),
                            Identifier("osm", "tags", "tags"),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s) AS geometry"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_AsText(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s), 3) AS geometry_wkt"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_X(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS longitude"),
                                ]
                            ),
                            SQL(", "),
                            Composed(
                                [
                                    SQL("ST_Y(ST_Centroid(ST_Transform("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", %s))) AS latitude"),
                                ]
                            ),
                            SQL(", "),
                            Identifier("osm", "infrastructure", "osm_subtype"),
                            SQL(", "),
                            Composed(
                                [
                                    Identifier("county"),
                                    SQL(".name AS county_name"),
                                ]
                            ),
                            SQL(", "),
                            Composed([Identifier("city"), SQL(".name AS city_name")]),
                        ]
                    ),
                ]
            ),
            [4326, 4326, 4326, 4326],
        ),
    ],
)
def test_create_select_statement(
    input_params, expected_select_statement, expected_params
):

    query_builder = query.CRLQuery(
        category=input_params["category"],
        osm_types=input_params["osm_types"],
        osm_subtypes=input_params["osm_subtypes"],
        county=input_params["county"],
        city=input_params["city"],
        epsg_code=input_params["epsg_code"],
        climate_variable=input_params["climate_variable"],
        climate_decade=input_params["climate_decade"],
        climate_month=input_params["climate_month"],
        climate_ssp=input_params["climate_ssp"],
        climate_metadata=input_params["climate_metadata"],
    )

    generated_select_statement = query_builder._create_select_statement()
    generated_params = query_builder.params

    assert generated_select_statement == expected_select_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_from_statement",
    [
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            Composed(
                [
                    SQL("FROM "),
                    Identifier("osm"),
                    SQL("."),
                    Identifier("infrastructure"),
                ]
            ),
        )
    ],
)
def test_create_from_statement(input_params, expected_from_statement):

    query_builder = query.CRLQuery(
        category=input_params["category"],
        osm_types=input_params["osm_types"],
        osm_subtypes=input_params["osm_subtypes"],
        county=input_params["county"],
        city=input_params["city"],
        epsg_code=input_params["epsg_code"],
        climate_variable=input_params["climate_variable"],
        climate_decade=input_params["climate_decade"],
        climate_month=input_params["climate_month"],
        climate_ssp=input_params["climate_ssp"],
        climate_metadata=input_params["climate_metadata"],
    )

    generated_from_statement = query_builder._create_from_statement()

    assert generated_from_statement == expected_from_statement


@pytest.mark.parametrize(
    "input_params, expected_join_statement, expected_params",
    [
        # Test case with all possible input params
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            Composed(
                [
                    Composed(
                        [
                            Composed(
                                [
                                    Composed(
                                        [
                                            SQL("JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(" ON "),
                                            Identifier("osm"),
                                            SQL("."),
                                            SQL("infrastructure"),
                                            SQL(".osm_id = "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("tags"),
                                            SQL(".osm_id"),
                                        ]
                                    ),
                                    SQL(" "),
                                    Composed(
                                        [
                                            SQL("LEFT JOIN "),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("place_polygon"),
                                            SQL(" "),
                                            Identifier("county"),
                                            SQL("ON ST_Intersects("),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("infrastructure"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(", "),
                                            Identifier("county"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(") AND "),
                                            Identifier("county"),
                                            SQL(".admin_level = %s "),
                                        ]
                                    ),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("place_polygon"),
                                    SQL(" "),
                                    Identifier("city"),
                                    SQL("ON ST_Intersects("),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(", "),
                                    Identifier("city"),
                                    SQL("."),
                                    Identifier("geom"),
                                    SQL(") AND "),
                                    Identifier("city"),
                                    SQL(".admin_level = %s "),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            SQL("LEFT JOIN ("),
                            SQL(
                                "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("scenariomip"),
                                    SQL(" s "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("scenariomip_variables"),
                                    SQL(" v "),
                                ]
                            ),
                            SQL("ON s.variable_id = v.id "),
                            SQL(
                                "WHERE v.ssp = %s AND v.variable = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            Composed(
                                [
                                    SQL(") AS "),
                                    Identifier("climate_data"),
                                    SQL(" "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_data"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [6, 8, 126, "burntFractionAll", (2060, 2070), (8, 9)],
        ),
        # Test case with no city and no count
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
            ),
            Composed(
                [
                    Composed(
                        [
                            SQL("JOIN "),
                            Identifier("osm"),
                            SQL("."),
                            Identifier("tags"),
                            SQL(" ON "),
                            Identifier("osm"),
                            SQL("."),
                            SQL("infrastructure"),
                            SQL(".osm_id = "),
                            Identifier("osm"),
                            SQL("."),
                            Identifier("tags"),
                            SQL(".osm_id"),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            SQL("LEFT JOIN ("),
                            SQL(
                                "SELECT s.osm_id, v.ssp, v.variable, s.month, s.decade, s.value, v.metadata AS climate_metadata "
                            ),
                            Composed(
                                [
                                    SQL("FROM "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("scenariomip"),
                                    SQL(" s "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("LEFT JOIN "),
                                    Identifier("climate"),
                                    SQL("."),
                                    Identifier("scenariomip_variables"),
                                    SQL(" v "),
                                ]
                            ),
                            SQL("ON s.variable_id = v.id "),
                            SQL(
                                "WHERE v.ssp = %s AND v.variable = %s AND s.decade IN %s AND s.month IN %s"
                            ),
                            Composed(
                                [
                                    SQL(") AS "),
                                    Identifier("climate_data"),
                                    SQL(" "),
                                ]
                            ),
                            Composed(
                                [
                                    SQL("ON "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL(".osm_id = "),
                                    Identifier("climate_data"),
                                    SQL(".osm_id"),
                                ]
                            ),
                        ]
                    ),
                ]
            ),
            [126, "burntFractionAll", (2060, 2070), (8, 9)],
        ),
        # Test case no climate
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=False,
                city=False,
                epsg_code=4326,
                climate_variable=None,
                climate_decade=None,
                climate_month=None,
                climate_ssp=None,
                climate_metadata=False,
            ),
            Composed(
                [
                    SQL("JOIN "),
                    Identifier("osm"),
                    SQL("."),
                    Identifier("tags"),
                    SQL(" ON "),
                    Identifier("osm"),
                    SQL("."),
                    SQL("infrastructure"),
                    SQL(".osm_id = "),
                    Identifier("osm"),
                    SQL("."),
                    Identifier("tags"),
                    SQL(".osm_id"),
                ]
            ),
            [],
        ),
    ],
)
def test_create_join_statement(
    input_params, expected_join_statement, expected_params
):

    query_builder = query.CRLQuery(
        category=input_params["category"],
        osm_types=input_params["osm_types"],
        osm_subtypes=input_params["osm_subtypes"],
        county=input_params["county"],
        city=input_params["city"],
        epsg_code=input_params["epsg_code"],
        climate_variable=input_params["climate_variable"],
        climate_decade=input_params["climate_decade"],
        climate_month=input_params["climate_month"],
        climate_ssp=input_params["climate_ssp"],
        climate_metadata=input_params["climate_metadata"],
    )
    generated_join_statement = query_builder._create_join_statement()
    generated_params = query_builder.params

    assert generated_join_statement == expected_join_statement
    assert generated_params == expected_params


@pytest.mark.parametrize(
    "input_params, expected_where_clause, expected_params",
    [
        (
            dict(
                category="infrastructure",
                osm_types=["power"],
                osm_subtypes=["line"],
                county=True,
                city=True,
                epsg_code=4326,
                climate_variable="burntFractionAll",
                climate_decade=[2060, 2070],
                climate_month=[8, 9],
                climate_ssp=126,
                climate_metadata=True,
                bbox=FeatureCollection(type=TEST_BBOX["type"], features=TEST_BBOX["features"]),
            ),
            Composed(
                [
                    Composed(
                        [
                            Composed(
                                [
                                    SQL("WHERE "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("osm_type"),
                                    SQL(" IN %s"),
                                ]
                            ),
                            SQL(" "),
                            Composed(
                                [
                                    SQL("AND "),
                                    Identifier("osm"),
                                    SQL("."),
                                    Identifier("infrastructure"),
                                    SQL("."),
                                    Identifier("osm_subtype"),
                                    SQL(" IN %s"),
                                ]
                            ),
                        ]
                    ),
                    SQL(" "),
                    Composed(
                        [
                            Composed(
                                [
                                    Composed(
                                        [
                                            Composed(
                                                [
                                                    SQL("AND ("),
                                                    SQL(" "),
                                                    Composed(
                                                        [
                                                            SQL(
                                                                "ST_Intersects(ST_Transform("
                                                            ),
                                                            Identifier("osm"),
                                                            SQL("."),
                                                            Identifier(
                                                                "infrastructure"
                                                            ),
                                                            SQL("."),
                                                            Identifier("geom"),
                                                            SQL(
                                                                ", %s), ST_GeomFromText(%s, %s))"
                                                            ),
                                                        ]
                                                    ),
                                                ]
                                            ),
                                            SQL(" "),
                                            SQL("OR"),
                                        ]
                                    ),
                                    SQL(" "),
                                    Composed(
                                        [
                                            SQL("ST_Intersects(ST_Transform("),
                                            Identifier("osm"),
                                            SQL("."),
                                            Identifier("infrastructure"),
                                            SQL("."),
                                            Identifier("geom"),
                                            SQL(", %s), ST_GeomFromText(%s, %s))"),
                                        ]
                                    ),
                                ]
                            ),
                            SQL(" "),
                            SQL(")"),
                        ]
                    ),
                ]
            ),
            [
                ("power",),
                ("line",),
                4326,
                "POLYGON ((-119.32662963867189 47.61402337357123, -119.32662963867189 47.62651702078168, -119.27650451660158 47.62651702078168, -119.27650451660158 47.61402337357123, -119.32662963867189 47.61402337357123))",
                4326,
                4326,
                "POLYGON ((-119.30191040039064 47.49541671416695, -119.30191040039064 47.50747495167563, -119.27444458007814 47.50747495167563, -119.27444458007814 47.49541671416695, -119.30191040039064 47.49541671416695))",
                4326,
            ],
        )
    ],
)
def test_create_where_clause(
    input_params, expected_where_clause, expected_params
):
    query_builder = query.CRLQuery(
        category=input_params["category"],
        osm_types=input_params["osm_types"],
        osm_subtypes=input_params["osm_subtypes"],
        county=input_params["county"],
        city=input_params["city"],
        epsg_code=input_params["epsg_code"],
        bbox=input_params["bbox"],
        climate_variable=input_params["climate_variable"],
        climate_decade=input_params["climate_decade"],
        climate_month=input_params["climate_month"],
        climate_ssp=input_params["climate_ssp"],
        climate_metadata=input_params["climate_metadata"],
    )
    generated_where_clause = query_builder._create_where_clause()
    generated_params = query_builder.params

    assert generated_where_clause == expected_where_clause
    assert generated_params == expected_params
