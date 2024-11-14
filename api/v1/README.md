# climate-risk-map-api

A repository for a data API that allows access to physical asset and climate risk data curated by the Climate Risk Lab.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [API Endpoints](#api-endpoints)
  - [Get Data](#get-data)
  - [Get Climate Metadata](#get-climate-metadata)
- [Testing](#testing)
- [License](#license)
- [Contact](#contact)

## Features

- Access physical asset and climate risk data.
- Supports GeoJSON and CSV response formats.
- Flexible querying with various filters like category, OSM types, bounding boxes, and climate variables.
- Designed for deployment on AWS Lambda using Mangum.

## Prerequisites

- Python 3.12
- [Poetry](https://python-poetry.org/) for dependency management
- PostgreSQL database with PostGIS extension
- AWS account (if deploying to AWS Lambda)

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/yourusername/climate-risk-map-api.git
   cd climate-risk-map-api/api
   ```

2. **Install dependencies using Poetry**

   ```bash
   poetry install
   ```

3. **Set up environment variables**

   Create a `.env` file in the root directory and add the necessary environment variables:

   ```env
    PG_DBNAME=pgosm_flex_washington
    PG_USER=osm_ro_user
    PG_PASSWORD=mysecretpassword
    PG_HOST=localhost
   ```

## Usage

### Running Locally

1. **Activate the virtual environment**

   ```bash
   poetry shell
   ```

2. **Run the FastAPI application**

   ```bash
   uvicorn app.main:app --reload
   ```

   The API will be accessible at `http://127.0.0.1:8000`.

### Deploying to AWS Lambda

1. **Build the deployment package**

   Use AWS SAM `template.yml`

2. **Deploy using AWS CLI or your preferred deployment tool**

## API Endpoints

### Get Data

Retrieve data based on various parameters.

- **Endpoint**

  ```
  GET /api/v1/data/{response_format}/{osm_category}/{osm_type}/
  ```

- **Parameters**

  | Name               | Type     | Description                                                   |
  | ------------------ | -------- | ------------------------------------------------------------- |
  | `response_format`  | String   | Format of the response (`geojson` or `csv`).                 |
  | `osm_category`         | String   | OSM Category to retrieve data from.                           |
  | `osm_type`         | String   | OSM Type to filter on.                                        |
  | `osm_subtypes`     | List&lt;String&gt; | (Optional) OSM Subtypes to filter on.                      |
  | `bbox`             | List&lt;String&gt; | (Optional) Bounding box in JSON format (*bbox={"xmin": -126.0, "xmax": -119.0, "ymin": 46.1, "ymax": 47.2}*).                 |
  | `county`           | Boolean  | (Optional) If true, includes county information.              |
  | `city`             | Boolean  | (Optional) If true, includes city information.                |
  | `epsg_code`        | Integer  | (Optional) Spatial reference ID. Default is `4326`.           |
  | `geom_type`        | String   | (Optional) Filter by geometry type.                           |
  | `climate_variable` | String   | (Optional) Climate variable to filter on.                     |
  | `climate_ssp`      | Integer  | (Optional) Climate SSP to filter on.                           |
  | `climate_month`    | List&lt;Integer&gt; | (Optional) List of months to filter on.                    |
  | `climate_decade`   | List&lt;Integer&gt; | (Optional) List of decades to filter on.                   |
  | `climate_metadata` | Boolean  | (Optional) If true, includes climate metadata.                |
  | `limit`            | Integer  | (Optional) Limit the number of results.                        |

- **Example Request**

  ```bash
  curl -X GET http://127.0.0.1:8000/api/v1/data/geojson/infrastructure/power/?osm_subtypes=plant&osm_subtypes=line&bbox={"xmin":-126.0,"xmax":-119.0,"ymin":46.1,"ymax":47.2}
  ```

- **Example Response**

  ```json
  {
      "type": "FeatureCollection",
      "features": [
          {
              "type": "Feature",
              "geometry": { ... },
              "properties": { ... }
          },
          ...
      ]
  }
  ```

### Get Climate Metadata

Retrieve metadata for a specific climate variable and SSP.

- **Endpoint**

  ```
  GET /api/v1/climate-metadata/{climate_variable}/{ssp}
  ```

- **Parameters**

  | Name              | Type   | Description                       |
  | ----------------- | ------ | --------------------------------- |
  | `climate_variable` | String | Name of the climate variable.     |
  | `ssp`             | String | SSP number.                        |

- **Example Request**

  ```bash
  curl -X GET "http://127.0.0.1:8000/api/v1/climate-metadata/burntFractionAll/585"
  ```

- **Example Response**

  ```json
  {
      "variable": "temperature",
      "ssp": "ssp5",
      "metadata": { ... }
  }
  ```

## Testing

Run the unit tests using pytest:

```bash
pytest
```

Ensure that all tests pass before deploying or making significant changes.

## License

This project is licensed under the CC-BY-SA-4.0 license.

## Contact

For any inquiries or support, please contact [<ecol07@uw.edu>](mailto:ecol07@uw.edu).