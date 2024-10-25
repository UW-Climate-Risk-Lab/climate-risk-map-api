from fastapi import FastAPI
from mangum import Mangum

from routers import data

app = FastAPI(
    title="Climate Risk Data API",
    description="An API for accessing climate risk data.",
    version="0.1.0"
)

app.include_router(data.data_router)

# AWS Lambda handler
handler = Mangum(app)