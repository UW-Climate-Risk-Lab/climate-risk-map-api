from fastapi import FastAPI
from mangum import Mangum

import app.v1.api as v1

app = FastAPI(
    title="Climate Risk Data API",
    description="An API for accessing climate risk data.",
    version="0.1.0"
)

app.include_router(v1.router, prefix="/api/v1")

# AWS Lambda handler
handler = Mangum(app)