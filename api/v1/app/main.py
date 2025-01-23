from fastapi import FastAPI
from mangum import Mangum

from . import api

app = FastAPI(
    title="Climate Risk Data API",
    description="An API for accessing climate risk data.",
    version="0.1.0"
)

app.include_router(api.router)

@app.get("/")
def root():
    return {"message": "Hello World"}

# AWS Lambda handler
handler = Mangum(app)