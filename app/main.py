# TBD: real API
from fastapi import FastAPI

app = FastAPI(title="Photo API (skeleton)")

@app.get("/healthz")
def healthz():
    return {"status": "ok", "version": "skeleton"}

@app.get("/")
def root():
    return {"message": "TBD"}
