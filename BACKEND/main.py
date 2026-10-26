from fastapi import FastAPI

app = FastAPI(title="AIoT-CORE API")


@app.get("/")
def root():
    return {
        "project": "AIoT-CORE",
        "status": "running"
    }


@app.get("/health")
def health():
    return {
        "api": "ok"
    }
