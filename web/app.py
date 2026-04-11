from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from web.api import router

app = FastAPI(title="OCSASim")
app.include_router(router)


@app.get("/builder", include_in_schema=False)
async def builder_page():
    return FileResponse("web/static/builder.html")


app.mount("/", StaticFiles(directory="web/static", html=True), name="static")
