from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response
import base64, csv, io, zipfile, os

API_KEY = os.getenv("API_KEY", "set-a-strong-secret")

app = FastAPI(title="File Transform API", version="1.0")

def is_success(row: dict) -> bool:
    try:
        return float(row.get("amount","")) >= 0
    except Exception:
        return False

def parse_csv(b: bytes):
    text = b.decode("utf-8", errors="replace")
    r = csv.DictReader(io.StringIO(text))
    return [dict(x) for x in r], r.fieldnames or []

def build_csv(headers, rows):
    s = io.StringIO()
    w = csv.DictWriter(s, fieldnames=headers, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return s.getvalue()

@app.get("/")
async def root():
    return {"ok": True, "message": "POST /process with JSON and header X-API-Key"}

@app.post("/process")
async def process(request: Request):
    key = request.headers.get("X-API-Key","")
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.json()
    files = body.get("files", [])
    if not files:
        raise HTTPException(status_code=400, detail="Provide at least one file.")

    all_rows, headers_master, seen = [], [], set()
    for f in files:
        b64 = f.get("content_base64") or f.get("contentBase64")
        if not b64:
            raise HTTPException(status_code=400, detail="content_base64 missing")
        rows, headers = parse_csv(base64.b64decode(b64))
        for h in headers:
            if h not in seen:
                seen.add(h); headers_master.append(h)
        for r in rows:
            r["_source_file"] = f.get("filename","")
        all_rows += rows
    if "_source_file" not in seen:
        headers_master.append("_source_file")

    success = [r for r in all_rows if is_success(r)]
    failure = [r for r in all_rows if not is_success(r)]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("Success.csv", build_csv(headers_master, success))
        z.writestr("Failure.csv", build_csv(headers_master, failure))
    buf.seek(0)

    return Response(
        content=buf.read(),
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="results.zip"'}
    )
