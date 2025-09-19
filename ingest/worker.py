import os, time, hashlib, uuid, io
from datetime import datetime, timezone
from pathlib import Path
from PIL import Image, ExifTags
import pillow_heif, imagehash, clickhouse_connect
from minio import Minio

PHOTOS_ROOT = os.getenv("PHOTOS_ROOT", "/photos")
CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "app")
CH_PASSWORD = os.getenv("CH_PASSWORD", "supersecret")
CH_DATABASE = os.getenv("CH_DATABASE", "photos")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "photo-thumbs")

client = clickhouse_connect.get_client(
    host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DATABASE
)
s3 = Minio(MINIO_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)

try:
    if not s3.bucket_exists(MINIO_BUCKET):
        s3.make_bucket(MINIO_BUCKET)
except Exception as e:
    print("[WARN] bucket check:", e)

EXIF_TAGS = {v: k for k, v in ExifTags.TAGS.items()}
NS = uuid.UUID("c3b9e6c0-3f55-4b3c-9fd8-6db8a6b7e001")
EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif"}

def file_id_for(p: Path, st) -> uuid.UUID:
    return uuid.uuid5(NS, f"{p.relative_to(PHOTOS_ROOT)}|{st.st_size}|{int(st.st_mtime)}")

def sha1_of_file(p: Path) -> str:
    h = hashlib.sha1()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def safe(s):  # coalesce None -> ""
    return s if s is not None else ""

def extract_exif(path: Path):
    data = {
        "width": 0, "height": 0, "orientation": "",
        "camera_make": "", "camera_model": "", "lens_model": "",
        "iso": 0, "f_number": 0.0, "exposure_time": "", "focal_length_mm": 0.0,
        "taken_at_utc": None, "phash": ""
    }
    try:
        with Image.open(path) as img:
            data["width"], data["height"] = img.size
            exif = img.getexif()
            if exif:
                dto = exif.get(EXIF_TAGS.get("DateTimeOriginal"))
                if dto:
                    try:
                        dt = datetime.strptime(dto, "%Y:%m:%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        data["taken_at_utc"] = dt.replace(tzinfo=None)
                    except Exception:
                        pass
                data["camera_make"] = safe(exif.get(EXIF_TAGS.get("Make")))
                data["camera_model"] = safe(exif.get(EXIF_TAGS.get("Model")))
                data["lens_model"] = safe(exif.get(EXIF_TAGS.get("LensModel")))
                ori = exif.get(EXIF_TAGS.get("Orientation"))
                data["orientation"] = str(ori) if ori else ""
            try:
                data["phash"] = str(imagehash.phash(img))
            except Exception:
                data["phash"] = ""
    except Exception as e:
        print("[WARN] EXIF", path, e)
    return data

def upload_thumbnail(path: Path, key: str, side=2048) -> bool:
    try:
        with Image.open(path) as img:
            img.thumbnail((side, side))
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=85, optimize=True)
            bs = buf.getvalue()
            s3.put_object(MINIO_BUCKET, key, io.BytesIO(bs), length=len(bs), content_type="image/jpeg")
            return True
    except Exception as e:
        print("[WARN] thumb", path, e)
        return False

def insert_row(row: dict):
    cols = [
        "file_id","path","relpath","size_bytes","mtime","ctime","sha1","phash","mime",
        "width","height","orientation","camera_make","camera_model","lens_model",
        "iso","f_number","exposure_time","focal_length_mm","taken_at_utc","tz_offset_minutes",
        "gps_lat","gps_lon","gps_alt_m","subjects","rating","description","ingestion_run_id"
    ]
    client.insert("raw_files", [[row.get(c) for c in cols]], column_names=cols)

def process_file(p: Path, run_id: str):
    st = p.stat()
    fid = str(file_id_for(p, st))
    rel = str(p.relative_to(PHOTOS_ROOT))
    exif = extract_exif(p)
    sha1 = sha1_of_file(p)
    mime = {
        ".jpg":"image/jpeg",".jpeg":"image/jpeg",".png":"image/png",".heic":"image/heic",".heif":"image/heif"
    }.get(p.suffix.lower(), "application/octet-stream")

    upload_thumbnail(p, f"{fid}.jpg")
    row = dict(
        file_id=fid,
        path=str(p),
        relpath=rel,
        size_bytes=st.st_size,
        mtime=datetime.fromtimestamp(st.st_mtime),
        ctime=datetime.fromtimestamp(st.st_ctime),
        sha1=sha1,
        phash=safe(exif["phash"]),
        mime=safe(mime),
        width=exif["width"],
        height=exif["height"],
        orientation=safe(exif["orientation"]),
        camera_make=safe(exif["camera_make"]),
        camera_model=safe(exif["camera_model"]),
        lens_model=safe(exif["lens_model"]),
        iso=exif.get("iso", 0),
        f_number=exif.get("f_number", 0.0),
        exposure_time=safe(exif.get("exposure_time")),
        focal_length_mm=exif.get("focal_length_mm", 0.0),
        taken_at_utc=exif.get("taken_at_utc") or datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).replace(tzinfo=None),
        tz_offset_minutes=0,
        gps_lat=None, gps_lon=None, gps_alt_m=None,
        subjects=[], rating=None, description="",
        ingestion_run_id=run_id
    )
    insert_row(row)

def main():
    while True:
        run_id = datetime.utcnow().strftime("run%Y%m%d%H%M%S")
        print("[INFO] scan start", run_id)
        for p in Path(PHOTOS_ROOT).rglob("*"):
            if p.is_file() and p.suffix.lower() in EXTS:
                try:
                    process_file(p, run_id)
                except Exception as e:
                    print("[ERR]", p, e)
        print("[INFO] scan done", run_id)
        time.sleep(600)  # 10 min
if __name__ == "__main__":
    main()
