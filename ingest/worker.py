import os, time, hashlib, uuid, io
from datetime import datetime, timezone
from pathlib import Path
from datetime import timedelta


from PIL import Image, ExifTags, ImageOps, ImageFile
from PIL.ExifTags import GPSTAGS
ImageFile.LOAD_TRUNCATED_IMAGES = True  # tolerate partial JPEGs

import pillow_heif
pillow_heif.register_heif_opener()      # enable .heic/.heif

import imagehash, clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError
from minio import Minio

# ----------------------------
# Configuration
# ----------------------------
PHOTOS_ROOT = os.getenv("PHOTOS_ROOT") or "/photos"   # works even if env is set but empty

CH_HOST = os.getenv("CH_HOST", "clickhouse")
CH_PORT = int(os.getenv("CH_PORT", "8123"))
CH_USER = os.getenv("CH_USER", "app")
CH_PASSWORD = os.getenv("CH_PASSWORD", "supersecret")
CH_DATABASE = os.getenv("CH_DATABASE", "photos")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "admin12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "photo-thumbs")

# Extensions
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif"}
RAW_EXTS   = {".raf"}                  # extend later (".cr2",".arw",".dng",...)
EXTS       = IMAGE_EXTS | RAW_EXTS

# ClickHouse client (lazy init with retry)
_client = None
def get_client():
    global _client
    if _client:
        return _client
    for _ in range(30):  # ~30s
        try:
            _client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT,
                username=CH_USER, password=CH_PASSWORD,
                database=CH_DATABASE,
            )
            _client.query("SELECT 1")
            return _client
        except Exception as e:
            print("[WARN] CH not ready, retrying...", e)
            time.sleep(1)
    raise RuntimeError("ClickHouse not reachable")

# MinIO client & bucket check
s3 = Minio(MINIO_ENDPOINT, access_key=MINIO_ROOT_USER, secret_key=MINIO_ROOT_PASSWORD, secure=False)
try:
    if not s3.bucket_exists(MINIO_BUCKET):
        s3.make_bucket(MINIO_BUCKET)
except Exception as e:
    print("[WARN] bucket check:", e)

# ----------------------------
# Helpers
# ----------------------------
EXIF_TAGS = {v: k for k, v in ExifTags.TAGS.items()}  # name -> tag id
NS = uuid.UUID("c3b9e6c0-3f55-4b3c-9fd8-6db8a6b7e001")

def file_id_for(p: Path, st) -> uuid.UUID:
    # Deterministic UUIDv5: relpath|size|mtime(sec)
    return uuid.uuid5(NS, f"{p.relative_to(PHOTOS_ROOT)}|{st.st_size}|{int(st.st_mtime)}")

def sha1_of_file(p: Path) -> str:
    h = hashlib.sha1()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def safe(s):  # coalesce None -> ""
    return s if s is not None else ""

def _frac_to_float(v):
    # EXIF rationals can be tuple(num, den) or PIL IFDRational
    try:
        if hasattr(v, 'numerator') and hasattr(v, 'denominator'):
            return float(v.numerator) / float(v.denominator or 1)
        if isinstance(v, tuple) and len(v) == 2:
            num, den = v
            return float(num) / float(den or 1)
        return float(v)
    except Exception:
        return 0.0

def _dms_to_deg(d, m, s, ref):
    deg = _frac_to_float(d) + _frac_to_float(m)/60.0 + _frac_to_float(s)/3600.0
    if ref in ('S', 'W'):
        deg = -deg
    return deg

def _parse_tz_offset(offset_str: str) -> int:
    """
    EXIF OffsetTimeOriginal example: '+02:00' or '-07:00'
    Return minutes (e.g., +120, -420)
    """
    try:
        s = str(offset_str).strip()
        sign = -1 if s.startswith('-') else 1
        hh, mm = s.strip('+').strip('-').split(':', 1)
        return sign * (int(hh)*60 + int(mm))
    except Exception:
        return 0

def extract_exif_raster(path: Path):
    """EXIF for JPEG/PNG/HEIC (things Pillow can open)"""
    data = {
        "width": 0, "height": 0, "orientation": "",
        "camera_make": "", "camera_model": "", "lens_model": "",
        "iso": 0, "f_number": 0.0, "exposure_time": "", "focal_length_mm": 0.0,
        "taken_at_utc": None, "phash": "",
        "_gps_lat": None, "_gps_lon": None, "_gps_alt": None,
        "_tz_offset_minutes": 0,
        "_city": "", "_state": "", "_country": ""
    }
    try:
        with Image.open(path) as im0:
            img = ImageOps.exif_transpose(im0)  # honor orientation
            data["width"], data["height"] = img.size

            exif = img.getexif()
            if exif:
                get = exif.get

                # DateTimeOriginal -> naive UTC
                dto = get(EXIF_TAGS.get("DateTimeOriginal"))
                if dto:
                    try:
                        dt = datetime.strptime(dto, "%Y:%m:%d %H:%M:%S")

                        tz_offset = data.get("_tz_offset_minutes", 0)

                        if tz_offset:
                            dt = dt - timedelta(minutes=tz_offset)

                        data["taken_at_utc"] = dt

                    except Exception:
                        pass

                data["camera_make"] = safe(get(EXIF_TAGS.get("Make")))
                data["camera_model"] = safe(get(EXIF_TAGS.get("Model")))
                data["lens_model"]  = safe(get(EXIF_TAGS.get("LensModel")))

                ori = get(EXIF_TAGS.get("Orientation"))
                data["orientation"] = str(ori) if ori else ""

                # ISO
                iso_tag = EXIF_TAGS.get("ISOSpeedRatings") or EXIF_TAGS.get("PhotographicSensitivity")
                if iso_tag:
                    try:
                        iso_val = get(iso_tag)
                        if isinstance(iso_val, (list, tuple)):
                            iso_val = iso_val[0]
                        data["iso"] = int(iso_val or 0)
                    except Exception:
                        pass

                # FNumber (fraction)
                fn_tag = EXIF_TAGS.get("FNumber")
                if fn_tag:
                    fn = get(fn_tag)
                    try:
                        if isinstance(fn, tuple):
                            num, den = fn
                            data["f_number"] = float(num) / float(den or 1)
                        elif fn:
                            data["f_number"] = float(fn)
                    except Exception:
                        pass

                # ExposureTime (store as "num/den" if fraction)
                et_tag = EXIF_TAGS.get("ExposureTime")
                if et_tag:
                    et = get(et_tag)
                    try:
                        if isinstance(et, tuple):
                            num, den = et
                            data["exposure_time"] = f"{num}/{den}"
                        elif et:
                            data["exposure_time"] = str(et)
                    except Exception:
                        pass

                # FocalLength (fraction)
                fl_tag = EXIF_TAGS.get("FocalLength")
                if fl_tag:
                    fl = get(fl_tag)
                    try:
                        if isinstance(fl, tuple):
                            num, den = fl
                            data["focal_length_mm"] = float(num) / float(den or 1)
                        elif fl:
                            data["focal_length_mm"] = float(fl)
                    except Exception:
                        pass

                # GPS
                gps_tag_id = EXIF_TAGS.get("GPSInfo")
                gps = get(gps_tag_id)
                if gps:
                    gps_data = {GPSTAGS.get(k, k): v for k, v in gps.items()}

                    lat = lon = alt = None
                    lat_ref = gps_data.get('GPSLatitudeRef')
                    lat_val = gps_data.get('GPSLatitude')
                    lon_ref = gps_data.get('GPSLongitudeRef')
                    lon_val = gps_data.get('GPSLongitude')
                    alt_ref = gps_data.get('GPSAltitudeRef')
                    alt_val = gps_data.get('GPSAltitude')

                    if lat_ref and lat_val and len(lat_val) == 3:
                        lat = _dms_to_deg(lat_val[0], lat_val[1], lat_val[2], lat_ref)

                    if lon_ref and lon_val and len(lon_val) == 3:
                        lon = _dms_to_deg(lon_val[0], lon_val[1], lon_val[2], lon_ref)

                    if alt_val is not None:
                        alt = _frac_to_float(alt_val)
                        if alt_ref in (1, b'\x01'):  # below sea level
                            alt = -abs(alt)

                    data["_gps_lat"] = lat
                    data["_gps_lon"] = lon
                    data["_gps_alt"] = alt

                # OffsetTimeOriginal -> tz offset minutes
                oto_tag = EXIF_TAGS.get("OffsetTimeOriginal")
                if oto_tag:
                    raw = get(oto_tag)
                    if raw:
                        data["_tz_offset_minutes"] = _parse_tz_offset(str(raw))

                # Text location tags (if present; not always populated)
                city_tag    = EXIF_TAGS.get("City")     or 0
                state_tag   = EXIF_TAGS.get("State")    or 0
                country_tag = EXIF_TAGS.get("Country")  or 0

                if city_tag:    data["_city"]    = safe(get(city_tag))
                if state_tag:   data["_state"]   = safe(get(state_tag))
                if country_tag: data["_country"] = safe(get(country_tag))

            # Perceptual hash on oriented image
            try:
                data["phash"] = str(imagehash.phash(img))
            except Exception:
                data["phash"] = ""
    except Exception as e:
        print("[WARN] EXIF", path, e)
    return data

def minimal_exif_raw(path: Path):
    """Minimal fields for RAW files (no Pillow/RAW decode)"""
    st = path.stat()
    return {
        "width": 0, "height": 0, "orientation": "",
        "camera_make": "Fujifilm" if path.suffix.lower() == ".raf" else "",
        "camera_model": "", "lens_model": "",
        "iso": 0, "f_number": 0.0, "exposure_time": "", "focal_length_mm": 0.0,
        "taken_at_utc": datetime.utcfromtimestamp(st.st_mtime),
        "phash": "",
        "_gps_lat": None, "_gps_lon": None, "_gps_alt": None,
        "_tz_offset_minutes": 0,
        "_city": "", "_state": "", "_country": ""
    }

def upload_thumbnail(path: Path, key: str, side=2048) -> bool:
    try:
        with Image.open(path) as im0:
            img = ImageOps.exif_transpose(im0)
            img.thumbnail((side, side))
            buf = io.BytesIO()
            img.convert("RGB").save(buf, format="JPEG", quality=85, optimize=True)
            bs = buf.getvalue()
            s3.put_object(MINIO_BUCKET, key, io.BytesIO(bs), length=len(bs), content_type="image/jpeg")
            return True
    except Exception as e:
        print("[WARN] thumb", path, e)
        return False

def already_ingested(fid: str, sha1: str) -> bool:
    try:
        client = get_client()
        rs = client.query(
            "SELECT count() FROM raw_files WHERE file_id = %(fid)s OR sha1 = %(sha1)s",
            {"fid": fid, "sha1": sha1},
        )
        return (rs.result_rows[0][0] or 0) > 0
    except Exception as e:
        print("[WARN] dedupe check failed:", e)
        return False

def insert_row(row: dict):
    client = get_client()
    cols = [
        "file_id","path","relpath","size_bytes","mtime","ctime","sha1","phash","mime",
        "width","height","orientation","camera_make","camera_model","lens_model",
        "iso","f_number","exposure_time","focal_length_mm","taken_at_utc","tz_offset_minutes",
        "gps_lat","gps_lon","gps_alt_m","country","state","city",  # <- added
        "subjects","rating","description","ingestion_run_id"
    ]
    client.insert("raw_files", [[row.get(c) for c in cols]], column_names=cols)

def process_file(p: Path, run_id: str):
    st = p.stat()
    suffix = p.suffix.lower()
    fid = str(file_id_for(p, st))                # UUID string
    rel = str(p.relative_to(PHOTOS_ROOT))
    sha1 = sha1_of_file(p)

    # dedupe
    if already_ingested(fid, sha1):
        print("[INFO] skip duplicate", rel)
        return

    # MIME guess
    mime_map = {
        ".jpg":"image/jpeg",".jpeg":"image/jpeg",".png":"image/png",
        ".heic":"image/heic",".heif":"image/heif",".raf":"image/x-fuji-raf"
    }
    mime = mime_map.get(suffix, "application/octet-stream")

    # EXIF / metadata
    if suffix in RAW_EXTS:
        exif = minimal_exif_raw(p)
        thumb_ok = False  # no thumbnail for RAWs
    else:
        exif = extract_exif_raster(p)
        thumb_ok = upload_thumbnail(p, f"thumbs/{fid}.jpg")

    # UTC for filesystem times
    mtime_utc = datetime.utcfromtimestamp(st.st_mtime)
    ctime_utc = datetime.utcfromtimestamp(st.st_ctime)

    row = dict(
        file_id=fid,
        path=str(p),
        relpath=rel,
        size_bytes=st.st_size,
        mtime=mtime_utc,
        ctime=ctime_utc,
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
        exposure_time= safe(exif.get("exposure_time")),
        focal_length_mm=exif.get("focal_length_mm", 0.0),
        taken_at_utc=exif.get("taken_at_utc") or mtime_utc,
        tz_offset_minutes=exif.get("_tz_offset_minutes", 0),
        gps_lat=exif.get("_gps_lat"),
        gps_lon=exif.get("_gps_lon"),
        gps_alt_m=exif.get("_gps_alt"),
        country=exif.get("_country",""),
        state=exif.get("_state",""),
        city=exif.get("_city",""),
        subjects=[], rating=None, description="",
        ingestion_run_id=run_id
    )
    insert_row(row)
    print("[INFO] inserted", rel, "(thumb:" + ("ok" if thumb_ok else "skip") + ")")

def main():
    root = Path(PHOTOS_ROOT)
    if not root.exists():
        print(f"[ERR] PHOTOS_ROOT does not exist: {PHOTOS_ROOT}")
    while True:
        run_id = datetime.utcnow().strftime("run%Y%m%d%H%M%S")
        print("[INFO] scan start", run_id)
        for p in Path(PHOTOS_ROOT).rglob("*"):
            if p.is_file() and p.suffix.lower() in EXTS:
                try:
                    process_file(p, run_id)
                except OperationalError as e:
                    print("[ERR] ClickHouse op", p, e)
                except Exception as e:
                    print("[ERR]", p, e)
        print("[INFO] scan done", run_id)
        time.sleep(600)  # 10 min

if __name__ == "__main__":
    main()
