import requests
import re
from bs4 import BeautifulSoup
import polars as pl
import os
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timezone
from pysolar.solar import get_altitude
import mgrs

# Helpers
_TIME_RE = re.compile(r"_(\d{8}T\d{6})_")
_TILE_RE = re.compile(r"_T(\d{2}[A-Z]{3})_")

_mgrs = mgrs.MGRS()
_tile_cache: dict[str, tuple[float, float]] = {}  # tile_id -> (lat, lon)

# --- CONFIGURATION ---
BASE_URL_ROOT = "https://csv.dataspace.copernicus.eu/Sentinel-2"
OUTPUT_DIR = "./ESA_monthly_summaries/20250320/"
BASE = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def odata_escape_string(s: str) -> str:
    # OData uses single quotes for string literals; escape any internal quotes by doubling them
    return s.replace("'", "''").strip()


def fetch_esa_metadata(granule_name: str) -> dict:
    
    #Query CDSE OData for a granule by Name and return a small metadata record.
    #Note: Sun angle/SZA typically isn't present in OData Attributes; expect ESA_SZA=None.
    granule_name_clean = odata_escape_string(granule_name)

    params = {
        "$filter": f"Name eq '{granule_name_clean}'",
        "$expand": "Attributes",
        "$top": "1",
    }

    result = {
        "Granule_Name": granule_name,
        "ESA_cloud": None,
        "ESA_SZA": None,   
        "ESA_S2": granule_name[:3],  # S2A/S2B/S2C inferred from prefix
    }

    try:
        r = requests.get(BASE, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()

        if data.get("value"):
            attrs = data["value"][0].get("Attributes", [])

            cloud = next((a.get("Value") for a in attrs if a.get("Name") == "cloudCover"), None)
            if cloud is not None:
                result["ESA_cloud"] = float(cloud)

            # These keys often do NOT exist in CDSE OData
            sza = next(
                (a.get("Value") for a in attrs if a.get("Name") in {"illuminationZenithAngle", "meanSunZenithAngle"}),
                None,
            )
            if sza is not None:
                result["ESA_SZA"] = float(sza)

    except Exception:
        # keep quiet for batch runs; you can log if you want
        pass

    return result


def enrich_csv_fast(input_csv: str, output_csv: str, max_workers: int = 10) -> None:
    if not os.path.exists(input_csv):
        print(f"Input file {input_csv} not found.")
        return

    print(f"Reading the file {input_csv}...")
    df = pl.read_csv(input_csv, ignore_errors=True)

    # Ensure we have a Granule_Name column.
    if "Granule_Name" not in df.columns:
        # If your CSV has no header / default headers, take the 2nd column as requested
        if df.width < 2:
            print("Input CSV has fewer than 2 columns; cannot use second column as Granule_Name.")
            print("Columns found:", df.columns)
            return

        second_col = df.columns[1]
        print(f"'Granule_Name' not found. Using 2nd column '{second_col}' as Granule_Name.")
        df = df.rename({second_col: "Granule_Name"})

    # Make sure Granule_Name is string + stripped
    df = df.with_columns(pl.col("Granule_Name").cast(pl.Utf8).str.strip_chars())

    unique_granules = (
        df.select("Granule_Name")
          .drop_nulls()
          .unique()
          .to_series()
          .to_list()
    )
    print(f"Number of {len(unique_granules)} unique granules to query.")
    metadata_records = []

    print(f"Working on {max_workers} concurrent threads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_granule = {executor.submit(fetch_esa_metadata, g): g for g in unique_granules}

        for future in tqdm(as_completed(future_to_granule), total=len(unique_granules), desc="Downloading Metadata"):
            metadata_records.append(future.result())

    df_meta = pl.DataFrame(metadata_records)

    print("\nMerging metadata back into the main dataset...")
    df_final = df.join(df_meta, on="Granule_Name", how="left")

    df_final.write_csv(output_csv)
    print(f"Saved metadata from ESA with {df_final.height} rows to {output_csv}")



def sensing_time_utc_from_granule_name(granule_name: str) -> datetime | None:

    #Extract sensing time YYYYMMDDTHHMMSS from SAFE name and return UTC datetime.
    
    if not granule_name:
        return None
    m = _TIME_RE.search(granule_name)
    if not m:
        return None
    t = datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    return t


def tile_id_from_row(granule_name: str, tile_id: str | None) -> str | None:
    """
    Prefer provided Tile_ID column; otherwise extract from granule name.
    """
    if tile_id and str(tile_id).strip():
        return str(tile_id).strip()
    if not granule_name:
        return None
    m = _TILE_RE.search(granule_name)
    return m.group(1) if m else None


def tile_to_latlon(tile_id: str) -> tuple[float, float] | None:
    """
    Convert tile_id to (lat, lon) using cache.
    NOTE: mgrs.MGRS().toLatLon expects a full MGRS coordinate; for your workflow,
    we treat tile_id as a representative point (library-dependent).
    If this raises, return None.
    """
    tile_id = tile_id.strip()
    if tile_id in _tile_cache:
        return _tile_cache[tile_id]

    try:
        lat, lon = _mgrs.toLatLon(tile_id)
        lat, lon = float(lat), float(lon)
        _tile_cache[tile_id] = (lat, lon)
        return lat, lon
    except Exception:
        return None


def compute_sza(granule_name: str, tile_id: str | None) -> float | None:
    """
    Compute Solar Zenith Angle = 90 - solar_altitude
    Returns None if anything needed is missing/invalid.
    """
    t = sensing_time_utc_from_granule_name(granule_name)
    if t is None:
        return None

    tile = tile_id_from_row(granule_name, tile_id)
    if tile is None:
        return None

    latlon = tile_to_latlon(tile)
    if latlon is None:
        return None

    lat, lon = latlon
    try:
        altitude = get_altitude(lat, lon, t)  # degrees
        return float(90.0 - altitude)
    except Exception:
        return None




def get_hls_granule_count(date_str):
    
    # Queries CMR for HLS S30 granules on a specific date.
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.json"
    
    # We search for HLS S30 and HLS L30
    concept_id = ["C2021957295-LPCLOUD", "C2021957657-LPCLOUD"]
    datasets = {
        "HLS S30 Granule": "C2021957295-LPCLOUD",
        "HLS L30 Granule": "C2021957657-LPCLOUD"
    }
    
    # Format the Date (Temporal Range)
    # Format: yyyy-MM-ddThh:mm:ssZ,yyyy-MM-ddThh:mm:ssZ
    start_date = f"{date_str}T00:00:00Z"
    end_date = f"{date_str}T23:59:59Z"
    
    params = {
        "collection_concept_id": concept_id,
        "temporal": f"{start_date},{end_date}",
        "page_size": 0  # We only want the count, not the file list (faster)
    }
    
    headers = {
        "User-Agent": "HLS-Recon-Tool/1.0"
    }

    results = {}
    print(f"---- Querying CMR for HLS Granules on {date_str} ----\n")

    # We loop the collection to find the granule count for S30 and L30
    for name, concept_id in datasets.items():
        params = {
            "collection_concept_id": concept_id, 
            "temporal": f"{start_date},{end_date}",
            "page_size": 0  # We only want the count, not the file list
        }
        
        try:
            response = requests.get(cmr_url, params=params, headers=headers)
            response.raise_for_status()
            
            # Extract the count from the 'CMR-Hits' header
            hits = int(response.headers.get("CMR-Hits", 0))
            results[name] = hits
            
            print(f"{name}: {hits:,} granules")

        except Exception as e:
            print(f"Error querying {name} ({concept_id}): {e}")
            results[name] = 0
            
    # Print the combined total just in case you still want it
    total_hits = sum(results.values())
    print(f"Total granules: {total_hits:,}")
    
    return results


def get_hls_metadata(date_str):
    cmr_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"
    
    search_start_date = f"{date_str}T00:00:00Z"
    search_end_date = f"{date_str}T23:59:59Z"
    concept_id = [
        "C2021957295-LPCLOUD", # HLS S30
        "C2021957657-LPCLOUD"
        ]

    params = {
        "collection_concept_id": concept_id,
        "temporal": f"{search_start_date},{search_end_date}",
        "page_size": 2000, 
    }
    
    headers = {
        "User-Agent": "HLS-Metadata-Extractor/1.0"
    }
    
    all_records = []
    page_num = 1
    
    print(f"Retrieving the CMR metadata for {date_str}...")

    while True:
        params["page_num"] = page_num
        
        try:
            response = requests.get(cmr_url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            items = data.get("items", [])
            if not items:
                break 
            
            print(f"   PAGE {page_num}: Found {len(items)} granules...")
            
            for item in items:
                umm = item.get("umm", {})
                meta = item.get("meta", {})
                
                # --- 1. Core Identifiers ---
                granule_ur = umm.get("GranuleUR", "N/A")
                
                # --- 2. FIX: Insert Time ---
                # Strategy A: Check meta 'creation-date' (System Insert Time)
                insert_time = meta.get("creation-date")
                
                # Strategy B: Check ProviderDates for type 'Insert' (Metadata Insert Time)
                if not insert_time:
                    provider_dates = umm.get("ProviderDates", [])
                    for date_obj in provider_dates:
                        if date_obj.get("Type") == "Insert":
                            insert_time = date_obj.get("Date")
                            break
                
                # Fallback to N/A if still not found
                if not insert_time:
                    insert_time = "N/A"

                last_update = meta.get("revision-date", "N/A")
                
                # --- 3. FIX: Temporal Fields (TemporalExtent) ---
                # UMM-JSON uses 'TemporalExtent', not 'Temporal'
                temporal = umm.get("TemporalExtent", {})
                
                # Sometimes RangeDateTime is a list, sometimes a dict
                range_date_time = temporal.get("RangeDateTime")
                
                if isinstance(range_date_time, list) and len(range_date_time) > 0:
                    range_date_time = range_date_time[0]
                elif not isinstance(range_date_time, dict):
                    range_date_time = {}

                begin_date = range_date_time.get("BeginningDateTime", "N/A")
                end_date = range_date_time.get("EndingDateTime", "N/A")

                # --- 4. Additional Attributes ---
                cloud_cover = None
                sza = None
                product_uri = None
                
                attributes = umm.get("AdditionalAttributes", [])
                for attr in attributes:
                    name = attr.get("Name")
                    val_list = attr.get("Values", [])
                    val = val_list[0] if val_list else None
                    
                    if name == "CLOUD_COVERAGE":
                        cloud_cover = val
                    elif name == "MEAN_SUN_ZENITH_ANGLE":
                        sza = val
                    elif name == "PRODUCT_URI":
                        product_uri = val
                
                # --- 5. Append Record ---
                all_records.append({
                    "Granule_ID": granule_ur,
                    "Product_URI": product_uri,
                    "Insert_Time": insert_time,
                    "Last_Update": last_update,
                    "Beginning_DateTime": begin_date,
                    "Ending_DateTime": end_date,
                    "Cloud_Cover": cloud_cover,
                    "Mean_Sun_Zenith_Angle": sza
                })
            
            page_num += 1
            
        except Exception as e:
            print(f"Error on page {page_num}: {e}")
            break
            
    return all_records

