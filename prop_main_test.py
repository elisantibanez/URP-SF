"""
United Realty Partners - Property Processing Engine (Enhanced Version)
===========================================================================

Enhanced with timestamp watermarks replacing single Processed__c checkbox.
Preserves all original business logic including fuzzy matching and validation.

New Features:
- Timestamp-based watermarks for continuous processing
- Handoff flag for Contact worker coordination
- Optional change fingerprinting to skip unchanged records
- Optional locking for parallel processing safety
- Per-cycle deduplication
"""

import re
import time
import logging
import json
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
from simple_salesforce import Salesforce
from fuzzywuzzy import fuzz
import pandas as pd
from dotenv import load_dotenv
from os import getenv
from typing import Dict, Set


# Load environment variables
load_dotenv()

# ==================== CONFIGURATION ====================
SALESFORCE_USERNAME = getenv("SF_USERNAME")
SALESFORCE_PASSWORD = getenv("SF_PASSWORD")
SALESFORCE_SECURITY_TOKEN = getenv("SF_TOKEN")
# SF_DOMAIN = getenv("SF_DOMAIN").strip().strip('"').strip("'")  # "test" for sandbox, "login" for production
SF_DOMAIN = (getenv("SF_DOMAIN", "test") or "test").strip().strip('"').strip("'")
# Processing configuration
BATCH_SIZE = int(getenv("BATCH_SIZE", "150"))
SLEEP_SECONDS = int(getenv("SLEEP_SECONDS", "20"))
FINGERPRINT_ENABLED = getenv("FINGERPRINT_ENABLED", "true").lower() == "true"
LOCKING_ENABLED = getenv("LOCKING_ENABLED", "false").lower() == "true"  # Default false for single worker
LOCK_TTL_SECONDS = int(getenv("LOCK_TTL_SECONDS", "120"))
BACKOFF_MINUTES = int(getenv("BACKOFF_MINUTES", "5"))
LOOKBACK_DAYS = int(getenv("LOOKBACK_DAYS", "3"))  # how many days of changes to consider


# Matching thresholds for property deduplication (preserved from original)
MATCH_THRESHOLD_STRONG = 90     # Auto-match confidence level
MATCH_THRESHOLD_REVIEW = 70     # Flag for manual review

# Worker identifier for locking
WORKER_ID = f"property-worker-{getenv('DYNO', 'local')}"

# --- Allowed picklist constants for Prop_Last_Result__c ---
PLR_CREATED = "created"
PLR_UPDATED = "updated"
PLR_SUCCESS = "success"  # (use if you ever want a generic success)
PLR_ERROR = "error"
PLR_REVIEW = "review-needed"
PLR_SKIPPED_UNCHANGED = "skipped:unchanged-fingerprint"
PLR_SKIPPED_LOCK = "skipped:lock-not-acquired"
PLR_SKIPPED_ALREADY = "skipped:already-processed"
PLR_SKIPPED_WAITING = "skipped:waiting-for-property"  # probably for your contact worker


# ==================== SETUP ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("property_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ProcessingResult:
    """Results container for individual record processing"""
    success: bool
    property_id: str = None
    status: str = ""
    needs_review: bool = False
    error_message: str = ""
    match_score: int = 0
    match_type: str = ""

# ==================== UTILITY FUNCTIONS ====================

def utcnow() -> datetime:
    """Get current UTC timestamp"""
    return datetime.now(timezone.utc)

def sf_datetime_now() -> str:
    """Get current UTC timestamp formatted for Salesforce"""
    return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def parse_sf_dt(s: Optional[str]) -> Optional[datetime]:
    """Parse Salesforce ISO8601 datetimes like '2025-08-15T06:40:06Z' to aware datetime"""
    if not s:
        return None
    # Salesforce returns Z suffix; Python needs +00:00
    return datetime.fromisoformat(s.replace('Z', '+00:00'))


def compute_property_fingerprint(record: Dict) -> str:
    """
    Compute SHA-1 fingerprint of property-relevant fields
    Used to detect if meaningful data has changed
    """
    relevant_fields = {
        'Parcel_ID__c': record.get('Parcel_ID__c'),
        'Street_Number__c': record.get('Street_Number__c'),
        'Street_Name__c': record.get('Street_Name__c'),
        'City__c': record.get('City__c'),
        'State__c': record.get('State__c'),
        'Zip__c': record.get('Zip__c'),
        'County__c': record.get('County__c'),
        'Legal_Description__c': record.get('Legal_Description__c'),
        'Total_Appraised_Value__c': record.get('Total_Appraised_Value__c'),
        'Mortgage_Balance__c': record.get('Mortgage_Balance__c'),
        'Foreclosure_Status__c': record.get('Foreclosure_Status__c'),
        'Delinquent_Taxes_Found__c': record.get('Delinquent_Taxes_Found__c')
    }
    
    # Convert to JSON and hash
    json_str = json.dumps(relevant_fields, sort_keys=True, separators=(',', ':'))
    return hashlib.sha1(json_str.encode('utf-8')).hexdigest()

# ==================== SALESFORCE CONNECTION ====================
def connect_to_salesforce() -> Salesforce:
    """Establish connection to Salesforce org"""
    try:
        return Salesforce(
            username=SALESFORCE_USERNAME,
            password=SALESFORCE_PASSWORD,
            security_token=SALESFORCE_SECURITY_TOKEN,
            domain="test",
        )
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {e}")
        raise



# ==================== PROPERTY SCHEMA HELPERS ====================
_PROP_FIELDS_CACHE = None

def _property_fields(sf) -> set:
    """
    Describe Left_Main__Property__c once per run and cache the field names.
    """
    global _PROP_FIELDS_CACHE
    if _PROP_FIELDS_CACHE is None:
        desc = sf.Left_Main__Property__c.describe()
        _PROP_FIELDS_CACHE = {f["name"] for f in desc["fields"]}
    return _PROP_FIELDS_CACHE

# def _safe_prop_payload(sf, payload: dict) -> dict:
#     """
#     Drop any keys not present on Left_Main__Property__c to avoid INVALID_FIELD.
#     """
#     fields = _property_fields(sf)
#     return {k: v for k, v in payload.items() if k in fields}

# def _safe_prop_payload(sf, payload: dict) -> dict:
#     """TEMPORARY: Return all fields without filtering for debugging"""
#     logger.warning(f"BYPASSING field validation - sending all {len(payload)} fields")
#     return payload

def _safe_prop_payload(sf, payload: dict) -> dict:
    """Drop any keys not present on Left_Main__Property__c to avoid INVALID_FIELD"""
    fields = _property_fields(sf)
    filtered = {k: v for k, v in payload.items() if k in fields}
    
    # Log dropped fields for debugging
    dropped = set(payload.keys()) - set(filtered.keys())
    if dropped:
        logger.debug(f"Dropped {len(dropped)} invalid fields: {dropped}")
    
    return filtered

# ==================== LOCKING MECHANISM ====================
def try_acquire_lock(sf: Salesforce, record_id: str) -> bool:
    if not LOCKING_ENABLED:
        return True
    try:
        rec = sf.Data_Management__c.get(record_id)
        locked = bool(rec.get('Processing_Lock__c'))
        expires = parse_sf_dt(rec.get('Lock_Expires_At__c'))
        if locked and expires and expires > utcnow():
            return False  # someone else holds it

        lock_expires = (utcnow() + timedelta(seconds=LOCK_TTL_SECONDS)).strftime('%Y-%m-%dT%H:%M:%SZ')
        sf.Data_Management__c.update(record_id, {
            'Processing_Lock__c': True,
            'Lock_Owner__c': WORKER_ID,
            'Lock_Expires_At__c': lock_expires
        })
        return True
    except Exception as e:
        logger.debug(f"Could not acquire lock for {record_id}: {e}")
        return False

def release_lock(sf: Salesforce, record_id: str):
    """Release processing lock on a Data_Management__c record"""
    if not LOCKING_ENABLED:
        return
    
    try:
        sf.Data_Management__c.update(record_id, {
            'Processing_Lock__c': False,
            'Lock_Owner__c': None,
            'Lock_Expires_At__c': None
        })
    except Exception as e:
        logger.warning(f"Error releasing lock for {record_id}: {e}")

# ==================== DATA VALIDATION (PRESERVED FROM ORIGINAL) ====================
# def validate_property_data(record: Dict) -> Tuple[bool, List[str]]:
#     """
#     Validate essential property data fields for processing
    
#     Args:
#         record: Data management record to validate
        
#     Returns:
#         Tuple of (is_valid, list_of_errors)
#     """
#     errors = []
    
#     # Check for required identification fields
#     if not record.get('Property_Address__c') and not record.get('Legal_Description__c'):
#         if not (record.get('Street_Number__c') and record.get('Street_Name__c')):
#             errors.append("Missing property identification fields")
    
#     # Validate parcel ID format if present
#     parcel_id = record.get('Parcel_ID__c', '')
#     if parcel_id and not re.match(r'^[A-Z0-9\-]+$', str(parcel_id).upper()):
#         errors.append(f"Invalid parcel ID format: {parcel_id}")
    
#     # Validate address contains numbers (if using Property_Address__c)
#     if record.get('Property_Address__c'):
#         address = record['Property_Address__c']
#         if not re.search(r'\d', address):
#             errors.append("Property address appears incomplete - no street numbers")
    
#     return len(errors) == 0, errors

def validate_property_data(record: Dict) -> Tuple[bool, List[str]]:
    """
    Validate essential property data fields for processing
    """
    errors = []

    # Required identifiers: either (Street_Number + Street_Name) or Legal_Description
    has_street_parts = bool(record.get('Street_Number__c') and record.get('Street_Name__c'))
    has_legal        = bool(record.get('Legal_Description__c'))
    if not (has_street_parts or has_legal):
        errors.append("Missing property identification: need Street_Number__c + Street_Name__c OR Legal_Description__c")

    # Validate parcel ID format if present (allow digits, letters, hyphen only)
    parcel_id = record.get('Parcel_ID__c', '')
    if parcel_id and not re.match(r'^[A-Z0-9\-]+$', str(parcel_id).upper()):
        errors.append(f"Invalid parcel ID format: {parcel_id}")

    # Optional: ensure street number is actually numeric-ish if provided
    sn = str(record.get('Street_Number__c') or '').strip()
    if sn and not re.search(r'\d', sn):
        errors.append("Street_Number__c should contain digits")

    return len(errors) == 0, errors

# ==================== DATA NORMALIZATION (PRESERVED FROM ORIGINAL) ====================
def normalize_text(text: str) -> str:
    """Normalize text for consistent fuzzy matching"""
    if not text:
        return ""
    normalized = re.sub(r'[^\w\s]', '', str(text).lower())
    return ' '.join(normalized.split())

def normalize_address(record: Dict) -> str:
    """
    Construct normalized address from individual components
    Handles float street numbers (e.g., 843.0 -> "843")
    """
    def clean_component(comp):
        if isinstance(comp, float) and comp.is_integer():
            return str(int(comp))
        return str(comp).strip() if comp else ""

    components = [
        record.get('Street_Number__c', ''),
        record.get('Street_Name__c', ''),
        record.get('City__c', ''),
        record.get('State__c', ''),
        record.get('Zip__c', '')
    ]
    return ', '.join([clean_component(comp) for comp in components if clean_component(comp)])

def _build_address_line(dm: Dict) -> str:
    """
    Build a single street line for Left_Main__Address__c from components.
    Example: "123 Main St"
    (City/State/Zip live in their own fields, so we keep this to street number + name.)
    """
    num  = str(dm.get('Street_Number__c') or '').strip()
    name = str(dm.get('Street_Name__c')   or '').strip()
    return " ".join([p for p in (num, name) if p])

def normalize_parcel_id(parcel_id: str, county: str = "") -> str:
    """
    Normalize parcel ID with optional county prefix for uniqueness
    """
    if not parcel_id:
        return ""
    normalized = str(parcel_id).upper().strip()
    if county and not normalized.startswith(county.upper()):
        normalized = f"{county.upper()}-{normalized}"
    return normalized

# ==================== PROPERTY MATCHING (PRESERVED FROM ORIGINAL) ====================
def fuzzy_match_score(text1: str, text2: str) -> int:
    """Calculate fuzzy match score between two text strings"""
    return fuzz.token_sort_ratio(normalize_text(text1), normalize_text(text2))

# def find_property_match(sf: Salesforce, new_record: Dict) -> Tuple[Optional[str], str, int]:
#     """
#     Find existing property match using hierarchical matching:
#     1. Parcel ID (exact match - highest confidence)
#     2. Address + Legal Description (fuzzy match)
#     3. Address only (fuzzy match)
    
#     Returns:
#         Tuple of (property_id, match_type, match_score)
#         match_type: 'parcel_match', 'strong_fuzzy_match', 'address_match', 'needs_review', 'no_match'
#     """
#     parcel_id = normalize_parcel_id(new_record.get('Parcel_ID__c', ''), new_record.get('County__c', ''))
#     address = normalize_address(new_record)
#     legal_desc = new_record.get('Legal_Description__c', '')

#     query = """
#     SELECT Id, Parcel_ID__c, Left_Main__Address__c, Legal_Description__c 
#     FROM Left_Main__Property__c 
#     WHERE Parcel_ID__c != null OR Left_Main__Address__c != null
#     """

def find_property_match(sf: Salesforce, new_record: Dict) -> Tuple[Optional[str], str, int]:
    parcel_id = normalize_parcel_id(new_record.get('Parcel_ID__c', ''), new_record.get('County__c', ''))
    
    # Use street-only for address matching to match what's stored in Properties
    street_address = _build_address_line(new_record)  # "123 Main St"
    legal_desc = new_record.get('Legal_Description__c', '')

    # Keep both raw and normalized parcel forms
    raw_incoming_parcel = (new_record.get('Parcel_ID__c') or "").strip().upper()
    norm_incoming_parcel = parcel_id

    city = (new_record.get('City__c') or "").replace("'", "\\'")
    state = (new_record.get('State__c') or "").replace("'", "\\'")
    zipc = re.sub(r'\D','', str(new_record.get('Zip__c') or ""))[:9]

    where = ["(Parcel_ID__c != null OR Left_Main__Address__c != null)"]

    # Use city as a soft filter so we still pick up records where city is missing
    # Example: first load had no city (Property city is null), later load has city
    # We want both to be considered for matching instead of forcing a new Property
    if city:
        where.append(f"(Left_Main__City__c = '{city}' OR Left_Main__City__c = null)")

    if state:
        where.append(f"Left_Main__State__c = '{state}'")
    if zipc:
        where.append(f"Left_Main__Zip_Code__c = '{zipc}'")

    query = f"""
      SELECT Id, Parcel_ID__c, Left_Main__Address__c, Legal_Description__c,
             Left_Main__City__c, Left_Main__State__c, Left_Main__Zip_Code__c
      FROM Left_Main__Property__c
      WHERE {" AND ".join(where)}
      LIMIT 2000
    """

    try:
        existing_properties = sf.query_all(query)['records']
    except Exception as e:
        logger.error(f"Error querying properties: {e}")
        return None, 'error', 0

    if not existing_properties:
        logger.info("No properties found in Salesforce – treating as no match.")
        return None, 'no_match', 0

    best_match_id = None
    best_score = 0
    match_reason = 'no_match'

    for prop in existing_properties:
        # Priority 1: Parcel ID match (try both normalized and raw)
        prop_raw_parcel = (prop.get('Parcel_ID__c') or "").strip().upper()
        prop_norm_parcel = normalize_parcel_id(prop_raw_parcel, new_record.get('County__c', ''))

        if norm_incoming_parcel and prop_norm_parcel == norm_incoming_parcel:
            return prop['Id'], 'parcel_match', 100
        if raw_incoming_parcel and prop_raw_parcel == raw_incoming_parcel:
            return prop['Id'], 'parcel_match', 100

        # Priority 2: Combined address and legal description fuzzy match
        if street_address and legal_desc:
            # Compare street-to-street (apples to apples)
            addr_score = fuzzy_match_score(street_address, prop.get('Left_Main__Address__c', ''))
            legal_score = fuzzy_match_score(legal_desc, prop.get('Legal_Description__c', ''))
            combined_score = (addr_score + legal_score) / 2

            if combined_score >= MATCH_THRESHOLD_STRONG:
                return prop['Id'], 'strong_fuzzy_match', int(combined_score)
            elif combined_score >= MATCH_THRESHOLD_REVIEW and combined_score > best_score:
                best_match_id = prop['Id']
                best_score = combined_score
                match_reason = 'needs_review'

        # Priority 3: Address-only fuzzy match
        elif street_address:
            # Compare street-to-street consistently
            addr_score = fuzzy_match_score(street_address, prop.get('Left_Main__Address__c', ''))
            
            if addr_score >= MATCH_THRESHOLD_STRONG:
                return prop['Id'], 'address_match', int(addr_score)
            elif addr_score >= MATCH_THRESHOLD_REVIEW and addr_score > best_score:
                best_match_id = prop['Id']
                best_score = addr_score
                match_reason = 'needs_review'
    
    return best_match_id, match_reason, int(best_score)

# def find_property_match(sf: Salesforce, new_record: Dict) -> Tuple[Optional[str], str, int]:
#     parcel_id = normalize_parcel_id(new_record.get('Parcel_ID__c', ''), new_record.get('County__c', ''))
#     address   = normalize_address(new_record)
#     legal_desc = new_record.get('Legal_Description__c', '')

#     # Keep both raw and normalized parcel forms handy
#     raw_incoming_parcel  = (new_record.get('Parcel_ID__c') or "").strip().upper()
#     norm_incoming_parcel = parcel_id  # already normalized with county

#     city  = (new_record.get('City__c') or "").replace("'", "\\'")
#     state = (new_record.get('State__c') or "").replace("'", "\\'")
#     zipc  = re.sub(r'\D','', str(new_record.get('Zip__c') or ""))[:9]

#     where = ["(Parcel_ID__c != null OR Left_Main__Address__c != null)"]
#     if city:  where.append(f"Left_Main__City__c = '{city}'")
#     if state: where.append(f"Left_Main__State__c = '{state}'")
#     if zipc:  where.append(f"Left_Main__Zip_Code__c = '{zipc}'")

#     query = f"""
#       SELECT Id, Parcel_ID__c, Left_Main__Address__c, Legal_Description__c
#       FROM Left_Main__Property__c
#       WHERE {" AND ".join(where)}
#       LIMIT 2000
#     """

#     try:
#         existing_properties = sf.query_all(query)['records']
#     except Exception as e:
#         logger.error(f"Error querying properties: {e}")
#         return None, 'error', 0

#     if not existing_properties:
#         logger.info("No properties found in Salesforce – treating as no match.")
#         return None, 'no_match', 0

#     best_match_id = None
#     best_score = 0
#     match_reason = 'no_match'

#     # for prop in existing_properties:
#     #     # Priority 1: Exact parcel ID match
#     #     if parcel_id and prop.get('Parcel_ID__c') == parcel_id:
#     #         return prop['Id'], 'parcel_match', 100
        
#     for prop in existing_properties:
#         # Priority 1: Parcel ID match (try both normalized and raw)
#         prop_raw_parcel  = (prop.get('Parcel_ID__c') or "").strip().upper()
#         prop_norm_parcel = normalize_parcel_id(prop_raw_parcel, new_record.get('County__c', ''))

#         if norm_incoming_parcel and prop_norm_parcel == norm_incoming_parcel:
#             return prop['Id'], 'parcel_match', 100
#         if raw_incoming_parcel and prop_raw_parcel == raw_incoming_parcel:
#             return prop['Id'], 'parcel_match', 100

#         # Priority 2: Combined address and legal description fuzzy match
#         if address and legal_desc:
#             addr_score = fuzzy_match_score(address, prop.get('Left_Main__Address__c', ''))
#             legal_score = fuzzy_match_score(legal_desc, prop.get('Legal_Description__c', ''))
#             combined_score = (addr_score + legal_score) / 2

#             if combined_score >= MATCH_THRESHOLD_STRONG:
#                 return prop['Id'], 'strong_fuzzy_match', int(combined_score)
#             elif combined_score >= MATCH_THRESHOLD_REVIEW and combined_score > best_score:
#                 best_match_id = prop['Id']
#                 best_score = combined_score
#                 match_reason = 'needs_review'

#         # Priority 3: Address-only fuzzy match
#         elif address:
#             street_only = _build_address_line(new_record)
#             addr_score = fuzzy_match_score(street_only, prop.get('Left_Main__Address__c', ''))
#             # addr_score = fuzzy_match_score(address, prop.get('Left_Main__Address__c', ''))
#             if addr_score >= MATCH_THRESHOLD_STRONG:
#                 return prop['Id'], 'address_match', int(addr_score)
#             elif addr_score >= MATCH_THRESHOLD_REVIEW and addr_score > best_score:
#                 best_match_id = prop['Id']
#                 best_score = addr_score
#                 match_reason = 'needs_review'
    
#     return best_match_id, match_reason, int(best_score)



# ==================== PROPERTY UPDATE HELPER ====================
# Assumes you already have: utcnow(), parse_sf_dt(), normalize_address(), logger

# ---- Config: thresholds, transitions, field maps ----
PRICE_PCT_THRESHOLD = 0.015   # 1.5% change needed to update appraised/total values
MORTGAGE_ABS_THRESHOLD = 500  # $500 minimum change to update mortgage balance

ALLOWED_STATUS_TRANSITIONS = {
    # Foreclosure_Status__c: allow moves in this set; direction governed by freshness already
    # You can tighten this later if needed
    "Active Foreclosure": {"Cancelled Foreclosure", "Still in Auction", "Check Lawfirm", "Active Foreclosure"},
    "Still in Auction": {"Cancelled Foreclosure", "Active Foreclosure", "Check Lawfirm", "Still in Auction"},
    "Cancelled Foreclosure": {"Active Foreclosure", "Check Lawfirm", "Cancelled Foreclosure"},
    "Check Lawfirm": {"Active Foreclosure", "Cancelled Foreclosure", "Still in Auction", "Check Lawfirm"},
}

# DM -> Property (one-to-one scalar) you want to sync
# kind: "text" | "num ber" | "currency" | "date" | "datetime" | "status" | "zip"
# policy: "conservative" (null-fill + rare overwrite) | "normal" (controlled overwrite) | "immutable" (set-once)
FIELD_MAP = {
    # Identifiers & address (conservative/immutable)
    "Parcel_ID__c":                 {"prop": "Parcel_ID__c",                 "kind": "text",     "policy": "immutable"},
    "Street_Number__c":             {"prop": "Street_Number__c",            "kind": "text", "policy": "conservative"},    
    "Address_Line__computed":       {"prop": "Left_Main__Address__c",        "kind": "text",     "policy": "conservative"},    
    "City__c":                      {"prop": "Left_Main__City__c",           "kind": "text",     "policy": "conservative"},
    "State__c":                     {"prop": "Left_Main__State__c",          "kind": "text",     "policy": "conservative"},
    "Zip__c":                       {"prop": "Left_Main__Zip_Code__c",       "kind": "zip",      "policy": "conservative"},
    "County__c":                    {"prop": "Left_Main__County__c",         "kind": "text",     "policy": "conservative"},
    "Legal_Description__c":         {"prop": "Legal_Description__c",         "kind": "text",     "policy": "conservative"},

    # Numeric facts (normal controlled overwrite)
    "Total_Appraised_Value__c":     {"prop": "Total_Appraised_Value__c",     "kind": "currency", "policy": "normal", "threshold": {"pct": PRICE_PCT_THRESHOLD}},
    "Appraised_Value_Improvement__c":{"prop":"Appraised_Value_Improvement__c","kind":"currency","policy":"normal", "threshold": {"pct": PRICE_PCT_THRESHOLD}},
    "Appraised_Land_Value__c":      {"prop": "Appraised_Value_Land__c",      "kind": "currency", "policy": "normal", "threshold": {"pct": PRICE_PCT_THRESHOLD}},
    "Mortgage_Balance__c":          {"prop": "Mortgage_Balance__c",          "kind": "currency", "policy": "normal", "threshold": {"abs": MORTGAGE_ABS_THRESHOLD}},
    "Bedrooms__c":                  {"prop": "Bedrooms__c",                  "kind": "number",   "policy": "normal"},
    "Baths__c":                     {"prop": "Baths__c",                     "kind": "number",   "policy": "normal"},
    "Year_Built__c":                {"prop": "Year_Built__c",                "kind": "number",   "policy": "normal"},
    "Sq_Footage__c":                {"prop": "Sq_Footage__c",                "kind": "number",   "policy": "normal"},

    # Dates (normal controlled overwrite)
    "Foreclosure_Sale_Date__c":     {"prop": "Foreclosure_Sale_Date__c",     "kind": "date",     "policy": "normal"},
    "Tax_Auction_Date__c":          {"prop": "Tax_Auction_Date__c",          "kind": "date",     "policy": "normal"},
    "HOA_Auction_Date__c":          {"prop": "HOA_Auction_Date__c",          "kind": "date",     "policy": "normal"},
    "Mortgage_Auction_Date__c":     {"prop": "Mortgage_Auction_Date__c",     "kind": "date", "policy": "normal"},

    # Picklist statuses (normal + allowed transitions)
    "Foreclosure_Status__c":        {"prop": "Foreclosure_Status__c",        "kind": "status",   "policy": "normal"},
    "Occupancy_Type__c":            {"prop": "Occupancy_Type__c",            "kind": "status",   "policy": "normal"},
    "HOA_Payment_Status__c":        {"prop": "HOA_Payment_Status__c",        "kind": "status",   "policy": "normal"},
    "Mortgage_Status__c":           {"prop": "Mortgage_Status__c",           "kind": "status",   "policy": "normal"},
    "Property_Type__c":             {"prop": "Property_Type__c",             "kind": "status",   "policy": "normal"},

    # Booleans (normal; newer wins if different)
    "Tax_Delinquent__c":            {"prop": "Delinquent_Taxes_Found__c",    "kind": "boolean",  "policy": "normal"},
    "TAX_Foreclosure__c":           {"prop": "TAX_Foreclosure__c",           "kind": "boolean",  "policy": "normal"},
    "HOA_Foreclosure__c":           {"prop": "HOA_Foreclosure__c",           "kind": "boolean",  "policy": "normal"},
    "Mortgage_Foreclosure__c":      {"prop": "Mortgage_Foreclosure__c",      "kind": "boolean",  "policy": "normal"},
    "Property_Foreclosure__c":      {"prop": "Property_Foreclosure__c",      "kind": "boolean",  "policy": "normal"},
}

# Multipicklists to UNION (never remove silently)
MULTIPICKLIST_MAP = {
    "Foreclosure_Type__c":  "Foreclosure_Type__c",
    "Lien_Types__c":        "Lien_Types__c",
    "Tax_Type__c": "Tax_Type__c",  # Make sure this is present
}

JUNKY_TOKENS = {"unknown", "n/a", "na", "none", "null", "tbd", "pending", "—", "-", ""}

def _norm_text(x: Optional[str]) -> str:
    if x is None:
        return ""
    return re.sub(r'\s+', ' ', str(x)).strip().lower()

def _same_after_norm(a, b, kind: str) -> bool:
    if kind in ("number", "currency"):
        try:
            return float(a) == float(b)
        except Exception:
            return _norm_text(a) == _norm_text(b)
    if kind in ("date", "datetime"):
        return _norm_text(a) == _norm_text(b)
    if kind == "boolean":
        return (bool(a) == bool(b))
    if kind == "zip":
        return re.sub(r'\D', '', str(a) or "") == re.sub(r'\D', '', str(b) or "")
    return _norm_text(a) == _norm_text(b)

def _looks_junky(val, kind: str) -> bool:
    if val is None:
        return True
    s = _norm_text(val)
    if s in JUNKY_TOKENS:
        return True
    if kind == "currency":
        try:
            return float(val) == 0.0
        except Exception:
            return True
    if kind == "zip":
        digits = re.sub(r'\D', '', str(val))
        return not (len(digits) == 5 or len(digits) == 9)
    return False

def _union_multipicklist(existing: str, incoming: str) -> str:
    def split_mp(v):
        if not v:
            return set()
        return {t.strip() for t in str(v).split(";") if t.strip()}
    merged = split_mp(existing) | split_mp(incoming)
    return ";".join(sorted(merged)) if merged else ""

def _allowed_status_change(field: str, old: str, new: str) -> bool:
    if field != "Foreclosure_Status__c":
        return True  # other picklists unrestricted for now
    if not old:
        return True
    return new in ALLOWED_STATUS_TRANSITIONS.get(old, {old})

def _threshold_pass(kind: str, old, new, threshold: dict | None) -> bool:
    if kind not in ("number", "currency") or threshold is None:
        return True
    try:
        oldf = float(old)
        newf = float(new)
    except Exception:
        return True
    abs_ok = True
    pct_ok = True
    if "abs" in threshold:
        abs_ok = abs(newf - oldf) >= float(threshold["abs"])
    if "pct" in threshold:
        base = abs(oldf) if abs(oldf) > 0 else 1.0
        pct_ok = abs(newf - oldf) >= base * float(threshold["pct"])
    return abs_ok and pct_ok

def _is_stale(dm_dt: Optional[str], prop_last_dt: Optional[str]) -> bool:
    dm = parse_sf_dt(dm_dt) if dm_dt else None
    pr = parse_sf_dt(prop_last_dt) if prop_last_dt else None
    if dm and pr and dm <= pr:
        return True
    return False

def _should_overwrite(old, new, *, kind: str, policy: str, threshold: dict | None,
                      field_name: str, dm_dt: Optional[str], prop_last_dt: Optional[str]) -> tuple[bool, str]:
    # identical?
    if _same_after_norm(old, new, kind):
        return (False, "skip:identical")

    # junky new?
    if _looks_junky(new, kind):
        return (False, "skip:junky")

    # staleness gate
    if _is_stale(dm_dt, prop_last_dt):
        return (False, "skip:stale")

    # policy
    if policy == "immutable":
        if old in (None, ""):
            return (True, "set:immutable-empty")
        else:
            return (False, "skip:immutable-present")

    if policy == "conservative":
        if old in (None, ""):
            return (True, "set:null-fill")
        # allow overwrite only if genuinely better looking & passes threshold (if any)
        if _threshold_pass(kind, old, new, threshold):
            return (True, "set:conservative-overwrite")
        return (False, "skip:conservative-threshold")

    # normal policy
    if kind == "status":
        if not _allowed_status_change(field_name, old, new):
            return (False, "skip:status-transition")
        return (True, "set:status")

    if not _threshold_pass(kind, old, new, threshold):
        return (False, "skip:threshold")

    return (True, "set:normal")

def build_property_update(dm_record: dict, prop_record: dict) -> tuple[dict, str, list[str]]:
    """
    Build an update dict for Left_Main__Property__c based on a DM row.
    Returns (update_data, change_summary, reasons)
    """
    update_data: dict = {}
    diffs: list[str] = []
    reasons: list[str] = []

    # hard guard: freeze flag on Property
    if prop_record.get("Freeze_Auto_Updates__c"):
        reasons.append("skip:freeze-flag-on-property")
        return {}, "", reasons

    dm_dt = dm_record.get("SystemModstamp")
    prop_last_dt = prop_record.get("Last_Updated_By_Script__c")
    # Inject computed street line into a working copy so FIELD_MAP can set it
    dm_record = dict(dm_record)  # shallow copy so we don't mutate caller's object
    dm_record["Address_Line__computed"] = _build_address_line(dm_record)
    # ---- 1) scalar fields
    for dm_field, cfg in FIELD_MAP.items():
        prop_field = cfg["prop"]
        kind = cfg["kind"]
        policy = cfg["policy"]
        threshold = cfg.get("threshold")

        dm_val = dm_record.get(dm_field)
        prop_val = prop_record.get(prop_field)

        # normalize some kinds
        if kind == "zip" and dm_val:
            dm_val = re.sub(r'\D', '', str(dm_val))[:9]
        if kind in ("date", "datetime") and dm_val:
            # pass through as-is; SOQL/REST will accept ISO or date literal
            pass

        # 0) guard: junk
        if _looks_junky(dm_val, kind):
            reasons.append(f"{prop_field}:skip:junky")
            continue

        ok, why = _should_overwrite(prop_val, dm_val, kind=kind, policy=policy,
                                    threshold=threshold, field_name=prop_field,
                                    dm_dt=dm_dt, prop_last_dt=prop_last_dt)
        reasons.append(f"{prop_field}:{why}")
        if not ok:
            continue

        update_data[prop_field] = dm_val
        # pretty diff for summary
        old_display = str(prop_val) if prop_val not in (None, "") else "∅"
        new_display = str(dm_val)
        # short codes for known fields
        label = {
            "Total_Appraised_Value__c": "AV",
            "Appraised_Value_Improvement__c": "AV_imp",
            "Appraised_Value_Land__c": "AV_land",
            "Mortgage_Balance__c": "Mortgage",
            "Foreclosure_Status__c": "FStatus",
            "Foreclosure_Sale_Date__c": "FSale",
        }.get(prop_field, prop_field)
        diffs.append(f"{label} {old_display}→{new_display}")

    # ---- 2) multipicklists (UNION)
    for dm_field, prop_field in MULTIPICKLIST_MAP.items():
        dm_val = dm_record.get(dm_field)
        if not dm_val:
            continue
        existing = prop_record.get(prop_field, "")
        merged = _union_multipicklist(existing, dm_val)
        if merged != existing:
            # freshness guard for mp too
            if _is_stale(dm_dt, prop_last_dt):
                reasons.append(f"{prop_field}:skip:stale-mp")
            else:
                update_data[prop_field] = merged
                diffs.append(f"{prop_field} +[{dm_val}]")
                reasons.append(f"{prop_field}:set:union")

    # ---- 3) recompute normalized address if we filled parts
    # (Optional) If you use a single address field, refresh from components set this round.
    # Example: if you rely on Left_Main__Address__c assembled elsewhere, skip this.

    # ---- 4) stamp the script-updated time (consumer will add this when calling .update)
    # Leave for caller: update_data['Last_Updated_By_Script__c'] = utcnow().isoformat()

    # ---- 5) build compact change summary (<= 255 chars)
    change_summary = "; ".join(diffs)
    if len(change_summary) > 240:
        change_summary = change_summary[:237] + "..."

    return update_data, change_summary, reasons



# ==================== RESULT MARKING FUNCTIONS ====================
def mark_success(sf: Salesforce, record_id: str, result: ProcessingResult, fingerprint: str = None, change_summary: str = ""):
    # choose created vs updated; if you prefer always 'success', flip logic here
    pick = PLR_CREATED if (result.status and "Created" in result.status) else PLR_UPDATED
    if result.needs_review:
        pick = PLR_REVIEW

    # (optional) human detail for logs; not stored
    detail = result.status or ""
    if result.match_type:
        detail += f" | match_type={result.match_type}"
    if result.match_score:
        detail += f" | score={result.match_score}"
    update_data = {
        'Prop_Last_Run_At__c': sf_datetime_now(),
        'Prop_Last_Result__c': pick,
        'Prop_Last_Error__c': None,
        'Property__c': result.property_id,
        'Fuzzy_Match_Score__c': result.match_score,
        'Attempt_Count__c': 0,
        'Next_Attempt_At__c': None,
        'Change_Summary__c': (change_summary[:255] if change_summary else None),
        'Ready_for_Contacts__c': True,
        
        # 'Hold_Until__c': None,
    }

    # Only mark "Ready for Contacts" true when a brand-new Property was created
    created_new = bool(result.status and "Created New" in result.status)

    if result.needs_review:
        # Needs manual review, so do NOT mark ready for contacts yet
        update_data.update({
            'Needs_Review__c': True,
            'Review_Status__c': 'New',          # lands in your queue
            'Ready_for_Contacts__c': True,     # keep contact worker OFF
        })
    else:
        # No review needed
        update_data.update({
            'Needs_Review__c': False,
            'Review_Status__c': 'Reviewed',                 # considered approved
            'Ready_for_Contacts__c': created_new,           # checkbox true only when a new Property was created
        })
    if FINGERPRINT_ENABLED and fingerprint:
        update_data['Prop_Last_Run_Fingerprint__c'] = fingerprint

    try:
        sf.Data_Management__c.update(record_id, update_data)
    except Exception as e:
        logger.error(f"Error marking success for {record_id}: {e}")


def mark_skipped(sf: Salesforce, record_id: str, reason: str, fingerprint: str = None):
    reason = (reason or "").strip().lower()

    if reason in ("unchanged-fingerprint", "unchanged", "no-change"):
        pick = PLR_SKIPPED_UNCHANGED
    elif reason in ("could-not-acquire-lock", "lock-failed", "lock-not-acquired"):
        pick = PLR_SKIPPED_LOCK
    elif reason in ("property-already-processed-this-cycle", "already-processed", "not-changed-since-last-run"):
        pick = PLR_SKIPPED_ALREADY
    elif reason in ("waiting-for-property", "waiting"):
        pick = PLR_SKIPPED_WAITING
    else:
        # fall back to a generic, allowed skipped value (pick the closest)
        pick = PLR_SKIPPED_ALREADY

    update_data = {
        'Prop_Last_Run_At__c': sf_datetime_now(),
        'Prop_Last_Result__c': pick,
        'Review_Reason__c': reason[:255],
        'Ready_for_Contacts__c': True,
    }
    if FINGERPRINT_ENABLED and fingerprint:
        update_data['Prop_Last_Run_Fingerprint__c'] = fingerprint

    try:
        sf.Data_Management__c.update(record_id, update_data)
    except Exception as e:
        logger.error(f"Error marking skipped for {record_id}: {e}")

def mark_error(sf: Salesforce, record_id: str, error_message: str, attempt_count: int = 0):
    next_attempt = (utcnow() + timedelta(minutes=BACKOFF_MINUTES * (attempt_count + 1))).strftime('%Y-%m-%dT%H:%M:%SZ')
    update_data = {
        'Prop_Last_Run_At__c': sf_datetime_now(),
        'Prop_Last_Result__c': PLR_ERROR,
        'Prop_Last_Error__c': str(error_message)[:32000],
        'Attempt_Count__c': attempt_count + 1,
        'Next_Attempt_At__c': next_attempt,
        'Needs_Review__c': True,
        'Review_Reason__c': f"Processing error after {attempt_count + 1} attempts"[:255],
        # ⬇️ keep gated on error too
        'Ready_for_Contacts__c': True,
    }
    try:
        sf.Data_Management__c.update(record_id, update_data)
    except Exception as e:
        logger.error(f"Error marking error for {record_id}: {e}")
# ==================== MAIN PROCESSING LOGIC ====================
def process_single_record(sf: Salesforce, record: Dict) -> ProcessingResult:
    """
    Process a single data management record through the property pipeline:
    1. Validate data
    2. Find/create property
    3. Update tracking fields
    
    Args:
        sf: Salesforce connection
        record: Data management record to process
        
    Returns:
        ProcessingResult with success status and details
    """
    result = ProcessingResult(success=False)

    try:
        # Step 1: Validate incoming data
        is_valid, validation_errors = validate_property_data(record)
        if not is_valid:
            result.error_message = f"Validation failed: {', '.join(validation_errors)}"
            result.needs_review = True
            return result
        
        # Step 2: Find existing property or create new one
        property_id, match_type, match_score = find_property_match(sf, record)
        result.match_score = match_score
        result.match_type = match_type
      
        if match_type == 'error':
            result.success = False
            result.error_message = "Property match query failed"
            result.needs_review = True
            return result
        
        if match_type == 'needs_review':
            result.needs_review = True
            result.status = f'Potential Duplicate - Score: {match_score}'
            result.property_id = property_id
            
        # elif match_type in ['parcel_match', 'strong_fuzzy_match', 'address_match']:
        #     # Update existing property with new data
        #     update_data = {
        #         'Lead_Source__c': record.get('Lead_Source__c'),
        #         'Last_Updated_By_Script__c': datetime.now().isoformat()
        #     }
            
        #     # Update value fields if present
        #     if record.get('Total_Appraised_Value__c'):
        #         update_data['Total_Appraised_Value__c'] = record.get('Total_Appraised_Value__c')
        #     if record.get('Mortgage_Balance__c'):
        #         update_data['Mortgage_Balance__c'] = record.get('Mortgage_Balance__c')
                
        #     sf.Left_Main__Property__c.update(property_id, update_data)
        #     result.property_id = property_id
        #     result.status = f'Updated ({match_type})'


        elif match_type in ['parcel_match', 'strong_fuzzy_match', 'address_match']:
            # Pull the current Property so we can merge
            prop = sf.Left_Main__Property__c.get(property_id)

            update_data, change_summary, reasons = build_property_update(record, prop)

            # Always bump script timestamp if we changed anything
            # if update_data:
            #     update_data['Last_Updated_By_Script__c'] = utcnow().isoformat()
            #     sf.Left_Main__Property__c.update(property_id, update_data)
            if update_data:
                update_data['Last_Updated_By_Script__c'] = sf_datetime_now()
                sf.Left_Main__Property__c.update(property_id, _safe_prop_payload(sf, update_data))
            # (Optional) log for debugging
            logger.info(f"Property {property_id} changes: {change_summary or 'none'} | reasons={reasons}")

            result.property_id = property_id
            if update_data:
                result.status = f'Updated ({match_type})'
            else:
                result.status = f'Updated ({match_type}) - no material changes'
        else:
            # Create new property record
            property_data = {
                # ===== ADDRESS & LOCATION =====
                'Left_Main__Address__c': _build_address_line(record),
                # 'Street_Address__c': _build_address_line(record),
                'Street_Address__c': record.get('Street_Name__c'),
                'Street_Number__c': record.get('Street_Number__c'),
                'Left_Main__City__c': record.get('City__c'),  # FIXED: was City__c
                'Left_Main__State__c': record.get('State__c'),  # FIXED: was State__c
                'Left_Main__Zip_Code__c': record.get('Zip__c'),
                'Left_Main__County__c': record.get('County__c'),  # FIXED: was County__c
                'Mailing_Address_of_Record__c': record.get('Mailing_Address_of_Record__c'),
                'Mailing_Address_Owner_1__c': record.get('Mailing_Address__c'),
                'Mailing_Address_1__c': record.get('Mailing_Address_1__c'),
                'Mailing_Address_2__c': record.get('Mailing_Address_2__c'),
                'Mailing_Address_3__c': record.get('Mailing_Address_3__c'),
                
                # ===== PROPERTY IDENTIFIERS =====
                'Parcel_ID__c': normalize_parcel_id(record.get('Parcel_ID__c', ''), record.get('County__c', '')),
                'Legal_Description__c': record.get('Legal_Description__c'),
                'Tax_Account_ID__c': record.get('Tax_Account_ID__c'),
                
                # ===== PROPERTY CHARACTERISTICS =====
                'Property_Type__c': record.get('Property_Type__c'),
                'Occupancy_Type__c': record.get('Occupancy_Type__c'),
                'Bedrooms__c': record.get('Bedrooms__c'),
                'Baths__c': record.get('Baths__c'),
                'Year_Built__c': record.get('Year_Built__c'),
                'Sq_Footage__c': record.get('Sq_Footage__c'),
                'Building_Square_Feet__c': record.get('Building_Square_Feet__c'),
                'Lot_Size_SQFT__c': record.get('Lot_Size_SQFT__c'),
                'Lot_Size_Acres__c': record.get('Lot_Size_Acres__c'),
                'Lot_Dimensions_Length__c': record.get('Lot_Dimensions_Length__c'),
                'Lot_Dimensions_Width__c': record.get('Lot_Dimensions_Width__c'),
                'Flood_Zone__c': record.get('Flood_Zone__c'),
                'MLS_Status__c': record.get('MLS_Status__c'),
                
                # ===== FINANCIAL FIELDS =====
                'Total_Appraised_Value__c': record.get('Total_Appraised_Value__c'),
                'Property_Value_Appraised_Value__c': record.get('Total_Appraised_Value__c'),
                'Appraised_Value_Improvement__c': record.get('Appraised_Value_Improvement__c'),
                'Appraised_Value_Land__c': record.get('Appraised_Land_Value__c'),  # Note: DM field is Appraised_Land_Value__c
                'Mortgage_Balance__c': record.get('Mortgage_Balance__c'),
                'Estimated_Equity__c': record.get('Estimated_Equity__c'),
                'Estimated_Loan_Balance__c': record.get('Estimated_Loan_Balance__c'),
                'Original_Loan_Amount__c': record.get('Original_Loan_Amount__c'),
                'Starting_Bid__c': record.get('Starting_Bid__c'),
                'Equity_Percentage_2__c': record.get('Equity_Percentage__c'),  # Note: DM has Equity_Percentage__c
                'Past_Due_Amount__c': record.get('Past_Due_Amount__c'),
                
                # ===== LOAN/MORTGAGE INFORMATION =====
                # 'Loan_Type__c': record.get('Loan_Type__c'),
                'Loan_Type__c': str(record.get('Loan_Type__c', '')),

                'Loan_Term__c': record.get('Loan_Term__c'),
                'Loan_Origin_Date__c': record.get('Loan_Origin_Date__c'),
                'Mortgagee_Name__c': record.get('Mortgagee_Name__c'),
                'Mortgage_Trustee__c': record.get('Trustee__c'),  # Note: DM field is Trustee__c
                # REMOVED: 'Mortgage_Status__c' - doesn't exist on Property
                'Borrower_Name__c': record.get('Full_Name_of_Record__c'),
                
                # ===== FORECLOSURE FIELDS =====
                'Foreclosure_Status__c': record.get('Foreclosure_Status__c'),
                'Foreclosure_Type__c': record.get('Foreclosure_Type__c'),
                'Foreclosure_Sale_Date__c': record.get('Foreclosure_Sale_Date__c'),
                'Property_Foreclosure__c': record.get('Property_Foreclosure__c'),
                'HOA_Foreclosure__c': record.get('HOA_Foreclosure__c'),
                'TAX_Foreclosure__c': record.get('TAX_Foreclosure__c'),
                'Mortgage_Foreclosure__c': record.get('Mortgage_Foreclosure__c'),
                
                # ===== TAX INFORMATION =====
                'Delinquent_Taxes_Found__c': record.get('Delinquent_Taxes_Found__c'),
                # REMOVED: 'Tax_Delinquent__c' - duplicate, already have Delinquent_Taxes_Found__c
                'Tax_Status__c': record.get('Tax_Status__c'),
                # REMOVED: 'Tax_Auction_Date__c' - doesn't exist on Property (confusion with HOA_Auction_Date__c)
                'TAX_Amount_per_year__c': record.get('Tax_Amount_Per_Year__c'),
                # Note: Tax_Type__c is multipicklist, handled separately below
                
                # ===== HOA INFORMATION =====
                # 'HOA_Status__c': record.get('HOA_Status__c'),
                'HOA_Status__c': 'Current' if record.get('HOA_Status__c') == 'Yes' else ('Unknown' if record.get('HOA_Status__c') in ['None', 'Unknown'] else record.get('HOA_Status__c')),

                'HOA_Payment_Status__c': record.get('HOA_Payment_Status__c'),
                'HOA_Amount_per_year__c': record.get('HOA_Amount_per_year__c'),
                # 'HOA_Auction_Date__c': record.get('HOA_Auction_Date__c'),
                
                # ===== LIEN INFORMATION =====
                'Liens__c': record.get('Liens__c'),
                'Lien_Types__c': record.get('Lien_Types__c'),  # Multipicklist
                
                # ===== AUCTION DATES =====
                # 'Mortgage_Auction_Date__c': record.get('Mortgage_Auction_Date__c'),
                'HOA_Auction_Date__c': record.get('Auction_Date__c'),  # Use generic Auction_Date from DM
                'Mortgage_Auction_Date__c': record.get('Mortgage_Auction_Date__c'),  # Use same generic date
                'Time_of_Sale__c': record.get('Time_of_Sale__c'),
                
                # ===== PERSONAL INFORMATION =====
                'Death__c': record.get('Death__c'),
                'Deceased__c': record.get('Deceased__c'),
                'Marital_Status__c': record.get('Marital_Status__c'),
                'Race__c': record.get('Race__c'),
                # Note: Age__c on Property is picklist, Years_Owned__c is also picklist - map carefully
                
                # ===== FLAGS =====
                'High_Equity__c': record.get('High_Equity__c'),
                
                # ===== SOURCE & PROCESSING =====
                'Lead_Source__c': record.get('Lead_Source__c'),
                'Lead_Score__c': record.get('Lead_Score__c'),
                'Created_By_Script__c': True,
                'Last_Updated_By_Script__c': sf_datetime_now(),
            }
            
            # Multipicklists - add separately if present
            if record.get('Foreclosure_Type__c'):
                property_data['Foreclosure_Type__c'] = record.get('Foreclosure_Type__c')
            if record.get('Tax_Type__c'):
                property_data['Tax_Type__c'] = record.get('Tax_Type__c')
            
            # Remove None values
            property_data = {k: v for k, v in property_data.items() if v is not None}
            
            # Filter to valid fields ONLY
            filtered_data = _safe_prop_payload(sf, property_data)
            
            # Log what we're creating
            logger.info(f"Creating Property with {len(filtered_data)} fields")
            if len(filtered_data) != len(property_data):
                dropped = len(property_data) - len(filtered_data)
                logger.warning(f"Dropped {dropped} invalid fields during filtering")
            
            # Create the property
            property_result = sf.Left_Main__Property__c.create(filtered_data)
            property_id = property_result['id']
            result.property_id = property_id
            result.status = 'Created New'

        # else:
        #     # Create new property record
        #     property_data = {
        #         # Address & Location Fields
        #         'Left_Main__Address__c': _build_address_line(record),
        #         'Street_Address__c': _build_address_line(record),  # Duplicate for this field
        #         'City__c': record.get('City__c'),
        #         'State__c': record.get('State__c'),
        #         'Left_Main__Zip_Code__c': record.get('Zip__c'),
        #         'County__c': record.get('County__c'),
        #         'Left_Main__County__c': record.get('County__c'),  # Both county fields
        #         'Street_Number__c': record.get('Street_Number__c'),
        #         'Mailing_Address_of_Record__c': record.get('Mailing_Address_of_Record__c'),
        #         'Mailing_Address_Owner_1__c': record.get('Mailing_Address__c'),
                
        #         # Property Identifiers
        #         'Parcel_ID__c': normalize_parcel_id(record.get('Parcel_ID__c', ''), record.get('County__c', '')),
        #         'Legal_Description__c': record.get('Legal_Description__c'),
        #         'Tax_Account_ID__c': record.get('Tax_Account_ID__c'),
                
        #         # Property Characteristics
        #         'Property_Type__c': record.get('Property_Type__c'),
        #         'Occupancy_Type__c': record.get('Occupancy_Type__c'),
        #         'Bedrooms__c': record.get('Bedrooms__c'),
        #         'Baths__c': record.get('Baths__c'),
        #         'Year_Built__c': record.get('Year_Built__c'),
        #         'Sq_Footage__c': record.get('Sq_Footage__c'),
        #         'Building_Square_Feet__c': record.get('Building_Square_Feet__c'),
        #         'Lot_Size_SQFT__c': record.get('Lot_Size_SQFT__c'),
        #         'Lot_Size_Acres__c': record.get('Lot_Size_Acres__c'),
        #         'Lot_Dimensions_Length__c': record.get('Lot_Dimensions_Length__c'),
        #         'Lot_Dimensions_Width__c': record.get('Lot_Dimensions_Width__c'),
        #         'Flood_Zone__c': record.get('Flood_Zone__c'),
        #         'MLS_Status__c': record.get('MLS_Status__c'),
                
        #         # Financial Fields
        #         'Total_Appraised_Value__c': record.get('Total_Appraised_Value__c'),
        #         'Property_Value_Appraised_Value__c': record.get('Total_Appraised_Value__c'),  # Duplicate
        #         'Appraised_Value_Improvement__c': record.get('Appraised_Value_Improvement__c'),
        #         'Appraised_Value_Land__c': record.get('Appraised_Land_Value__c'),
        #         'Mortgage_Balance__c': record.get('Mortgage_Balance__c'),
        #         'Estimated_Equity__c': record.get('Estimated_Equity__c'),
        #         'Estimated_Loan_Balance__c': record.get('Estimated_Loan_Balance__c'),
        #         'Original_Loan_Amount__c': record.get('Original_Loan_Amount__c'),
        #         'Starting_Bid__c': record.get('Starting_Bid__c'),
        #         'Equity_Percentage_2__c': record.get('Equity_Percentage__c'),
        #         'Past_Due_Amount__c': record.get('Past_Due_Amount__c'),
                
        #         # Loan/Mortgage Information
        #         'Loan_Type__c': record.get('Loan_Type__c'),
        #         'Loan_Term__c': record.get('Loan_Term__c'),
        #         'Loan_Origin_Date__c': record.get('Loan_Origin_Date__c'),
        #         'Mortgagee_Name__c': record.get('Mortgagee_Name__c'),
        #         'Mortgage_Trustee__c': record.get('Trustee__c'),
        #         'Mortgage_Status__c': record.get('Mortgage_Status__c'),
        #         'Borrower_Name__c': record.get('Full_Name_of_Record__c'),
                
        #         # Foreclosure Fields
        #         'Foreclosure_Status__c': record.get('Foreclosure_Status__c'),
        #         'Foreclosure_Type__c': record.get('Foreclosure_Type__c'),
        #         'Foreclosure_Sale_Date__c': record.get('Foreclosure_Sale_Date__c'),
        #         'Property_Foreclosure__c': record.get('Property_Foreclosure__c'),
        #         'HOA_Foreclosure__c': record.get('HOA_Foreclosure__c'),
        #         'TAX_Foreclosure__c': record.get('TAX_Foreclosure__c'),
        #         'Mortgage_Foreclosure__c': record.get('Mortgage_Foreclosure__c'),
                
        #         # Tax Information
        #         'Delinquent_Taxes_Found__c': record.get('Delinquent_Taxes_Found__c'),
        #         'Tax_Delinquent__c': record.get('Tax_Delinquent__c'),  # Both tax delinquent fields
        #         'Tax_Status__c': record.get('Tax_Status__c'),
        #         'Tax_Auction_Date__c': record.get('Tax_Auction_Date__c'),
        #         'TAX_Amount_per_year__c': record.get('Tax_Amount_Per_Year__c'),
        #         'Tax_Type__c': record.get('Tax_Type__c'),
                
        #         # HOA Information
        #         'HOA_Status__c': record.get('HOA_Status__c'),
        #         'HOA_Payment_Status__c': record.get('HOA_Payment_Status__c'),
        #         'HOA_Amount_per_year__c': record.get('HOA_Amount_per_year__c'),
        #         'HOA_Auction_Date__c': record.get('HOA_Auction_Date__c'),
                
        #         # Lien Information
        #         'Liens__c': record.get('Liens__c'),
        #         'Lien_Types__c': record.get('Lien_Types__c'),
                
        #         # Auction Dates
        #         'Mortgage_Auction_Date__c': record.get('Mortgage_Auction_Date__c'),
        #         'Time_of_Sale__c': record.get('Time_of_Sale__c'),
                
        #         # Personal Information
        #         'Death__c': record.get('Death__c'),
        #         'Deceased__c': record.get('Deceased__c'),
        #         'Marital_Status__c': record.get('Marital_Status__c'),
        #         'Race__c': record.get('Race__c'),
        #         'Age__c': record.get('Age__c'),
        #         'Years_Owned__c': record.get('Years_Owned__c'),
                
        #         # Flags
        #         'High_Equity__c': record.get('High_Equity__c'),
                
        #         # Source & Processing
        #         'Lead_Source__c': record.get('Lead_Source__c'),
        #         'Lead_Score__c': record.get('Lead_Score__c'),
        #         'Created_By_Script__c': True,
        #         'Last_Updated_By_Script__c': sf_datetime_now(),

        #     }
            # property_data = {
            #     # identifiers
            #     'Parcel_ID__c': normalize_parcel_id(record.get('Parcel_ID__c', ''), record.get('County__c', '')),

            #     # address on Property uses a single street line + LM city/state/zip/county
            #     'Left_Main__Address__c': _build_address_line(record),   # "123 Main St"
            #     'Left_Main__City__c':    record.get('City__c') or None,
            #     'Left_Main__State__c':   record.get('State__c') or None,
            #     'Left_Main__Zip_Code__c': record.get('Zip__c') or None,
            #     'Left_Main__County__c':  record.get('County__c') or None,

            #     'City__c': record.get('City__c'),                       # Changed from Left_Main__City__c
            #     'State__c': record.get('State__c'),                     # Changed from Left_Main__State__c
            #     'Left_Main__Zip_Code__c': record.get('Zip__c'),        # This one is correct
            #     'County__c': record.get('County__c'),                   # Changed from Left_Main__County__c
                
            #     # This field exists on Property
            #     'Street_Number__c': record.get('Street_Number__c'),
                
            #     # Other property details
            #     'Legal_Description__c': record.get('Legal_Description__c'),
            #     'Total_Appraised_Value__c': record.get('Total_Appraised_Value__c'),
            #     'Appraised_Value_Improvement__c': record.get('Appraised_Value_Improvement__c'),
            #     'Appraised_Value_Land__c': record.get('Appraised_Land_Value__c'),  # Note field name difference
            #     'Mortgage_Balance__c': record.get('Mortgage_Balance__c'),
            #     'Lead_Source__c': record.get('Lead_Source__c'),
            #     'Foreclosure_Status__c': record.get('Foreclosure_Status__c'),
            #     'Delinquent_Taxes_Found__c': record.get('Delinquent_Taxes_Found__c') or False,
                
            #     # Additional fields from Data Management that exist on Property
            #     'Bedrooms__c': record.get('Bedrooms__c'),
            #     'Baths__c': record.get('Baths__c'),
            #     'Year_Built__c': record.get('Year_Built__c'),
            #     'Sq_Footage__c': record.get('Sq_Footage__c'),
            #     'Property_Type__c': record.get('Property_Type__c'),
            #     'Occupancy_Type__c': record.get('Occupancy_Type__c'),
            #     'Foreclosure_Type__c': record.get('Foreclosure_Type__c'),
            #     'Foreclosure_Sale_Date__c': record.get('Foreclosure_Sale_Date__c'),
            #     'Tax_Auction_Date__c': record.get('Tax_Auction_Date__c'),
            #     'HOA_Auction_Date__c': record.get('HOA_Auction_Date__c'),
            #     'Mortgage_Auction_Date__c': record.get('Mortgage_Auction_Date__c'),
            #     'HOA_Foreclosure__c': record.get('HOA_Foreclosure__c'),
            #     'TAX_Foreclosure__c': record.get('TAX_Foreclosure__c'),
            #     'Mortgage_Foreclosure__c': record.get('Mortgage_Foreclosure__c'),
            #     'Property_Foreclosure__c': record.get('Property_Foreclosure__c'),
            #     'HOA_Payment_Status__c': record.get('HOA_Payment_Status__c'),
            #     'Mortgage_Status__c': record.get('Mortgage_Status__c'),
            #     'Lien_Types__c': record.get('Lien_Types__c'),
            #     'Tax_Type__c': record.get('Tax_Type__c'),
                
            #     # Script tracking
            #     'Created_By_Script__c': True,
            #     'Last_Updated_By_Script__c': sf_datetime_now(),
            # }
            # drop Nones so we don't try to set empty fields unnecessarily
            # property_data = {k: v for k, v in property_data.items() if v is not None}
            # # LOG BEFORE FILTERING
            # logger.info(f"Property data BEFORE filtering (count={len(property_data)}): {list(property_data.keys())}")
            # # Filter to valid fields
            # filtered_data = _safe_prop_payload(sf, property_data)
            # # LOG AFTER FILTERING  
            # logger.info(f"Property data AFTER filtering (count={len(filtered_data)}): {list(filtered_data.keys())}")
            # logger.info(f"Filtered data values: {filtered_data}")
            # # Also log what fields exist on Property object
            # prop_fields = _property_fields(sf)
            # logger.info(f"Available Property fields (first 20): {sorted(list(prop_fields))[:20]}")



                        
            # # Add individual address components if available
            # if record.get('Street_Number__c'):
            #     property_data['Street_Number__c'] = record.get('Street_Number__c')
            # # if record.get('Street_Name__c'):
            # #     property_data['Street_Name__c'] = record.get('Street_Name__c')
            # if record.get('City__c'):
            #     property_data['City__c'] = record.get('City__c')
            # if record.get('State__c'):
            #     property_data['State__c'] = record.get('State__c')
            # if record.get('Zip__c'):
            #     property_data['Zip__c'] = record.get('Zip__c')
                
            # property_result = sf.Left_Main__Property__c.create(_safe_prop_payload(sf, property_data))    
            # property_result = sf.Left_Main__Property__c.create(filtered_data)
        
            # property_id = property_result['id']
            # result.property_id = property_id
            # result.status = 'Created New'

        result.success = True
        logger.info(f"Successfully processed record {record['Id']}: {result.status}")

    except Exception as e:
        err_msg = str(e) if str(e).strip() else "Unknown error occurred during processing"
        logger.error(f"Error processing record {record.get('Id', 'UNKNOWN_ID')}: {err_msg}", exc_info=True)
        result.error_message = err_msg
        result.needs_review = True

    return result

def process_batch(sf: Salesforce) -> Dict[str, int]:
    """
    Process a batch of unprocessed Data Management records
    Using timestamp watermarks instead of simple Processed__c flag
    
    Returns:
        Dictionary with processing statistics
    """
    stats = {
        'queried': 0,
        'processed': 0,
        'successful': 0,
        'errors': 0,
        'skipped': 0,
        'needs_review': 0,
        'properties_created': 0,
        'properties_updated': 0
    }
    
    # Track properties processed in this cycle to avoid duplicates
    processed_properties: Set[str] = set()

    try:
    # Query using watermark pattern - records that haven't been processed or have changed
        # IMPORTANT: SOQL cannot compare two fields, so we use a rolling lookback window
        now_iso = utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        query = f"""
        SELECT Id, SystemModstamp, Property__c,
            Prop_Last_Run_At__c, Prop_Last_Run_Fingerprint__c, 
            Attempt_Count__c, Next_Attempt_At__c,
            Processing_Lock__c, Lock_Owner__c, Lock_Expires_At__c,
            Parcel_ID__c, Street_Number__c, Street_Name__c, City__c, State__c, 
            Zip__c, Legal_Description__c, County__c, Lead_Source__c,
            Total_Appraised_Value__c, Mortgage_Balance__c,
            Foreclosure_Status__c, Delinquent_Taxes_Found__c,
            Property_Type__c, Bedrooms__c, Baths__c, Year_Built__c, Sq_Footage__c,
            Occupancy_Type__c, Building_Square_Feet__c, Lot_Size_SQFT__c, 
            Lot_Size_Acres__c, Lot_Dimensions_Length__c, Lot_Dimensions_Width__c,
            Flood_Zone__c, MLS_Status__c, High_Equity__c,
            Appraised_Value_Improvement__c, Appraised_Land_Value__c,
            Estimated_Equity__c, Estimated_Loan_Balance__c,
            Original_Loan_Amount__c, Starting_Bid__c, Equity_Percentage__c,
            Past_Due_Amount__c, Loan_Type__c, Loan_Term__c, Loan_Origin_Date__c,
            Mortgagee_Name__c, Trustee__c, Full_Name_of_Record__c,
            Foreclosure_Type__c, Foreclosure_Sale_Date__c,
            Property_Foreclosure__c, HOA_Foreclosure__c, TAX_Foreclosure__c,
            Mortgage_Foreclosure__c, Tax_Status__c, Tax_Amount_Per_Year__c,
            HOA_Status__c, HOA_Payment_Status__c, HOA_Amount_per_year__c,
            Auction_Date__c, Liens__c, Lien_Types__c,
            Time_of_Sale__c, Mortgage_Auction_Date__c,
            Death__c, Deceased__c, Marital_Status__c, Race__c, Age__c,
            Years_Owned__c, Lead_Score__c, Tax_Account_ID__c,
            Mailing_Address__c, Mailing_Address_of_Record__c,
            Mailing_Address_1__c, Mailing_Address_2__c, Mailing_Address_3__c
        FROM Data_Management__c
        WHERE (Prop_Last_Run_At__c = NULL OR SystemModstamp >= LAST_N_DAYS:{LOOKBACK_DAYS})
        AND (Next_Attempt_At__c = NULL OR Next_Attempt_At__c <= LAST_N_DAYS:0)

        ORDER BY SystemModstamp ASC
        LIMIT {BATCH_SIZE}
        """.strip()

        # (Optional) safety: strip any trailing punctuation
        import re as _re
        query = _re.sub(r'[;\.\s]+$', '', query)

        logger.debug("FINAL SOQL:\n%s", query)
        records = sf.query_all(query)['records']

        stats['queried'] = len(records)
        logger.info(f"Found {len(records)} records to process")

        for record in records:
            record_id = record['Id']
            
            # # Check if property already processed in this cycle
            # existing_property = record.get('Property__c')
            # if existing_property in processed_properties:
            #     mark_skipped(sf, record_id, 'property-already-processed-this-cycle')
            #     stats['skipped'] += 1
            #     continue
            existing_property = record.get('Property__c')

            # Only de-dupe when we actually have a Property Id
            if existing_property and existing_property in processed_properties:
                mark_skipped(sf, record_id, 'property-already-processed-this-cycle')
                stats['skipped'] += 1
                continue
            # Precise field-to-field check in Python (SOQL can't do this)
            last_run_at = parse_sf_dt(record.get('Prop_Last_Run_At__c'))
            sys_mod = parse_sf_dt(record.get('SystemModstamp'))

            if last_run_at and sys_mod and sys_mod <= last_run_at:
                # Nothing changed since last successful run; skip quietly
                mark_skipped(sf, record['Id'], 'not-changed-since-last-run', record.get('Prop_Last_Run_Fingerprint__c'))
                stats['skipped'] += 1
                continue

            
            # Check fingerprint if enabled
            if FINGERPRINT_ENABLED:
                current_fingerprint = compute_property_fingerprint(record)
                last_fingerprint = record.get('Prop_Last_Run_Fingerprint__c')
                
                if current_fingerprint == last_fingerprint:
                    mark_skipped(sf, record_id, 'unchanged-fingerprint', current_fingerprint)
                    stats['skipped'] += 1
                    continue
            else:
                current_fingerprint = None
            
            # Try to acquire lock if enabled
            if not try_acquire_lock(sf, record_id):
                mark_skipped(sf, record_id, 'could-not-acquire-lock')
                stats['skipped'] += 1
                continue
            
            try:
                # Process the record
                stats['processed'] += 1
                result = process_single_record(sf, record)
                
                if result.success:
                    stats['successful'] += 1
                    if 'Created' in result.status:
                        stats['properties_created'] += 1
                    elif 'Updated' in result.status:
                        stats['properties_updated'] += 1
                    
                    # Track processed property
                    if result.property_id:
                        processed_properties.add(result.property_id)
                    
                    # Mark success in Data_Management__c
                    mark_success(sf, record_id, result, current_fingerprint)
                else:
                    stats['errors'] += 1
                    attempt_count = record.get('Attempt_Count__c', 0) or 0
                    mark_error(sf, record_id, result.error_message, attempt_count)
                
                if result.needs_review:
                    stats['needs_review'] += 1
                    
            finally:
                # Always release lock
                release_lock(sf, record_id)

        logger.info(f"Batch complete. Processed: {stats['processed']}, "
                   f"Successful: {stats['successful']}, Errors: {stats['errors']}, "
                   f"Skipped: {stats['skipped']}")

    except Exception as e:
        logger.error(f"Error in batch processing: {e}", exc_info=True)
        stats['errors'] += 1

    return stats

# ==================== MAIN EXECUTION ====================
def main():
    """Main execution function for single run"""
    try:
        # Connect to Salesforce
        sf = connect_to_salesforce()
        logger.info("Connected to Salesforce successfully")
        
        # Process records
        stats = process_batch(sf)
        
        # Print summary
        print("\n" + "="*50)
        print("PROPERTY PROCESSING SUMMARY")
        print("="*50)
        print(f"Records Queried: {stats['queried']}")
        print(f"Records Processed: {stats['processed']}")
        print(f"Successful: {stats['successful']}")
        print(f"Errors: {stats['errors']}")
        print(f"Skipped: {stats['skipped']}")
        print(f"Needs Review: {stats['needs_review']}")
        print(f"Properties Created: {stats['properties_created']}")
        print(f"Properties Updated: {stats['properties_updated']}")
        print("="*50)
        
        return stats
        
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")
        raise

def run_continuous():
    """Continuous execution loop for deployment"""
    logger.info(f"Starting continuous property processor ({WORKER_ID})...")
    
    while True:
        try:
            # Connect/reconnect to Salesforce
            sf = connect_to_salesforce()
            logger.info("Connected to Salesforce")
            
            # Process batch
            stats = process_batch(sf)
            
            # Log summary
            if stats['processed'] > 0:
                logger.info(f"Cycle complete: Processed={stats['processed']}, "
                           f"Success={stats['successful']}, Errors={stats['errors']}, "
                           f"Skipped={stats['skipped']}")
            else:
                logger.debug("No records to process in this cycle")
                
        except Exception as e:
            logger.error(f"Error during scheduled execution: {e}", exc_info=True)
        
        # Wait before next cycle
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    # Check if running in continuous mode (for deployment) or single run (for testing)
    if getenv("CONTINUOUS_MODE", "true").lower() == "true":
        run_continuous()
    else:
        # Single run for testing
        main()
