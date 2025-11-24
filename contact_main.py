from datetime import datetime, timedelta, timezone
import hashlib, json
import re
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from os import getenv
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from fuzzywuzzy import fuzz
import time, os, fcntl, sys, signal

LOCK_PATH = "/tmp/urp-contact.lock"
SLEEP_SEC = 20
_SHUTDOWN = False

def _sigterm(_s, _f):
    global _SHUTDOWN
    _SHUTDOWN = True

def _try_lock(path):
    f = open(path, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(str(os.getpid()))
        f.flush()
        return f  # keep open to hold the lock
    except BlockingIOError:
        f.close()
        return None
    

"""
United Realty Partners - Contact-Only Processing Engine (Mode A)
---------------------------------------------------------------
- Processes Data_Management__c rows that already have Property__c.
- Extracts Owners + Relatives -> upserts Lead (Role: Owner/Co-Owner/Relative) linked to Property.
- Upserts Contact_Method__c per phone (source of truth).
- Matching order: Email (exact) -> Phone (exact) -> Fuzzy Name (same Property).
- Lead.Phone is set ONLY when a Contact Method is Verified – Valid (Mode A).
- Optional: LeadSource mapped from Data_Management__c.Lead_Source__c to standard LeadSource.

Author: Path Insights Solutions Team
"""


def _sigterm(_s, _f):
    global _SHUTDOWN
    _SHUTDOWN = True

def _try_lock(path):
    f = open(path, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        f.write(str(os.getpid()))
        f.flush()
        return f  # keep open to hold the lock
    except BlockingIOError:
        f.close()
        return None


CONTACT_LOOKBACK_DAYS = int(getenv("CONTACT_LOOKBACK_DAYS", "3"))
BACKOFF_MINUTES = int(getenv("CONTACT_BACKOFF_MINUTES", "5"))
CONTACT_DEFERRAL_DAYS = int(getenv("CONTACT_DEFERRAL_DAYS", "30"))

# CONTACT_FINGERPRINT_FIELDS = [
#     # owners
#     "First_Name__c","Last_Name__c","Email_s__c",
#     "Phone_1__c","Phone_2__c","Phone_3__c","Phone_4__c","Phone_5__c",
#     "Owner_1_Phone_1_Type__c","Owner_1_Phone_2_Type__c","Owner_1_Phone_3_Type__c",
#     "Owner_1_Phone_4_Type__c","Owner_1_Phone_5_Type__c",
#     "Owner_2_First_Name__c","Owner_2_Last_Name__c","Owner_2_Email__c",
#     "Owner_2_Phone_1__c","Owner_2_Phone_2__c","Owner_2_Phone_3__c","Owner_2_Phone_4__c","Owner_2_Phone_5__c",
#     "Owner_2_Phone_1_Type__c","Owner_2_Phone_2_Type__c","Owner_2_Phone_3_Type__c","Owner_2_Phone_4_Type__c","Owner_2_Phone_5_Type__c",
# ] + sum(([  # relatives 01..10
#     f"Relative_{i:02d}_First_Name__c", f"Relative_{i:02d}_Last_Name__c", f"Relative_{i:02d}_Email__c",
#     f"Relative_{i:02d}_Phone_1__c", f"Relative_{i:02d}_Phone_1_Type__c",
#     f"Relative_{i:02d}_Phone_2__c", f"Relative_{i:02d}_Phone_2_Type__c",
#     f"Relative_{i:02d}_Phone_3__c", f"Relative_{i:02d}_Phone_3_Type__c",
#     f"Relative_{i:02d}_Phone_4__c", f"Relative_{i:02d}_Phone_4_Type__c",
#     f"Relative_{i:02d}_Phone_5__c", f"Relative_{i:02d}_Phone_5_Type__c",
#     f"Relative_{i:02d}_Do_Not_Call__c", f"Relative_{i:02d}_Do_Not_SMS__c", f"Relative_{i:02d}_Do_Not_Email__c",
#     f"Relative_{i:02d}_Notes__c",
# ] for i in range(1,11)), [])

CONTACT_FINGERPRINT_FIELDS = [
    # owners
    "First_Name__c","Last_Name__c","Email_s__c",
    "Phone_1__c","Phone_2__c","Phone_3__c","Phone_4__c","Phone_5__c",
    "Owner_2_First_Name__c","Owner_2_Last_Name__c","Owner_2_Email__c",
    "Owner_2_Phone_1__c","Owner_2_Phone_2__c","Owner_2_Phone_3__c","Owner_2_Phone_4__c","Owner_2_Phone_5__c",
] + sum(([  # relatives 01..10
    f"Relative_{i:02d}_First_Name__c", f"Relative_{i:02d}_Last_Name__c", f"Relative_{i:02d}_Email__c",
    f"Relative_{i:02d}_Phone_1__c",
    f"Relative_{i:02d}_Phone_2__c",
    f"Relative_{i:02d}_Phone_3__c",
    f"Relative_{i:02d}_Phone_4__c",
    f"Relative_{i:02d}_Phone_5__c",
    f"Relative_{i:02d}_Do_Not_Call__c", f"Relative_{i:02d}_Do_Not_SMS__c", f"Relative_{i:02d}_Do_Not_Email__c",
    # f"Relative_{i:02d}_Notes__c",   # <-- remove this line if Notes changes should NOT retrigger processing
] for i in range(1,11)), [])

def contact_fingerprint(dm: dict) -> str:
    subset = {k: dm.get(k) for k in CONTACT_FINGERPRINT_FIELDS}
    return hashlib.sha1(json.dumps(subset, sort_keys=True, separators=(',',':')).encode()).hexdigest()

def _utcnow_iso() -> str:
    # timezone-aware, no deprecation warning
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ===== Contact_Method__c batching (sObject Collections) =====
from typing import List

_METHOD_BUFFER: List[dict] = []
METHOD_BATCH_SIZE = 200  # API limit for composite/sobjects

def _flush_methods(sf) -> int:
    """
    POST /composite/sobjects for up to 200 Contact_Method__c rows.
    Returns count of successful creates in this flush.
    Keeps buffer on failure so caller can retry.
    """
    global _METHOD_BUFFER
    if not _METHOD_BUFFER:
        return 0

    chunk = _METHOD_BUFFER[:METHOD_BATCH_SIZE]
    payload = {
        "allOrNone": False,
        "records": [
            {"attributes": {"type": "Contact_Method__c"}, **row}
            for row in chunk
        ]
    }

    try:
        resp = sf.restful("composite/sobjects", method="POST", json=payload)

        # Normalize both possible shapes:
        if isinstance(resp, dict):
            results = resp.get("results", [])
        elif isinstance(resp, list):
            results = resp
        else:
            log.error("Unexpected composite response type: %s | %r", type(resp), resp)
            return 0

        # Tally successes and log failures (without raising)
        created = 0
        for r in results:
            if isinstance(r, dict) and r.get("success"):
                created += 1
            else:
                # r may be {'success': False, 'errors': [...], 'id': None}
                try:
                    log.warning("Composite upsert error (Contact_Method__c): %s", r)
                except Exception:
                    pass

        # Drop the chunk we just sent (regardless of partial failures)
        _METHOD_BUFFER = _METHOD_BUFFER[METHOD_BATCH_SIZE:]
        return created

    except Exception as e:
        # Keep buffer for retry and log the exception
        log.exception("Composite /composite/sobjects failed: %s", e)
        return 0





def _flush_all_methods(sf) -> int:
    """Flushes the entire buffer in chunks of 200. Returns total successful creates."""
    total = 0
    while _METHOD_BUFFER:
        total += _flush_methods(sf)
    return total


# ==================== ENV / LOGGING ====================
load_dotenv()
SF_USERNAME = getenv("SF_USERNAME")
SF_PASSWORD = getenv("SF_PASSWORD")
SF_TOKEN    = getenv("SF_TOKEN")
SF_DOMAIN   = getenv("SF_DOMAIN", "test")  # 'login' for prod

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("contact_only")

_PROPERTY_LEAD_CACHE: Dict[str, List[Dict]] = {}


# ==================== CONSTANTS ========================
NAME_STRONG = 92
NAME_REVIEW = 82

ROLE_OWNER    = "Owner"
ROLE_COOWNER  = "Co-Owner"
ROLE_RELATIVE = "Relative"

# Picklist values should match Contact_Method__c.Phone_Type__c
# PHONE_TYPES = {"Mobile", "Home", "Work", "Main", "Work Fax", "Private Fax", "Other"}

# Relatives supported (01..10)
RELATIVE_INDEXES = [f"{i:02d}" for i in range(1, 11)]

# LeadSource allowed (your standard picklist)
LEAD_SOURCE_ALLOWED = {
    "Google Ads", "Facebook", "Direct Mail -- Equity", "Direct Mail -- Vacant", "D4$",
    "Referral", "NTSMHF", "Employee Referral", "External Referral", "Partner",
    "Web", "Word of mouth", "Other"
}

# ==================== MODELS ===========================
@dataclass
class Person:
    first_name: str
    last_name: str
    role: str
    emails: List[str]
    # phones: List[str]  # normalized numbers only
    phones: List[Tuple[str, str]]  # (phone, type) tuples
    dnc_call: Optional[bool] = None
    dnc_sms: Optional[bool] = None
    dnc_email: Optional[bool] = None
    notes: Optional[str] = None
    age: Optional[int] = None

# ==================== CONNECTION =======================
def connect() -> Salesforce:
    return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN, domain=SF_DOMAIN)

# ==================== UTILITIES ========================
def soql_escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("'", "\\'")

def norm_email(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def digits_only(s: Optional[str]) -> str:
    return re.sub(r"\D", "", s or "")

def norm_phone(us_phone: Optional[str]) -> str:
    d = digits_only(us_phone)
    if len(d) >= 10:
        return d[-10:]  # US last-10
    return d

def name_key(fn: str, ln: str) -> str:
    s = f"{(fn or '').lower()} {(ln or '').lower()}".strip()
    return re.sub(r"[^a-z ]", "", s)

def fuzzy_score_name(a_fn: str, a_ln: str, b_fn: str, b_ln: str) -> int:
    return fuzz.token_sort_ratio(name_key(a_fn, a_ln), name_key(b_fn, b_ln))

DM_TO_STD_LEADSOURCE = {
    "Auction.com": "Other",
    "County_Records": "Other",
    "Drive_for_Dollar": "Other",
    "Foreclosure_Houston": "Other",
    "Linebarger": "Other",
    "Tax_Lien_List": "Other",
}

def map_lead_source(dm_val: Optional[str]) -> Optional[str]:
    if not dm_val:
        return None
    v = dm_val.strip()
    # If the org’s LeadSource picklist already contains these, return v directly.
    # Otherwise map with fallback:
    return DM_TO_STD_LEADSOURCE.get(v, v if v in LEAD_SOURCE_ALLOWED else "Other")


# ==================== SCHEMA HELPERS ====================
# Prospect record type cache
_PROSPECT_RTID = None
def get_prospect_record_type_id(sf) -> Optional[str]:
    global _PROSPECT_RTID
    if _PROSPECT_RTID is None:
        q = "SELECT Id FROM RecordType WHERE SObjectType='Lead' AND DeveloperName='TRUE_Prospect' LIMIT 1"
        r = sf.query(q)
        _PROSPECT_RTID = r["records"][0]["Id"] if r["records"] else None
    return _PROSPECT_RTID

# Describe cache + payload remapper
_LEAD_FIELDS_CACHE = None
def _lead_fields(sf) -> set:
    global _LEAD_FIELDS_CACHE
    if _LEAD_FIELDS_CACHE is None:
        desc = sf.Lead.describe()
        _LEAD_FIELDS_CACHE = {f["name"] for f in desc["fields"]}
    return _LEAD_FIELDS_CACHE

def _map_lead_payload_to_existing_fields(sf, payload: dict) -> dict:
    fields = _lead_fields(sf)
    out = dict(payload)

    # Role__c -> Title fallback
    if "Role__c" in out and "Role__c" not in fields:
        if "Title" in fields and out["Role__c"]:
            out["Title"] = out["Role__c"]
        out.pop("Role__c", None)

    # Property__c -> Left_Main__Property__c fallback
    if "Property__c" in out and "Property__c" not in fields:
        if "Left_Main__Property__c" in fields:
            out["Left_Main__Property__c"] = out["Property__c"]
        out.pop("Property__c", None)

    # Consent fallbacks to standard fields
    if "Do_Not_Call__c" in out and "Do_Not_Call__c" not in fields:
        if "DoNotCall" in fields:
            out["DoNotCall"] = bool(out["Do_Not_Call__c"])
        out.pop("Do_Not_Call__c", None)

    if "Do_Not_Email__c" in out and "Do_Not_Email__c" not in fields:
        if "HasOptedOutOfEmail" in fields:
            out["HasOptedOutOfEmail"] = bool(out["Do_Not_Email__c"])
        out.pop("Do_Not_Email__c", None)

    if "Do_Not_SMS__c" in out and "Do_Not_SMS__c" not in fields:
        sms_keys = [k for k in ("HasOptedOutOfSMS", "HasOptedOutOfSms") if k in fields]
        if sms_keys:
            out[sms_keys[0]] = bool(out["Do_Not_SMS__c"])
        out.pop("Do_Not_SMS__c", None)

    # Drop anything not on Lead (except RecordTypeId which we set on create)
    out = {k: v for k, v in out.items() if k in fields or k == "RecordTypeId"}

    # Ensure Company exists (required on create; we also guard in create path)
    if "Company" in fields and not out.get("Company"):
        out["Company"] = "Individual Property Owner"

    return out

def _lead_property_where_clause(sf: Salesforce, prop_id: str) -> str:
    fields = _lead_fields(sf)
    if "Property__c" in fields:
        return f"Property__c = '{soql_escape(prop_id)}'"
    if "Left_Main__Property__c" in fields:
        return f"Left_Main__Property__c = '{soql_escape(prop_id)}'"
    # Fallback (shouldn't happen): no property field on Lead
    return "Id != null"

# ==================== EXTRACTION ========================
def _split_emails(multi: Optional[str]) -> List[str]:
    if not multi:
        return []
    return [e.strip().lower() for e in re.split(r"[;, ]+", multi) if e.strip()]

def extract_owner(dm: Dict, owner_idx: int) -> Optional[Person]:
    """
    owner_idx = 1 or 2
    Uses: First_Name__c/Last_Name__c and Owner_2_* for idx=2
    Types: Owner_1_Phone_i_Type__c / Owner_2_Phone_i_Type__c (i = 1..5)
    """
    if owner_idx == 1:
        fn = dm.get("First_Name__c") or ""
        ln = dm.get("Last_Name__c") or ""
        email_field = "Email_s__c"
        phone_prefix = "Phone_"
        type_prefix  = "Owner_1_Phone_"
        role = ROLE_OWNER
        dnc_call = dnc_sms = dnc_email = None
        age = dm.get("Age__c")
    else:
        fn = dm.get("Owner_2_First_Name__c") or ""
        ln = dm.get("Owner_2_Last_Name__c") or ""
        email_field = "Owner_2_Email__c"
        phone_prefix = "Owner_2_Phone_"
        type_prefix  = "Owner_2_Phone_"
        role = ROLE_COOWNER if (dm.get("First_Name__c") or dm.get("Last_Name__c")) else ROLE_OWNER
        dnc_call = dnc_sms = dnc_email = None
        # dm.get("Owner_2_Age__c")
        age = dm.get("Owner_2_Age__c")   # <-- add this line

    # skip if nothing present
    if not any([fn, ln, dm.get(email_field)] + [dm.get(f"{phone_prefix}{i}__c") for i in range(1, 6)]):
        return None

    # emails = _split_emails(dm.get(email_field))
    # phones: List[str] = []
    # for i in range(1, 6):
    #     raw = dm.get(f"{phone_prefix}{i}__c")
    #     if not raw:
    #         continue
    #     n = norm_phone(raw)
    #     if not n:
    #         continue
    #     phones.append(n)

    emails = _split_emails(dm.get(email_field))
    phones: List[Tuple[str, str]] = []
    for i in range(1, 6):
        raw = dm.get(f"{phone_prefix}{i}__c")
        if not raw:
            continue
        n = norm_phone(raw)
        if not n:
            continue
        ptype = dm.get(f"{type_prefix}{i}_Type__c") or "Other"
        phones.append((n, ptype))

    return Person(
        first_name=fn, last_name=ln, role=role,
        emails=emails, phones=phones,
        dnc_call=dnc_call, dnc_sms=dnc_sms, dnc_email=dnc_email, age=age
    )

def extract_relatives(dm: Dict) -> List[Person]:
    people: List[Person] = []
    for idx in RELATIVE_INDEXES:
        fn = dm.get(f"Relative_{idx}_First_Name__c") or ""
        ln = dm.get(f"Relative_{idx}_Last_Name__c") or ""
        email = dm.get(f"Relative_{idx}_Email__c")
        any_phone = any(dm.get(f"Relative_{idx}_Phone_{i}__c") for i in range(1, 6))
        if not any([fn, ln, email, any_phone]):
            continue

        # emails = [norm_email(email)] if email else []
        # # phones: List[Tuple[str, str]] = []
        # # for i in range(1, 6):
        # #     raw = dm.get(f"Relative_{idx}_Phone_{i}__c")
        # #     if not raw:
        # #         continue
        # #     n = norm_phone(raw)
        # #     if not n:
        # #         continue
        # #     ptype = dm.get(f"Relative_{idx}_Phone_{i}_Type__c") or "Other"
        # #     if ptype not in PHONE_TYPES:
        # #         ptype = "Other"
        # #     phones.append((n, ptype))

        # phones: List[str] = []
        # for i in range(1, 6):
        #     raw = dm.get(f"Relative_{idx}_Phone_{i}__c")
        #     if not raw:
        #         continue
        #     n = norm_phone(raw)
        #     if not n:
        #         continue
        #     phones.append(n)

        emails = [norm_email(email)] if email else []
        phones: List[Tuple[str, str]] = []
        for i in range(1, 6):
            raw = dm.get(f"Relative_{idx}_Phone_{i}__c")
            if not raw:
                continue
            n = norm_phone(raw)
            if not n:
                continue
            ptype = dm.get(f"Relative_{idx}_Phone_{i}_Type__c") or "Other"
            phones.append((n, ptype))

        people.append(Person(
            first_name=fn, last_name=ln, role=ROLE_RELATIVE,
            emails=emails, phones=phones,
            dnc_call=dm.get(f"Relative_{idx}_Do_Not_Call__c"),
            dnc_sms=dm.get(f"Relative_{idx}_Do_Not_SMS__c"),
            dnc_email=dm.get(f"Relative_{idx}_Do_Not_Email__c"),
            notes=dm.get(f"Relative_{idx}_Notes__c")
        ))
    return people

def extract_people(dm: Dict) -> List[Person]:
    people: List[Person] = []
    o1 = extract_owner(dm, 1)
    if o1: people.append(o1)
    o2 = extract_owner(dm, 2)
    if o2: people.append(o2)
    people.extend(extract_relatives(dm))
    return people

# ==================== MATCH / UPSERT ====================
_PROPERTY_EMAIL_CACHE: Dict[str, Dict[str, str]] = {}  # prop_id -> {email: leadId}

def find_lead_match(sf: Salesforce, prop_id: str, person: Person) -> Tuple[Optional[str], str, int]:
    # property-scoped email cache
    if person.emails:
        cache = _PROPERTY_EMAIL_CACHE.get(prop_id)
        if cache is None:
            where_prop = _lead_property_where_clause(sf, prop_id)
            q = f"SELECT Id, Email FROM Lead WHERE {where_prop} AND Email != null LIMIT 500"
            recs = sf.query_all(q)["records"]
            cache = { (r.get("Email") or "").lower() : r["Id"] for r in recs }
            _PROPERTY_EMAIL_CACHE[prop_id] = cache
        for e in person.emails:
            if cache.get(e):
                return cache[e], "Email", 100
    # # 2) Phone exact via Contact_Method__c then Lead.Phone
    # for p, _ in person.phones:
    #     if not p:
    #         continue
    #     # qc = f"SELECT Lead__c FROM Contact_Method__c WHERE Phone__c = '{soql_escape(p)}' LIMIT 1"
    #     # rc = sf.query(qc)
    #     # if rc["records"]:
    #     #     return rc["records"][0]["Lead__c"], "Phone", 100
    #     ql = f"SELECT Id FROM Lead WHERE Phone = '{soql_escape(p)}' LIMIT 1"
    #     rl = sf.query(ql)
    #     if rl["records"]:
    #         return rl["records"][0]["Id"], "Phone", 100

    # # 2) Phone exact via Lead.Phone
    # for p in person.phones:
    #     if not p:
    #         continue
    #     ql = f"SELECT Id FROM Lead WHERE Phone = '{soql_escape(p)}' LIMIT 1"
    #     rl = sf.query(ql)
    #     if rl["records"]:
    #         return rl["records"][0]["Id"], "Phone", 100

    # 2) Phone exact via Lead.Phone (scoped to the same property)
    where_prop = _lead_property_where_clause(sf, prop_id)
    # for p in person.phones:
    for p, _ in person.phones:  # Unpack tuple, ignore type
        if not p:
            continue
        ql = f"SELECT Id FROM Lead WHERE {where_prop} AND Phone = '{soql_escape(p)}' LIMIT 1"
        rl = sf.query(ql)
        if rl["records"]:
            return rl["records"][0]["Id"], "Phone", 100


    # 3) Fuzzy name within the same property
    
    if not (person.first_name or person.last_name):
        return None, "None", 0

    recs = _PROPERTY_LEAD_CACHE.get(prop_id)
    if recs is None:
        where_prop = _lead_property_where_clause(sf, prop_id)
        qf = f"""
            SELECT Id, FirstName, LastName
            FROM Lead
            WHERE {where_prop}
            AND (FirstName != null OR LastName != null)
            LIMIT 500
        """
        recs = sf.query_all(qf)["records"]
        _PROPERTY_LEAD_CACHE[prop_id] = recs

    best_id, best_score = None, 0
    for rec in recs:
        score = fuzzy_score_name(person.first_name, person.last_name, rec.get("FirstName",""), rec.get("LastName",""))
        if score > best_score:
            best_id, best_score = rec["Id"], score

    if best_id:
        if best_score >= NAME_STRONG:
            return best_id, "FuzzyName", best_score
        if best_score >= NAME_REVIEW:
            return best_id, "FuzzyName", best_score

    return None, "None", 0

def upsert_lead(
    sf: Salesforce,
    prop_id: str,
    person: Person,
    matched: Tuple[Optional[str], str, int],
    lead_source: Optional[str] = None
) -> str:
    lead_id, matched_by, score = matched

    base_payload = {
        "Property__c": prop_id,
        "FirstName": person.first_name or None,
        "LastName": person.last_name or "(Unknown)",
        "Role__c": person.role,
        "Matched_By__c": matched_by,
        "Match_Score__c": score,
        "Needs_Review__c": True if (matched_by == "FuzzyName" and score < NAME_STRONG) else False,
        "Created_By_Script__c": True,
    }
    # Age from Data_Management__c to Lead
    if person.age is not None:
        base_payload["Age__c"] = person.age
    # Consent mirrors if present (will map to standards if customs missing)
    if person.dnc_call is not None:  base_payload["Do_Not_Call__c"]  = bool(person.dnc_call)
    if person.dnc_sms  is not None:  base_payload["Do_Not_SMS__c"]   = bool(person.dnc_sms)
    if person.dnc_email is not None: base_payload["Do_Not_Email__c"] = bool(person.dnc_email)

    if lead_source:
        base_payload["LeadSource"] = lead_source

    payload = _map_lead_payload_to_existing_fields(sf, base_payload)

    if lead_id:
        sf.Lead.update(lead_id, payload)
        return lead_id
    else:
        rt_id = get_prospect_record_type_id(sf)
        if rt_id:
            payload["RecordTypeId"] = rt_id
        # Salesforce requires Company on create
        lf = _lead_fields(sf)
        if "Company" in lf and not payload.get("Company"):
            payload["Company"] = "Individual Property Owner"
        resp = sf.Lead.create(payload)
        return resp["id"]



# def upsert_contact_methods(sf: Salesforce, lead_id: str, person: Person) -> int:
#     existing_q = f"""
#         SELECT Phone__c, Phone_Type__c
#         FROM Contact_Method__c
#         WHERE Lead__c = '{soql_escape(lead_id)}'
#     """
#     exist = {(r.get("Phone__c"), (r.get("Phone_Type__c") or "Other")) for r in sf.query_all(existing_q)["records"]}
#     to_create = [(p, t) for (p, t) in person.phones if (p, t) not in exist]

#     created = 0
#     for phone, ptype in to_create:
#         sf.Contact_Method__c.create({
#             "Lead__c": lead_id,
#             "Phone__c": phone,
#             "Phone_Type__c": ptype,
#             "Status__c": "Not Verified"
#         })
#         created += 1
#     return created

# def upsert_contact_methods(sf: Salesforce, lead_id: str, person: Person) -> int:
#     """
#     Buffers Contact_Method__c INSERTS to batch via composite/sobjects.
#     Returns how many rows were staged (not how many were committed).
#     """
#     existing_q = f"""
#         SELECT Phone__c, Phone_Type__c
#         FROM Contact_Method__c
#         WHERE Lead__c = '{soql_escape(lead_id)}'
#     """
#     exist = {(r.get("Phone__c"), (r.get("Phone_Type__c") or "Other"))
#              for r in sf.query_all(existing_q)["records"]}

#     staged = 0
#     for phone, ptype in person.phones:
#         if not phone or (phone, ptype) in exist:
#             continue
#         _METHOD_BUFFER.append({
#             "Lead__c": lead_id,
#             "Phone__c": phone,
#             "Phone_Type__c": ptype,
#             "Status__c": "Not Verified"
#         })
#         staged += 1
#         # Opportunistic flush when buffer hits the limit
#         if len(_METHOD_BUFFER) >= METHOD_BATCH_SIZE:
#             _flush_methods(sf)

#     return staged

def upsert_contact_methods(sf: Salesforce, lead_id: str, person: Person) -> int:
    existing_q = f"""
        SELECT Id, Phone__c, Phone_Type__c, Status__c
        FROM Contact_Method__c
        WHERE Lead__c = '{soql_escape(lead_id)}'
    """
    existing = {r.get("Phone__c"): r for r in sf.query_all(existing_q)["records"]}

    staged = 0
    for phone, ptype in person.phones:
        if not phone:
            continue
        rec = existing.get(phone)
        if rec:
            # update type if different (avoid pointless update)
            if (rec.get("Phone_Type__c") or "Other") != ptype:
                sf.Contact_Method__c.update(rec["Id"], {"Phone_Type__c": ptype})
            continue

        _METHOD_BUFFER.append({
            "Lead__c": lead_id,
            "Phone__c": phone,
            "Phone_Type__c": ptype,
            "Status__c": "Not Verified"
        })
        staged += 1
        if len(_METHOD_BUFFER) >= METHOD_BATCH_SIZE:
            _flush_methods(sf)
    return staged


# def set_primary_from_verified(sf: Salesforce, lead_id: str) -> None:
#     lf = _lead_fields(sf)
#     # read current state first
#     curr = sf.Lead.get(lead_id) if "Phone" in lf else {}
#     curated_locked = bool(curr.get("Freeze_Auto_Updates__c")) if "Freeze_Auto_Updates__c" in lf else False

#     q = f"""
#         SELECT Phone__c, Phone_Type__c, Status__c, LastModifiedDate
#         FROM Contact_Method__c
#         WHERE Lead__c = '{soql_escape(lead_id)}' AND Status__c LIKE 'Verified%'
#         ORDER BY LastModifiedDate DESC
#         LIMIT 25
#     """
#     r = sf.query(q)
#     verified = [row for row in r.get("records", []) if "valid" in (row.get("Status__c") or "").lower()]

#     update = {}
#     if verified and not curated_locked:
#         top = verified[0]
#         if top.get("Phone__c") and ("Phone" in lf):
#             update["Phone"] = top["Phone__c"]
#         if "Primary_Contact_Method__c" in lf and top.get("Phone__c"):
#             update["Primary_Contact_Method__c"] = f"{top.get('Phone_Type__c','')} • •••-•••-{top['Phone__c'][-4:]}"
#     if "Verified_Valid_Methods__c" in lf:
#         update["Verified_Valid_Methods__c"] = len(verified)
#     if update:
#         sf.Lead.update(lead_id, update)



# def mark_contact_success(sf, dm_id, status, change_summary="", fingerprint=None):
#     payload = {
#         "Contact_Last_Run_At__c": datetime.utcnow().isoformat() + "Z",
#         "Contact_Last_Result__c": status,  # use your picklist: ['success','error','skipped:...','created','updated','review-needed']
#         "Contact_Last_Error__c": None,
#         "Attempt_Count__c": 0,
#         "Next_Attempt_At__c": None,
#         # optional: leave Ready_for_Contacts__c true if you want reprocessing on future changes
#         "Contact_Last_Run_Fingerprint__c": fingerprint
#     }
#     # If you want a lightweight summary, reuse Review_Reason__c or add Change_Summary__c
#     if change_summary:
#         payload["Review_Reason__c"] = (change_summary[:255])
#     sf.Data_Management__c.update(dm_id, payload)

def mark_contact_success(sf, dm_id, status, change_summary="", fingerprint=None):
    payload = {
        "Contact_Last_Run_At__c": _utcnow_iso(),
        "Lead_Last_Result__c": status,                 # <-- was Contact_Last_Result__c
        "Contact_Last_Error__c": None,
        "Attempt_Count__c": 0,
        "Next_Attempt_At__c": None,
        "Contact_Last_Run_Fingerprint__c": fingerprint
    }
    if change_summary:
        payload["Change_Summary__c"] = change_summary[:255]
    sf.Data_Management__c.update(dm_id, payload)




# def mark_contact_error(sf, dm_id, msg, attempt=0):
#     next_at = (datetime.utcnow() + timedelta(minutes=BACKOFF_MINUTES * (attempt + 1))).isoformat() + "Z"
#     sf.Data_Management__c.update(dm_id, {
#         "Contact_Last_Run_At__c": datetime.utcnow().isoformat() + "Z",
#         "Contact_Last_Result__c": "error",
#         "Contact_Last_Error__c": str(msg)[:32000],
#         "Attempt_Count__c": (attempt + 1),
#         "Next_Attempt_At__c": next_at,
#     })


def mark_contact_error(sf, dm_id, msg, attempt=0):
    next_at = (datetime.now(timezone.utc) + timedelta(minutes=BACKOFF_MINUTES * (attempt + 1))).isoformat().replace("+00:00","Z")
    sf.Data_Management__c.update(dm_id, {
        "Contact_Last_Run_At__c": _utcnow_iso(),
        "Lead_Last_Result__c": "error",                # <-- was Contact_Last_Result__c
        "Contact_Last_Error__c": str(msg)[:32000],
        "Attempt_Count__c": (attempt + 1),
        "Next_Attempt_At__c": next_at,
    })



# def mark_contact_skipped(sf, dm_id, reason, fingerprint=None):
#     sf.Data_Management__c.update(dm_id, {
#         "Contact_Last_Run_At__c": _utcnow_iso(),
#         "Lead_Last_Result__c": f"skipped:{reason}",    # <-- was Contact_Last_Result__c
#         "Contact_Last_Run_Fingerprint__c": fingerprint
#     })

def mark_contact_skipped(sf, dm_id, reason, fingerprint=None):
    payload = {
        "Contact_Last_Run_At__c": _utcnow_iso(),
        "Lead_Last_Result__c": f"skipped:{reason}",
        "Contact_Last_Run_Fingerprint__c": fingerprint
    }
    # Defer unchanged fingerprint rows for a window (default 30 days)
    if reason == "unchanged-fingerprint":
        payload["Next_Attempt_At__c"] = (
            datetime.now(timezone.utc) + timedelta(days=CONTACT_DEFERRAL_DAYS)
        ).isoformat().replace("+00:00", "Z")
    sf.Data_Management__c.update(dm_id, payload)

# ==================== PER-DM PROCESSOR =================

def process_dm_record(sf: Salesforce, dm: Dict) -> Dict:
    # stats = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}
    # stats = {"leads_created": 0, "leads_updated": 0, "flagged_review": 0, "errors": 0}
    stats = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}

    prop_id = dm.get("Property__c")
    if not prop_id:
        return stats

    people = extract_people(dm)
    lead_source = map_lead_source(dm.get("Lead_Source__c"))

    for p in people:
        try:
            matched = find_lead_match(sf, prop_id, p)
            pre_id = matched[0]
            lead_id = upsert_lead(sf, prop_id, p, matched, lead_source)
            log.info(f"    upserted Lead {lead_id} via {matched[1]} score={matched[2]}")

            # stats["leads_updated" if pre_id else "leads_created"] += 1
            # stats["methods_upserted"] += upsert_contact_methods(sf, lead_id, p)
            # set_primary_from_verified(sf, lead_id)
            stats["leads_updated" if pre_id else "leads_created"] += 1
            stats["methods_upserted"] += upsert_contact_methods(sf, lead_id, p)

            if matched[1] == "FuzzyName" and matched[2] < NAME_STRONG:
                stats["flagged_review"] += 1
        except Exception as e:
            log.exception("Error processing person %s %s: %s", p.first_name, p.last_name, e)
            stats["errors"] += 1

    return stats



# ==================== RUNNER ===========================

# def run(batch_size: int = 100):
#     sf = connect()
#     log.info("Connected to Salesforce (contact-only)")

#     BASE_DM_FIELDS = [
#         "Id", "Property__c",
#         "First_Name__c", "Last_Name__c", "Email_s__c",
#         "Phone_1__c", "Phone_2__c", "Phone_3__c", "Phone_4__c", "Phone_5__c",
#         "Owner_1_Phone_1_Type__c", "Owner_1_Phone_2_Type__c", "Owner_1_Phone_3_Type__c",
#         "Owner_1_Phone_4_Type__c", "Owner_1_Phone_5_Type__c",
#         "Owner_2_First_Name__c", "Owner_2_Last_Name__c", "Owner_2_Email__c",
#         "Owner_2_Phone_1__c", "Owner_2_Phone_2__c", "Owner_2_Phone_3__c", "Owner_2_Phone_4__c", "Owner_2_Phone_5__c",
#         "Owner_2_Phone_1_Type__c", "Owner_2_Phone_2_Type__c", "Owner_2_Phone_3_Type__c",
#         "Owner_2_Phone_4_Type__c", "Owner_2_Phone_5_Type__c",
#         "Lead_Source__c",
#     ]

#     RELATIVE_FIELDS: List[str] = []
#     for i in range(1, 11):
#         idx = f"{i:02d}"
#         RELATIVE_FIELDS += [
#             f"Relative_{idx}_First_Name__c",
#             f"Relative_{idx}_Last_Name__c",
#             f"Relative_{idx}_Relationship__c",
#             f"Relative_{idx}_Priority__c",
#             f"Relative_{idx}_Email__c",
#             f"Relative_{idx}_Phone_1__c", f"Relative_{idx}_Phone_1_Type__c",
#             f"Relative_{idx}_Phone_2__c", f"Relative_{idx}_Phone_2_Type__c",
#             f"Relative_{idx}_Phone_3__c", f"Relative_{idx}_Phone_3_Type__c",
#             f"Relative_{idx}_Phone_4__c", f"Relative_{idx}_Phone_4_Type__c",
#             f"Relative_{idx}_Phone_5__c", f"Relative_{idx}_Phone_5_Type__c",
#             f"Relative_{idx}_Do_Not_Call__c",
#             f"Relative_{idx}_Do_Not_SMS__c",
#             f"Relative_{idx}_Do_Not_Email__c",
#             f"Relative_{idx}_Notes__c",
#         ]

#     FIELDS = BASE_DM_FIELDS + RELATIVE_FIELDS + [
#         "SystemModstamp",
#         "Ready_for_Contacts__c",
#         "Contact_Last_Run_At__c",
#         "Contact_Last_Run_Fingerprint__c",
#         "Attempt_Count__c",
#         "Next_Attempt_At__c",
#     ]

#     soql = f"""
#         SELECT {', '.join(FIELDS)}
#         FROM Data_Management__c
#         WHERE Property__c != null
#           AND Ready_for_Contacts__c = true
#           AND (Contact_Last_Run_At__c = NULL OR SystemModstamp >= LAST_N_DAYS:{CONTACT_LOOKBACK_DAYS})
#           AND (Next_Attempt_At__c = NULL OR Next_Attempt_At__c <= LAST_N_DAYS:0)
#         ORDER BY SystemModstamp ASC
#         LIMIT {batch_size}
#     """
#     dms = sf.query_all(soql)["records"]
#     log.info(f"Fetched {len(dms)} Data_Management__c rows (ready for contacts)")

#     totals = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}

#     try:
#         for idx, dm in enumerate(dms, start=1):
#             dm_id = dm["Id"]
#             log.info(f"[{idx}/{len(dms)}] DM {dm_id} | Property {dm.get('Property__c')}")

#             # Fingerprint skip
#             fp = contact_fingerprint(dm)
#             if dm.get("Contact_Last_Run_Fingerprint__c") == fp:
#                 mark_contact_skipped(sf, dm_id, "unchanged-fingerprint", fp)
#                 continue

#             try:
#                 s = process_dm_record(sf, dm)
#                 for k in totals:
#                     totals[k] += s.get(k, 0)
#                 summary = f"leads +{s['leads_created']}/{s['leads_updated']}, methods +{s['methods_upserted']}"
#                 mark_contact_success(sf, dm_id, "success", summary, fp)
#             except Exception as e:
#                 log.exception("DM %s failed", dm_id)
#                 mark_contact_error(sf, dm_id, e, dm.get("Attempt_Count__c") or 0)
#                 totals["errors"] += 1

#     finally:
#         flushed = _flush_all_methods(sf)
#         log.info("Contact_Method__c batched create complete (created via batch=%s)", flushed)



BASE_DM_FIELDS = [
    "Id", "Property__c","Age__c",
    "First_Name__c", "Last_Name__c", "Email_s__c",
    "Phone_1__c", "Phone_2__c", "Phone_3__c", "Phone_4__c", "Phone_5__c",
    "Owner_1_Phone_1_Type__c", "Owner_1_Phone_2_Type__c", "Owner_1_Phone_3_Type__c",
    "Owner_1_Phone_4_Type__c", "Owner_1_Phone_5_Type__c",
    "Owner_2_First_Name__c", "Owner_2_Last_Name__c", "Owner_2_Email__c",
    "Owner_2_Phone_1__c", "Owner_2_Phone_2__c", "Owner_2_Phone_3__c", "Owner_2_Phone_4__c", "Owner_2_Phone_5__c",
    "Owner_2_Phone_1_Type__c", "Owner_2_Phone_2_Type__c", "Owner_2_Phone_3_Type__c",
    "Owner_2_Phone_4_Type__c", "Owner_2_Phone_5_Type__c",
    "Lead_Source__c",
]

RELATIVE_FIELDS: List[str] = []
for i in range(1, 11):
    idx = f"{i:02d}"
    RELATIVE_FIELDS += [
        f"Relative_{idx}_First_Name__c",
        f"Relative_{idx}_Last_Name__c",
        f"Relative_{idx}_Relationship__c",
        f"Relative_{idx}_Priority__c",
        f"Relative_{idx}_Email__c",
        f"Relative_{idx}_Phone_1__c", f"Relative_{idx}_Phone_1_Type__c",
        f"Relative_{idx}_Phone_2__c", f"Relative_{idx}_Phone_2_Type__c",
        f"Relative_{idx}_Phone_3__c", f"Relative_{idx}_Phone_3_Type__c",
        f"Relative_{idx}_Phone_4__c", f"Relative_{idx}_Phone_4_Type__c",
        f"Relative_{idx}_Phone_5__c", f"Relative_{idx}_Phone_5_Type__c",
        f"Relative_{idx}_Do_Not_Call__c",
        f"Relative_{idx}_Do_Not_SMS__c",
        f"Relative_{idx}_Do_Not_Email__c",
        f"Relative_{idx}_Notes__c",
    ]

FIELDS = BASE_DM_FIELDS + RELATIVE_FIELDS + [
    "SystemModstamp",
    "Ready_for_Contacts__c",
    "Contact_Last_Run_At__c",
    "Contact_Last_Run_Fingerprint__c",
    "Attempt_Count__c",
    "Next_Attempt_At__c",
]




def run(batch_size: int = 100):
    sf = connect()
    log.info("Connected to Salesforce (contact-only)")

    now_iso = _utcnow_iso()
    fields = BASE_DM_FIELDS + RELATIVE_FIELDS + [
        "Attempt_Count__c","Next_Attempt_At__c",
        "SystemModstamp","Contact_Last_Run_At__c",
        "Contact_Last_Run_Fingerprint__c","Ready_for_Contacts__c"
    ]

    soql = f"""
        SELECT {', '.join(fields)}
        FROM Data_Management__c
        WHERE Property__c != null
          AND Ready_for_Contacts__c = true
          AND (Contact_Last_Run_At__c = NULL OR SystemModstamp >= LAST_N_DAYS:{CONTACT_LOOKBACK_DAYS})
          AND (Next_Attempt_At__c = NULL OR Next_Attempt_At__c <= {now_iso})
        ORDER BY SystemModstamp ASC
        LIMIT {batch_size}
    """.strip()
    from datetime import datetime

    def _parse_iso(dt: str | None):
        if not dt:
            return None
        # Salesforce returns Z; normalize for fromisoformat
        return datetime.fromisoformat(dt.replace("Z", "+00:00"))

    _raw = sf.query_all(soql)["records"]

    # def _needs_run(r):
    #     last = _parse_iso(r.get("Contact_Last_Run_At__c"))
    #     mod  = _parse_iso(r.get("SystemModstamp"))
    #     return (last is None) or (mod and mod > last)

    def _needs_run(r):
        last = _parse_iso(r.get("Contact_Last_Run_At__c"))
        mod  = _parse_iso(r.get("SystemModstamp"))
        return (last is None) or (mod and mod > last)

    dms = [r for r in _raw if _needs_run(r)]

    log.info("Fetched %d Data_Management__c rows (ready for contacts)", len(dms))

    # totals = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}
    # totals = {"leads_created": 0, "leads_updated": 0, "flagged_review": 0, "errors": 0}
    totals = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}


    try:
        for idx, dm in enumerate(dms, start=1):
            dm_id = dm["Id"]
            log.info("[{}/{}] DM {} | Property {}".format(idx, len(dms), dm_id, dm.get("Property__c")))
            fp = contact_fingerprint(dm)
            if dm.get("Contact_Last_Run_Fingerprint__c") == fp:
                mark_contact_skipped(sf, dm_id, "unchanged-fingerprint", fp)
                continue

            s = process_dm_record(sf, dm)
            for k in totals: totals[k] += s.get(k, 0)

            # summary = f"leads +{s['leads_created']}/{s['leads_updated']}, methods +{s['methods_upserted']}"
            # summary = f"leads +{s['leads_created']}/{s['leads_updated']}"
            summary = f"leads +{s['leads_created']}/{s['leads_updated']}, methods +{s['methods_upserted']}"


            mark_contact_success(sf, dm_id, "success", summary, fp)
    except Exception as e:
        # ensure partial flush still happens below
        log.exception("run() loop failed: %s", e)
        # (optional) you could mark the specific DM error where it happened
    finally:
        flushed = _flush_all_methods(sf)
        log.info("Contact_Method__c batched create complete (created via batch=%s)", flushed)

    # log.info(
    #     # "Run complete | Created: %s | Updated: %s | Methods: %s | Flagged: %s | Errors: %s",
    #     # totals["leads_created"], totals["leads_updated"], totals["methods_upserted"],
    #     # totals["flagged_review"], totals["errors"]
    #     "Run complete | Created: %s | Updated: %s | Flagged: %s | Errors: %s",
    #     totals["leads_created"], totals["leads_updated"], totals["flagged_review"], totals["errors"]

    # )
    log.info(
        "Run complete | Created: %s | Updated: %s | Methods: %s | Flagged: %s | Errors: %s",
        totals["leads_created"], totals["leads_updated"], totals["methods_upserted"],
        totals["flagged_review"], totals["errors"]
    )


# def run(batch_size: int = 100):
#     sf = connect()
#     log.info("Connected to Salesforce (contact-only)")

#     # --- optional: see API limits & available RTs once per run
#     # log.info(sf.limits())
#     # infos = (sf.Lead.describe() or {}).get("recordTypeInfos", [])
#     # log.info("Lead RTs visible: " + ", ".join(f"{i.get('developerName')}:{i.get('recordTypeId')} (avail={i.get('available')})" for i in infos))

#     BASE_DM_FIELDS = [
#         "Id", "Property__c",
#         "First_Name__c", "Last_Name__c", "Email_s__c",
#         "Phone_1__c", "Phone_2__c", "Phone_3__c", "Phone_4__c", "Phone_5__c",
#         "Owner_1_Phone_1_Type__c", "Owner_1_Phone_2_Type__c", "Owner_1_Phone_3_Type__c",
#         "Owner_1_Phone_4_Type__c", "Owner_1_Phone_5_Type__c",
#         "Owner_2_First_Name__c", "Owner_2_Last_Name__c", "Owner_2_Email__c",
#         "Owner_2_Phone_1__c", "Owner_2_Phone_2__c", "Owner_2_Phone_3__c", "Owner_2_Phone_4__c", "Owner_2_Phone_5__c",
#         "Owner_2_Phone_1_Type__c", "Owner_2_Phone_2_Type__c", "Owner_2_Phone_3_Type__c",
#         "Owner_2_Phone_4_Type__c", "Owner_2_Phone_5_Type__c",
#         "Lead_Source__c",
#     ]

#     RELATIVE_FIELDS: List[str] = []
#     for i in range(1, 11):  # 01..10
#         idx = f"{i:02d}"
#         RELATIVE_FIELDS += [
#             f"Relative_{idx}_First_Name__c",
#             f"Relative_{idx}_Last_Name__c",
#             f"Relative_{idx}_Relationship__c",
#             f"Relative_{idx}_Priority__c",
#             f"Relative_{idx}_Email__c",
#             f"Relative_{idx}_Phone_1__c", f"Relative_{idx}_Phone_1_Type__c",
#             f"Relative_{idx}_Phone_2__c", f"Relative_{idx}_Phone_2_Type__c",
#             f"Relative_{idx}_Phone_3__c", f"Relative_{idx}_Phone_3_Type__c",
#             f"Relative_{idx}_Phone_4__c", f"Relative_{idx}_Phone_4_Type__c",
#             f"Relative_{idx}_Phone_5__c", f"Relative_{idx}_Phone_5_Type__c",
#             f"Relative_{idx}_Do_Not_Call__c",
#             f"Relative_{idx}_Do_Not_SMS__c",
#             f"Relative_{idx}_Do_Not_Email__c",
#             f"Relative_{idx}_Notes__c",
#         ]

#     FIELDS = BASE_DM_FIELDS + RELATIVE_FIELDS
#     soql = f"""
#         SELECT {', '.join(FIELDS)}
#         FROM Data_Management__c
#         WHERE Property__c != null
#         ORDER BY LastModifiedDate ASC
#         LIMIT {batch_size}
#     """
#     dms = sf.query_all(soql)["records"]
#     log.info(f"Fetched {len(dms)} Data_Management__c rows with Property__c")

#     totals = {"leads_created": 0, "leads_updated": 0, "methods_upserted": 0, "flagged_review": 0, "errors": 0}
#     for idx, dm in enumerate(dms, start=1):
#         log.info(f"[{idx}/{len(dms)}] DM {dm['Id']} | Property {dm.get('Property__c')}")
#         people = extract_people(dm)
#         log.info(f"  people={len(people)} (owners+relatives)")
#         s = process_dm_record(sf, dm)
#         for k in totals: totals[k] += s.get(k, 0)

#     log.info(
#         "Run complete | Created: %s | Updated: %s | Methods: %s | Flagged: %s | Errors: %s",
#         totals["leads_created"], totals["leads_updated"], totals["methods_upserted"],
#         totals["flagged_review"], totals["errors"]
#     )


# if __name__ == "__main__":
#     run(batch_size=150)

if __name__ == "__main__":
    loop = True
    if "--once" in sys.argv:
        loop = False

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)

    if loop:
        while not _SHUTDOWN:
            start = time.time()
            lock = _try_lock(LOCK_PATH)
            if lock:
                try:
                    run(batch_size=500)  # new SF session each tick
                except Exception as e:
                    log.exception("run() raised: %s", e)
                finally:
                    try: fcntl.flock(lock, fcntl.LOCK_UN)
                    except Exception: pass
                    try: lock.close()
                    except Exception: pass
            else:
                log.info("Previous cycle still running; skipping this tick")

            elapsed = time.time() - start
            time.sleep(max(0.0, SLEEP_SEC - elapsed))
    else:
        run(batch_size=500)
