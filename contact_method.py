# import time
# from simple_salesforce import Salesforce
# from dotenv import load_dotenv
# import os, re, logging
# from typing import List, Tuple
# from fuzzywuzzy import fuzz

# # === ENV SETUP ===
# load_dotenv()
# SF_USERNAME = os.getenv("SF_USERNAME")
# SF_PASSWORD = os.getenv("SF_PASSWORD")
# SF_TOKEN = os.getenv("SF_TOKEN")
# SF_DOMAIN = os.getenv("SF_DOMAIN", "test")

# # === LOGGING ===
# logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
# log = logging.getLogger("contact_scrubber")

# # === SALESFORCE CONNECTION ===
# def connect() -> Salesforce:
#     return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN, domain=SF_DOMAIN)

# # === UTILITIES ===
# def soql_escape(s: str) -> str:
#     return (s or "").replace("\\", "\\\\").replace("'", "\\'")

# def norm_phone(us_phone: str) -> str:
#     return re.sub(r"\D", "", us_phone or "")[-10:]

# # === CONTACT METHOD SYNC ===
# # def scrub_contact_methods(sf: Salesforce, limit: int = 100):
# #     q = f"""
# #         SELECT Id, FirstName, LastName, Phone, Freeze_Auto_Updates__c
# #         FROM Lead
# #         WHERE Phone != NULL
# #         ORDER BY LastModifiedDate DESC
# #         LIMIT {limit}
# #     """
# #     leads = sf.query_all(q)["records"]
# #     log.info("Fetched %s Leads for scrubbing", len(leads))

# #     updated = 0
# #     for lead in leads:
# #         lead_id = lead["Id"]
# #         phone = norm_phone(lead.get("Phone"))
# #         if not phone:
# #             continue

# #         q_cm = f"""
# #             SELECT Id, Phone__c, Phone_Type__c, Status__c
# #             FROM Contact_Method__c
# #             WHERE Lead__c = '{soql_escape(lead_id)}'
# #         """
# #         methods = sf.query_all(q_cm)["records"]
# #         match = next((m for m in methods if norm_phone(m.get("Phone__c")) == phone and "Verified" in (m.get("Status__c") or "")), None)

# #         update = {}
# #         if match:
# #             update["Verified_Valid_Methods__c"] = sum(1 for m in methods if "valid" in (m.get("Status__c") or "").lower())
# #             update["Primary_Contact_Method__c"] = f"{match.get('Phone_Type__c','')} • •••-•••-{phone[-4:]}"
# #         else:
# #             update["Verified_Valid_Methods__c"] = 0
# #             update["Primary_Contact_Method__c"] = None

# #         if not lead.get("Freeze_Auto_Updates__c"):
# #             update["Phone"] = match["Phone__c"] if match else None

# #         sf.Lead.update(lead_id, update)
# #         updated += 1
# #         log.info("Updated Lead %s: %s", lead_id, update)

# #     log.info("Scrubbing complete. Leads updated: %s", updated)

# def scrub_contact_methods(sf: Salesforce, limit: int = 100):
#     # Find Leads that have Contact_Method__c records
#     q = f"""
#         SELECT Id, FirstName, LastName, Phone, Freeze_Auto_Updates__c
#         FROM Lead
#         WHERE Id IN (
#             SELECT Lead__c 
#             FROM Contact_Method__c 
#             WHERE Lead__c != NULL
#         )
#         ORDER BY LastModifiedDate DESC
#         LIMIT {limit}
#     """
#     leads = sf.query_all(q)["records"]
#     log.info("Fetched %s Leads for scrubbing", len(leads))

#     updated = 0
#     for lead in leads:
#         lead_id = lead["Id"]
#         current_phone = norm_phone(lead.get("Phone") or "")

#         q_cm = f"""
#             SELECT Id, Phone__c, Phone_Type__c, Status__c
#             FROM Contact_Method__c
#             WHERE Lead__c = '{soql_escape(lead_id)}'
#         """
#         methods = sf.query_all(q_cm)["records"]
        
#         # Find verified valid method
#         match = next((m for m in methods 
#                      if "Verified" in (m.get("Status__c") or "") 
#                      and "Valid" in (m.get("Status__c") or "")), None)

#         update = {}
        
#         # Count all verified valid methods
#         verified_count = sum(1 for m in methods 
#                            if "Verified" in (m.get("Status__c") or "") 
#                            and "Valid" in (m.get("Status__c") or "").lower())
        
#         update["Verified_Valid_Methods__c"] = verified_count

#         if match:
#             phone = norm_phone(match.get("Phone__c"))
#             update["Primary_Contact_Method__c"] = f"{match.get('Phone_Type__c','')} • •••-•••-{phone[-4:]}"
            
#             # Only update Phone if not frozen
#             if not lead.get("Freeze_Auto_Updates__c"):
#                 update["Phone"] = match["Phone__c"]
#         else:
#             update["Primary_Contact_Method__c"] = None
#             if not lead.get("Freeze_Auto_Updates__c"):
#                 update["Phone"] = None

#         if update:
#             sf.Lead.update(lead_id, update)
#             updated += 1
#             log.info("Updated Lead %s: %s", lead_id, update)

#     log.info("Scrubbing complete. Leads updated: %s", updated)

# if __name__ == "__main__":
#     sf = connect()
#     while True:
#         scrub_contact_methods(sf, limit=200)
#         time.sleep(20)  # wait 20 seconds before running again

import time
from simple_salesforce import Salesforce
from dotenv import load_dotenv
import os, re, logging
from typing import List, Tuple

# === ENV SETUP ===
load_dotenv()
SF_USERNAME = os.getenv("SF_USERNAME")
SF_PASSWORD = os.getenv("SF_PASSWORD")
SF_TOKEN = os.getenv("SF_TOKEN")
SF_DOMAIN = os.getenv("SF_DOMAIN", "test")

# === LOGGING ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("contact_scrubber")

# === SALESFORCE CONNECTION ===
def connect() -> Salesforce:
    return Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN, domain=SF_DOMAIN)

# === UTILITIES ===
def soql_escape(s: str) -> str:
    return (s or "").replace("\\", "\\\\").replace("'", "\\'")

def norm_phone(us_phone: str) -> str:
    """Normalize phone to last 10 digits"""
    if not us_phone:
        return ""
    return re.sub(r"\D", "", us_phone)[-10:]

# === CONTACT METHOD SYNC ===
def scrub_contact_methods(sf: Salesforce, limit: int = 100):
    # Find Leads linked to Properties
    q = f"""
        SELECT Id, FirstName, LastName, Phone, Freeze_Auto_Updates__c, 
               Property__c, Verified_Valid_Methods__c, Primary_Contact_Method__c
        FROM Lead
        WHERE Property__c != NULL
        ORDER BY LastModifiedDate DESC
        LIMIT {limit}
    """
    leads = sf.query_all(q)["records"]
    log.info("Fetched %s Leads for scrubbing", len(leads))

    updated = 0
    for lead in leads:
        lead_id = lead["Id"]
        current_phone = norm_phone(lead.get("Phone") or "")
        current_verified_count = lead.get("Verified_Valid_Methods__c") or 0
        current_primary = lead.get("Primary_Contact_Method__c")

        # Get all Contact Methods for this Lead
        q_cm = f"""
            SELECT Id, Phone__c, Phone_Type__c, Status__c
            FROM Contact_Method__c
            WHERE Lead__c = '{soql_escape(lead_id)}'
            ORDER BY LastModifiedDate DESC
        """
        methods = sf.query_all(q_cm)["records"]
        
        if not methods:
            continue
        
        # Find ONLY methods with EXACT status "Verified – Valid"
        verified_methods = [
            m for m in methods 
            if m.get("Status__c") == "Verified – Valid"  # Exact match only
        ]
        
        # Pick the first verified valid method (most recently modified)
        match = verified_methods[0] if verified_methods else None
        
        # Count verified valid methods
        verified_count = len(verified_methods)
        
        # Build update payload
        update = {}
        
        # Always update count if changed
        if verified_count != current_verified_count:
            update["Verified_Valid_Methods__c"] = verified_count
        
        if match:
            phone = norm_phone(match.get("Phone__c"))
            new_primary = f"{match.get('Phone_Type__c','Unknown')} • •••-•••-{phone[-4:]}"
            
            # Update primary display if changed
            if new_primary != current_primary:
                update["Primary_Contact_Method__c"] = new_primary
            
            # Update Phone field if not frozen AND changed
            if not lead.get("Freeze_Auto_Updates__c"):
                if phone != current_phone:
                    update["Phone"] = match["Phone__c"]
        else:
            # No verified methods found
            if current_primary is not None:
                update["Primary_Contact_Method__c"] = None
            
            # Clear phone if not frozen AND currently has a value
            if not lead.get("Freeze_Auto_Updates__c") and current_phone:
                update["Phone"] = None

        # Only update if something actually changed
        if update:
            sf.Lead.update(lead_id, update)
            updated += 1
            log.info("Updated Lead %s: %s", lead_id, update)
        else:
            log.debug("Lead %s: No changes needed", lead_id)

    log.info("Scrubbing complete. Leads updated: %s / %s", updated, len(leads))

if __name__ == "__main__":
    sf = connect()
    while True:
        scrub_contact_methods(sf, limit=200)
        time.sleep(20)