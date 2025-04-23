from .fetch import (
    handle_hubspot,
    handle_hubspot_analytics,
    handle_forms,
    handle_form_submissions,
)
from .utility import format_date


SYNC_FUNCTIONS = {
    "deals": handle_hubspot("deals", url="objects/deals"),
    "contacts": handle_hubspot("contacts", url="objects/contacts"),
    "companies": handle_hubspot("companies", url="objects/companies"),
    "goals": handle_hubspot("goals", url="objects/goal_targets"),
    "owners": handle_hubspot("owners", url="owners"),
    "teams": handle_hubspot("teams", url="teams"),
    "engagements": handle_hubspot("engagements", url="objects/engagements"),
    "calls": handle_hubspot("calls", url="objects/calls"),
    "marketings": handle_hubspot_analytics("marketings", url=""),
    "forms": handle_forms("forms", url=""),
    "form_submissions": handle_form_submissions("form_submissions"),
}

SUB_STREAMS = {
    "deals": ["deals_companies", "deals_contacts"],
    "contacts": ["contacts_companies"],
    "engagements": ["engagements_companies", "engagements_contacts"],
}

# note there will typically either be one row per schema, or remove this and hard-code the function response as e.g. "ID"
ID_COLUMNS = {
    "deals": "id",
    "contacts": "id",
    "deals_companies": "id",
    "deals_contacts": "id",
    "contacts_companies": "id",
    "companies": "id",
    "owners": "id",
    "goals": "id",
    "teams": "id",
    "engagements": "id",
    "engagements_companies": "id",
    "engagements_contacts": "id",
    "calls": "id",
    "marketings": "id",
    "forms": "id",
    "form_submissions": "id",
}


def get_id_column(schema_name):
    return ID_COLUMNS.get(schema_name, "ID")


# # # override this with more custom logic if bookmarks for a stream aren't dates (very rare)
def format_bookmark(stream, val):
    if val is None:
        return None  # Prevents calling format_date() with None
    return format_date(val)
