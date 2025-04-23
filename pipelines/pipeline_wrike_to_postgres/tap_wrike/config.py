from .fetch import handle_wrike
from .utility import format_date


SYNC_FUNCTIONS = {
    "tasks": handle_wrike("tasks", url="tasks"),
    "contacts": handle_wrike("contacts", url="contacts"),
    "auditLogs": handle_wrike("auditLogs", url="audit_log"),
    "timeLogs": handle_wrike("timeLogs", url="timelogs"),
}

SUB_STREAMS = {"contacts": ["contacts_profiles"]}


# note there will typically either be one row per schema, or remove this and hard-code the function response as e.g. "ID"
ID_COLUMNS = {
    "tasks": "id",
    "contacts": "id",
    "auditLogs": "id",
    "contacts_profiles": "id",
    "timeLogs": "id",
}


def get_id_column(schema_name):
    return ID_COLUMNS.get(schema_name, "ID")


# # # override this with more custom logic if bookmarks for a stream aren't dates (very rare)
def format_bookmark(stream, val):
    if val is None:
        return None  # Prevents calling format_date() with None
    return format_date(val)
