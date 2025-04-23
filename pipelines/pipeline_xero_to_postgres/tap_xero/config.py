from .fetch import handle_xero, handle_xero_budget
from .utility import format_date

SYNC_FUNCTIONS = {
    "invoices": handle_xero("invoices", url="Invoices"),
    "budgets": handle_xero_budget("budgets", url="Budgets"),
}

SUB_STREAMS = {
    "invoices": ["invoices_lines"],
    "budgets": ["budgets_lines"],
}


# Define primary keys
ID_COLUMNS = {
    "invoices": "InvoiceID",
    "invoices_lines": "LineItemID",
    "budgets": "BudgetID",
    "budgets_lines": "ID",
}


def get_id_column(schema_name):
    return ID_COLUMNS.get(schema_name, "ID")  # Default to "ID" if missing


# Ensure bookmarks are formatted correctly
def format_bookmark(stream, val):
    if val is None:
        return None  # Prevents calling format_date() with None
    return format_date(val)
