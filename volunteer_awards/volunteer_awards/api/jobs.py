import frappe
from volunteer_awards.volunteer_awards.api import kobo_pull_awards

@frappe.whitelist()
def enqueue_batch(page_size: int = 1, start_page: int = 1, with_attachments: int = 1, log_missing: int = 1):
    frappe.enqueue(
        "volunteer_awards.volunteer_awards.api.kobo_pull_awards.pull_asset_batch",
        queue="long",
        page_size=int(page_size),
        start_page=int(start_page),
        with_attachments=int(with_attachments),
        log_missing=int(log_missing),
        job_name=f"VA2025 KoBo batch p{start_page}"
    )
    return {"enqueued": True}
