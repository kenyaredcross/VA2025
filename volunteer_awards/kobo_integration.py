import requests
import frappe

def get_kobo_data():
    """Fetch submissions from Kobo API"""
    settings = frappe.get_single("Kobo VA Settings")

    url = f"{settings.base_url}/api/v2/assets/{settings.asset_uid}/data/"

    headers = {
        "Authorization": f"Token {settings.api_token}"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json().get("results", []), settings.asset_uid
    else:
        frappe.throw(f"Kobo API Error: {response.text}")


@frappe.whitelist()
def sync_kobo_data():
    """Sync Kobo submissions into Volunteer Awards Doctype"""
    submissions, asset_uid = get_kobo_data()

    count = 0
    for sub in submissions:
        if not frappe.db.exists("Volunteer Awards", {"kobo_uid": sub["_uuid"]}):
            doc = frappe.new_doc("Volunteer Awards")
            doc.kobo_uid = sub["_uuid"]
            doc.form_id = asset_uid
            doc.submission_data = frappe.as_json(sub)  # raw JSON dump

            # 👇 Extract the Kobo field "category" → Frappe field "award_category"
            doc.award_category = sub.get("category")

            doc.insert(ignore_permissions=True)
            count += 1

    frappe.db.commit()
    return f"Synced {count} new submissions."
