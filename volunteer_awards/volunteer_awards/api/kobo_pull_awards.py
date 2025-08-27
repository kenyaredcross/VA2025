# volunteer_awards/volunteer_awards/api/kobo_pull_awards.py
# Frappe 15 — KoBo pull for "Volunteer Awards" (paged or full).
# - Exact KoBo group/field keys
# - Name docs by KoBo _uuid
# - Guard against assigning scalars into Table fields
# - Validate Select values strictly against DocField options (no remapping)
# - If KoBo omits a Select field, clear it so DocType defaults don’t “stick”

import json
import base64
import frappe
import requests

# ----- CONFIG -----
KOBO_BASE = "https://kobo.ifrc.org"
ASSET_UID = "ajZ5x7BwK7ouPLrJJRh7Ar"
DOCTYPE   = "Volunteer Awards"

# KoBo JSON key -> Frappe fieldname (EXACT keys)
FIELD_MAP = {
    # nomination_category
    "nomination_category/category":                         "award_category",
    "nomination_category/category_of_youth":                "category_of_youth",
    "nomination_category/youth_in_school":                  "youth_in_school",

    # group_nominee
    "group_nominee/nominee_full_name":                      "full_name",
    "group_nominee/location_region":                        "region",
    "group_nominee/location_county":                        "location_county",
    "group_nominee/nominee_phone_number":                   "phone",
    "group_nominee/nominee_email_address":                  "email",
    "group_nominee/duration":                               "volunteering_period",
    "group_nominee/dob":                                    "date_of_birth",

    # description
    "description/_1_In_not_more_than_his_her_work_involve": "volunteering_experience",
    "description/_2_In_not_more_than_y_or_KRCS_as_a_whole": "volunteer_achivements",  # ensure DocField spelling
    "description/_3_In_not_more_than_omination_should_win": "reason_to_win",

    # declaration_acknowledgement (optional)
    "declaration_acknowledgement/acknowledgement_agree":    "acknowledgement_agree",
    "declaration_acknowledgement/nominee_acknowledge_name": "nominee_acknowledge_name",
    "declaration_acknowledgement/nominee_acknowledge_sign": "nominee_acknowledge_sign",
    "declaration_acknowledgement/nominee_acknowledge_date": "nominee_acknowledge_date",
}

# Attachment question -> Frappe Attach field (suffix of question_xpath)
ATTACH_MAP = {
    "Attach_Cover_letter_ecommendation_letter": "cover_letter_recommendation_letter",
    "Attach_Videos_Maximum_of_1_minute":        "videos",
    "Attach_Press_cuttings":                    "press_cuttings",
    "Attach_Testimonial":                       "testimonial",
    "Attach_any_other_supporting_document":     "supporting_documents",
    "nominee_acknowledge_sign": "nominee_acknowledge_sign",  # image/signature
    "Attache_Application_form": "application_form",          # note KoBo typo: Attache
}

# ----- HTTP helpers -----
def _auth_headers():
    token = frappe.conf.get("kobo_kpi_token")
    if not token:
        frappe.throw("Add kobo_kpi_token to site_config.json")
    return {"Authorization": f"Token {token}"}

def _kobo_get(path_or_url: str, timeout=120):
    url = path_or_url if path_or_url.startswith("http") else f"{KOBO_BASE}{path_or_url}"
    return requests.get(url, headers=_auth_headers(), timeout=timeout)

# ----- file helpers -----
def _download_attachment(att: dict):
    meta_url = att.get("download_url")
    if not meta_url:
        return None
    meta_res = _kobo_get(meta_url, timeout=120)
    if meta_res.status_code == 404:
        return None
    meta_res.raise_for_status()

    if not meta_res.headers.get("Content-Type", "").startswith("application/json"):
        return (att.get("filename") or "file.bin", meta_res.content)

    meta = meta_res.json()
    file_url = meta.get("download_url") or meta.get("download_large_url") or meta.get("download_small_url")
    if not file_url:
        return None

    file_res = requests.get(file_url, timeout=180)
    file_res.raise_for_status()
    filename = att.get("filename") or meta.get("filename") or "file.bin"
    return (filename, file_res.content)

def _attach_file(doctype, name, filename, content_bytes, target_field=None):
    filedoc = frappe.get_doc({
        "doctype": "File",
        "file_name": filename,
        "attached_to_doctype": doctype,
        "attached_to_name": name,
        "content": base64.b64encode(content_bytes).decode(),
        "decode": 1,
        "is_private": 1,
    }).insert(ignore_permissions=True)

    if target_field and frappe.get_meta(doctype).has_field(target_field):
        frappe.db.set_value(doctype, name, target_field, filedoc.file_url, update_modified=False)

# ----- helpers -----
def _select_options(df):
    opts = (df.options or "").splitlines()
    return [o.strip() for o in opts if o.strip()]

def _safe_set_scalar(doc, meta, kobo_key, fieldname, value, present: bool):
    """
    Set scalar fields safely.
    - Never write into Table fields.
    - For Selects:
        * If KoBo omitted the field (present=False) or sent empty, CLEAR the field to avoid DocType defaults.
        * If KoBo sent a value, enforce allow-list (DocField.options).
    - For non-Selects: set only if KoBo provided a non-empty value.
    """
    df = meta.get_field(fieldname)
    if not df:
        return
    # don’t assign scalars to Table fields
    if df.fieldtype in ("Table", "Table MultiSelect"):
        return

    if df.fieldtype == "Select":
        if not present or value in (None, ""):
            # Explicitly clear so DocType default doesn't stick
            setattr(doc, fieldname, None)
            return

        options = _select_options(df)
        if options and value not in options:
            frappe.logger("kobo_pull_awards").warning(
                f"Skipping invalid Select: {fieldname}='{value}' not in {options}"
            )
            return

        setattr(doc, fieldname, value)
        return

    # Non-Select scalars: only set if meaningful
    if value not in (None, ""):
        setattr(doc, fieldname, value)

# ----- upsert (scalars only) -----
def _upsert_row_scalars(row: dict):
    kobo_uid = row.get("_uuid")
    if not kobo_uid:
        return None

    existing = frappe.get_all(DOCTYPE, filters={"kobo_uid": kobo_uid}, pluck="name")
    if existing:
        doc = frappe.get_doc(DOCTYPE, existing[0])
    else:
        doc = frappe.new_doc(DOCTYPE)
        doc.kobo_uid = kobo_uid
        doc.name = kobo_uid
        doc.flags.name_set = True

    # meta fields
    doc.date_submitted  = row.get("_submission_time")
    doc.form_id         = row.get("_xform_id_string")
    doc.submission_data = json.dumps(row, ensure_ascii=False)

    # mapped scalars (with fieldtype + Select checks)
    meta = frappe.get_meta(DOCTYPE)
    for kobo_key, frappe_field in FIELD_MAP.items():
        _safe_set_scalar(doc, meta, kobo_key, frappe_field, row.get(kobo_key), present=(kobo_key in row))

    # votes default only if not a table
    df_votes = meta.get_field("votes")
    if df_votes and df_votes.fieldtype not in ("Table", "Table MultiSelect"):
        if getattr(doc, "votes", None) in (None, ""):
            try:
                doc.votes = 0
            except Exception:
                pass

    doc.save(ignore_permissions=True)
    return doc.name

# ----- PUBLIC: one page -----
@frappe.whitelist()
def pull_asset_batch(page_size: int = 10, start_page: int = 1, with_attachments: int = 0, log_missing: int = 0):
    """
    Import one KoBo page.
      page_size        default 10
      start_page       1-based page index
      with_attachments 0 = skip files, 1 = download files
      log_missing      1 = warn if a mapped KoBo key is missing in a row
    """
    base = f"/api/v2/assets/{ASSET_UID}/data/?format=json&page_size={page_size}"
    path = base + (f"&page={start_page}" if start_page and start_page > 1 else "")

    res = _kobo_get(path, timeout=120)
    res.raise_for_status()
    payload = res.json()

    imported = 0
    logger = frappe.logger("kobo_pull_awards")
    meta = frappe.get_meta(DOCTYPE)

    for row in payload.get("results", []):
        if log_missing:
            for kobo_key in FIELD_MAP.keys():
                if row.get(kobo_key, None) is None:
                    logger.warning(f"KoBo key missing in row {row.get('_uuid')}: {kobo_key}")

        name = _upsert_row_scalars(row)
        imported += 1

        if with_attachments and name:
            for att in (row.get("_attachments") or []):
                qpath = (att.get("question_xpath") or "").strip()
                qname = qpath.split("/")[-1] if qpath else ""
                target_field = ATTACH_MAP.get(qname)
                df = meta.get_field(target_field) if target_field else None
                if not (target_field and df and df.fieldtype not in ("Table", "Table MultiSelect")):
                    continue
                try:
                    got = _download_attachment(att)
                    if got:
                        fn, content = got
                        _attach_file(DOCTYPE, name, fn, content, target_field)
                except requests.RequestException:
                    pass

    # next page pointer
    next_url = payload.get("next")
    next_page = None
    if next_url and "page=" in next_url:
        from urllib.parse import parse_qs, urlparse
        q = parse_qs(urlparse(next_url).query)
        try:
            next_page = int(q.get("page", [start_page + 1])[0])
        except Exception:
            next_page = start_page + 1

    frappe.db.commit()
    return {"ok": True, "imported": imported, "start_page": start_page, "next_page": next_page}

# ----- OPTIONAL: all pages -----
@frappe.whitelist()
def pull_asset_all(page_size: int = 500, with_attachments: int = 0):
    path = f"/api/v2/assets/{ASSET_UID}/data/?format=json&page_size={page_size}"
    total = 0
    meta = frappe.get_meta(DOCTYPE)

    while True:
        res = _kobo_get(path, timeout=120)
        res.raise_for_status()
        payload = res.json()

        for row in payload.get("results", []):
            name = _upsert_row_scalars(row)
            total += 1

            if with_attachments and name:
                for att in (row.get("_attachments") or []):
                    qpath = (att.get("question_xpath") or "").strip()
                    qname = qpath.split("/")[-1] if qpath else ""
                    target_field = ATTACH_MAP.get(qname)
                    df = meta.get_field(target_field) if target_field else None
                    if not (target_field and df and df.fieldtype not in ("Table", "Table MultiSelect")):
                        continue
                    try:
                        got = _download_attachment(att)
                        if got:
                            fn, content = got
                            _attach_file(DOCTYPE, name, fn, content, target_field)
                    except requests.RequestException:
                        pass

        nxt = payload.get("next")
        if not nxt:
            break
        path = nxt

    frappe.db.commit()
    return {"ok": True, "imported": total}
