
#!/usr/bin/env python3
# teams_importer.py
import os, json, time, argparse, yaml, requests, math
from urllib.parse import quote
from msal import ConfidentialClientApplication

GRAPH = "https://graph.microsoft.com"

def msal_app(tenant, client_id, client_secret):
    return ConfidentialClientApplication(
        client_id, authority=f"https://login.microsoftonline.com/{tenant}",
        client_credential=client_secret
    )

def get_token(app, scopes=["https://graph.microsoft.com/.default"]):
    res = app.acquire_token_silent(scopes, account=None) or app.acquire_token_for_client(scopes=scopes)
    if "access_token" not in res:
        raise RuntimeError(f"Auth failed: {res}")
    return res["access_token"]

def h(auth): return {"Authorization": f"Bearer {auth}", "Content-Type": "application/json"}

def backoff_try(fn, *args, **kwargs):
    delay = 1.0
    for i in range(8):
        r = fn(*args, **kwargs)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(delay); delay = min(delay*2, 60); continue
        return r
    return r

# ---------- Files (SharePoint) ------------------------------------

def get_channel_files_folder(auth, team_id, channel_id):
    # GET /teams/{team-id}/channels/{channel-id}/filesFolder -> driveItem (has parentReference.driveId)
    r = backoff_try(requests.get, f"{GRAPH}/v1.0/teams/{team_id}/channels/{channel_id}/filesFolder", headers=h(auth))
    r.raise_for_status()
    di = r.json()
    drive_id = di["parentReference"]["driveId"]
    item_id  = di["id"]
    return drive_id, item_id

def upload_small_file(auth, drive_id, parent_item_id, local_path, save_as_name=None):
    name = quote(save_as_name or os.path.basename(local_path))
    url = f"{GRAPH}/v1.0/drives/{drive_id}/items/{parent_item_id}:/{name}:/content"
    with open(local_path, "rb") as f:
        r = backoff_try(requests.put, url, headers={"Authorization": f"Bearer {auth}"}, data=f)
    r.raise_for_status()
    return r.json()  # driveItem (has webUrl)

def upload_large_file(auth, drive_id, parent_item_id, local_path, save_as_name=None, chunk=5*1024*1024):
    name = quote(save_as_name or os.path.basename(local_path))
    session_url = f"{GRAPH}/v1.0/drives/{drive_id}/items/{parent_item_id}:/{name}:/createUploadSession"
    s = requests.post(session_url, headers=h(auth)).json()
    upload_url = s["uploadUrl"]
    size = os.path.getsize(local_path)
    sent = 0
    with open(local_path, "rb") as f:
        while sent < size:
            to_send = min(chunk, size - sent)
            data = f.read(to_send)
            headers = {
                "Content-Length": str(to_send),
                "Content-Range": f"bytes {sent}-{sent+to_send-1}/{size}"
            }
            r = backoff_try(requests.put, upload_url, headers=headers, data=data)
            if r.status_code in (200, 201):  # completed
                return r.json()
            elif r.status_code not in (202,):
                r.raise_for_status()
            sent += to_send
    raise RuntimeError("Upload session did not complete")

# ---------- Migration Mode ----------------------------------------

def start_channel_migration(auth, team_id, channel_id, conversation_creation_iso=None):
    # POST /beta/teams/{team-id}/channels/{channel-id}/startMigration
    url = f"{GRAPH}/beta/teams/{team_id}/channels/{channel_id}/startMigration"
    body = {"conversationCreationDateTime": conversation_creation_iso} if conversation_creation_iso else {}
    r = backoff_try(requests.post, url, headers=h(auth), data=json.dumps(body))
    if r.status_code not in (204, 202): r.raise_for_status()

def complete_channel_migration(auth, team_id, channel_id):
    url = f"{GRAPH}/beta/teams/{team_id}/channels/{channel_id}/completeMigration"
    r = backoff_try(requests.post, url, headers=h(auth))
    if r.status_code not in (204,): r.raise_for_status()

# ---------- Messages ----------------------------------------------

def post_import_message(auth, team_id, channel_id, msg):
    # POST /v1.0/teams/{team-id}/channels/{channel-id}/messages  (with createdDateTime & from)
    url = f"{GRAPH}/v1.0/teams/{team_id}/channels/{channel_id}/messages"
    r = backoff_try(requests.post, url, headers=h(auth), data=json.dumps(msg))
    if r.status_code not in (200, 201): r.raise_for_status()
    return r.json()

# ---------- Main ---------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings", default="settings.yaml")
    ap.add_argument("--channel-key", help="Which channel_key to import (matches <channel_key>.jsonl)")
    ap.add_argument("--conversation-ts", help="Optional ISO to set conversationCreationDateTime on startMigration")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.settings, encoding="utf-8"))
    app = msal_app(cfg["tenant_id"], cfg["client_id"], cfg["client_secret"])
    token = get_token(app)

    # Choose target channel
    # For simplicity here, we assume you’ve set cfg.team_id and a single channel_id in settings.yaml
    team_id = cfg["team_id"]
    channel_id = cfg["channels"][0]["channel_id"]

    # 1) Start migration on existing channel (or create new team/channel in migration mode in an extended version)
    print("Starting migration...")
    start_channel_migration(token, team_id, channel_id, args.conversation_ts)

    # 2) Resolve files folder for the channel
    drive_id, parent_item_id = get_channel_files_folder(token, team_id, channel_id)
    print(f"Channel drive: {drive_id}, parent item: {parent_item_id}")

    # 3) Upload files referenced in manifest (<=4 MB simple; else upload session)
    staging_root = cfg["staging_root"]
    chan_key = args.channel_key or cfg["channels"][0]["source_label"].replace(" ", "_")
    manifest = os.path.join(staging_root, "teams_messages", f"{chan_key}_files_manifest.csv")
    web_urls_by_path = {}
    if os.path.exists(manifest):
        import csv, os
        with open(manifest, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                src, save_name, idx = row["source_path"], row["suggested_name"], int(row["message_index"])
                size = os.path.getsize(src)
                if size <= 4*1024*1024:
                    di = upload_small_file(token, drive_id, parent_item_id, src, save_as_name=save_name)
                else:
                    di = upload_large_file(token, drive_id, parent_item_id, src, save_as_name=save_name)
                web_urls_by_path[src] = di.get("webUrl")  # URL we’ll insert into the message
        print(f"Uploaded {len(web_urls_by_path)} file(s) to SharePoint.")

    # 4) Post messages with inline images + links to files
    q_path = os.path.join(staging_root, "teams_messages", f"{chan_key}.jsonl")
    count = 0
    with open(q_path, encoding="utf-8") as q:
        for line in q:
            msg = json.loads(line)

            # If this Google message had docs (non-image), append links at the bottom (illustrative)
            # In a richer version, we’d track which file belonged to which message row by index.
            # Here we simply add links section if any uploads exist.
            if web_urls_by_path:
                links_html = "".join([f'<div>{u}{u}</a></div>' for u in web_urls_by_path.values()])
                msg["body"]["content"] = msg["body"]["content"] + links_html

            post_import_message(token, team_id, channel_id, msg)
            count += 1
            if count % 50 == 0:
                print(f"Posted {count} messages...")

    print(f"Posted {count} messages in total.")

    # 5) Complete migration
    print("Completing migration...")
    complete_channel_migration(token, team_id, channel_id)
    print("Done.")

if __name__ == "__main__":
    main()
