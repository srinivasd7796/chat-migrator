
#!/usr/bin/env python3
# gchat_takeout_to_teams.py
import os, json, csv, base64, argparse, pathlib, re
from datetime import datetime, timezone
from html import escape
import yaml

# --- Helpers -------------------------------------------------------

def iso_ensure_z(dt_str: str) -> str:
    # Accepts ISO strings from Takeout, returns UTC Z format w/ milliseconds trimmed
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def load_user_map(staging_root):
    path = os.path.join(staging_root, "users_map.csv")
    m = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                m[row["google_email"].lower()] = row["aad_object_id"]
    return m

def ensure_user_map(staging_root):
    path = os.path.join(staging_root, "users_map.csv")
    if not os.path.exists(path):
        os.makedirs(staging_root, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["google_email", "aad_object_id", "display_name"])
        print(f"Created user map template: {path}")

def find_takeout_convs(takeout_root):
    # Return list of conversation folders containing messages.json
    convs = []
    for root, dirs, files in os.walk(takeout_root):
        if "messages.json" in files:
            convs.append(root)
    return sorted(convs)

# --- Core ----------------------------------------------------------

def transform_conversation(conv_path, channel_key, out_dir, user_map, inline_images=True):
    with open(os.path.join(conv_path, "messages.json"), encoding="utf-8") as f:
        data = json.load(f)

    msgs = data.get("messages", data if isinstance(data, list) else [])
    os.makedirs(out_dir, exist_ok=True)
    q_path = os.path.join(out_dir, f"{channel_key}.jsonl")
    files_manifest = os.path.join(out_dir, f"{channel_key}_files_manifest.csv")
    fm = open(files_manifest, "w", newline="", encoding="utf-8")
    fm_writer = csv.writer(fm)
    fm_writer.writerow(["source_path", "suggested_name", "message_index"])

    with open(q_path, "w", encoding="utf-8") as q:
        for i, m in enumerate(msgs):
            created = m.get("createTime") or m.get("createdTime") or m.get("created_at") or ""
            text = (m.get("text") or "").strip()
            creator = (m.get("creator") or m.get("sender") or {})
            g_email = (creator.get("email") or creator.get("id") or "").lower()
            aad_object_id = user_map.get(g_email, None)

            # Build HTML body; we’ll append inline <img> tags referencing hostedContents
            body_html = f"<div>{escape(text)}</div>" if text else "<div></div>"

            hosted = []
            temp_id = 1

            # Inline images: for image attachments in the same folder, read and embed
            atts = m.get("attachments") or []
            for att in atts:
                content_type = (att.get("contentType") or "").lower()
                file_path = None

                # Heuristics: Takeout often writes attachments next to messages.json or in /photos, /files
                for guess in [att.get("filePath"), att.get("path"), att.get("name")]:
                    if guess:
                        p = os.path.join(conv_path, guess)
                        if os.path.exists(p):
                            file_path = p
                            break
                # If clearly an image, embed as hostedContents; else put it in files_manifest
                if inline_images and content_type.startswith("image/") and file_path and os.path.getsize(file_path) < 4*1024*1024:
                    with open(file_path, "rb") as fimg:
                        b64 = base64.b64encode(fimg.read()).decode("utf-8")
                    hosted.append({
                        "@microsoft.graph.temporaryId": str(temp_id),
                        "contentBytes": b64,
                        "contentType": content_type or "image/png"
                    })
                    # reference in html
                    body_html = (
                        body_html +
                        f'<div>../hostedContents/{temp_id}/$value</div>'
                    )
                    temp_id += 1
                else:
                    # queue for SharePoint upload; contentUrl will be filled later by importer
                    if file_path:
                        fm_writer.writerow([os.path.abspath(file_path), os.path.basename(file_path), i])

            # Build Teams chatMessage payload
            payload = {
                "createdDateTime": iso_ensure_z(created) if created else None,
                "from": {
                    "user": {
                        "id": aad_object_id or "",  # If blank, Teams will show app name; we can still import
                        "displayName": creator.get("displayName") or "",
                        "userIdentityType": "aadUser" if aad_object_id else "unknownFutureValue"
                    }
                },
                "body": {"contentType": "html", "content": body_html}
            }
            if hosted:
                payload["hostedContents"] = hosted

            q.write(json.dumps(payload, ensure_ascii=False) + "\n")

    fm.close()
    print(f"Wrote queue: {q_path}")
    print(f"Wrote files manifest: {files_manifest}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--settings", default="settings.yaml")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.settings, encoding="utf-8"))
    takeout_root = cfg["takeout_root"]
    staging_root = cfg["staging_root"]
    ensure_user_map(staging_root)
    user_map = load_user_map(staging_root)

    convs = find_takeout_convs(takeout_root)
    print(f"Found {len(convs)} conversations.")
    for conv in convs:
        # Map folder name → channel key; you can add smarter mapping here
        chan_key = pathlib.Path(conv).name.replace(" ", "_")
        out_dir = os.path.join(staging_root, "teams_messages")
        transform_conversation(conv, chan_key, out_dir, user_map, inline_images=True)

if __name__ == "__main__":
    main()
