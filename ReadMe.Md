Prerequisites (one‑time)

Export Google Chat with Google Takeout (messages + attachments). Unzip it so you have Takeout/Google Chat/.../messages.json and any media per conversation. [tomtalks.blog]
In your Microsoft 365 tenant, have an admin register an app in Entra ID (Azure AD) and grant application permissions:

Teamwork.Migrate.All (required for import). Delegated auth isn’t supported for import. [teachersco...pslive.com]
For files: add Files.ReadWrite.All and Sites.ReadWrite.All (to upload to the Team’s SharePoint). See also the channel files folder API. [learn.microsoft.com]


Ensure all users you’ll map as senders exist in Entra ID (so we can set the from identity). [teachersco...pslive.com]


Notes & limits
• You can import into new teams/channels (standard) in migration mode or existing channels/chats via startMigration. Inline images are supported; rich media like videos are out‑of‑scope for import. After posting, call completeMigration. 
• Throttling: Microsoft documents migration ~5 RPS per channel; the general send API is not for migration. 
• Files shown in Teams messages must already live in SharePoint/OneDrive; we’ll upload there and reference the link/attachment. Some scenarios require shared permissions to let recipients open the file.

python gchat_takeout_to_teams.py --settings settings.yaml

# 0) (One time) pip install
pip install msal requests pyyaml

# 1) Transform Google Takeout
python gchat_takeout_to_teams.py --settings settings.yaml
# -> fill staging/users_map.csv with GoogleEmail -> AAD objectId mappings

# 2) Import into Teams
python teams_importer.py --settings settings.yaml --channel-key <channel_key> \
  --conversation-ts "2023-01-01T00:00:00Z"
