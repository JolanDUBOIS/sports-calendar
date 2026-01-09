# Google credentials Setup

To use Sports Calendar with Google Calendar, you need two files:
- `client_secret.json` — identifies your app to Google
- `token.json` — stores the access token for your Google account

These files must be placed in the user config directory:

```bash
mkdir -p ~/.config/sports-calendar/.credentials
```

Then copy both files there:

```text
~/.config/sports-calendar/.credentials/
├── client_secret.json
└── token.json
```

To create those two credentials files, follow the following steps:

## 1 - Create a Google Cloud Project

1. Go to the [Google Cloud Project Selector](https://console.cloud.google.com/projectselector2/home/dashboard) and click `+ CREATE PROJECT`.
2. Give it a recognizable name (e.g., `Sports Calendar`) and click `CREATE`.

## 2 - Configure OAuth Consent Screen

1. In the top-left menu (☰), go to **APIs & Services > OAuth consent screen**.
2. Select **User Type: External** and click **CREATE**.
3. Fill in the required fields:
    - **App name:** any name you like
    - **User support email:** your email
    - **Developer contact email:** your email
4. Click **SAVE AND CONTINUE** through all remaining screens.

> This step lets your app request permission to access your Google Calendar.

## 3. Create OAuth Client Credentials

1. Go to **APIs & Services > Credentials**.
2. Click `+ CREATE CREDENTIALS` → `OAuth client ID`.
3. Select **Application type: Desktop app**.
4. Give it a name (default is fine) and click **CREATE**.
5. Download the JSON file; this is your `client_secret.json`.

## 4. Enable the Google Calendar API

1. In the top-left menu (☰), go to **APIs & Services > Library**.
2. Search for **Google Calendar API**.
3. Click on the result and then **ENABLE** (If it’s already enabled, you can skip this.)

> This grants your project permission to access your Google Calendar.

## 5. Generate token.json

The first time you run Sports Calendar with your Google account, the app will prompt you to authenticate and grant permission for this project to access your Google Calendar. Once you complete this process, `token.json` will be automatically created in the same folder as `client_secret.json`.
> You do not need to manually create `token.json` — just make sure `client_secret.json` is in the correct location before running the app.

## 6. Additional Notes

- It is recommended to use a **dedicated Gmail account** for Sports Calendar. This is a precaution: in case of a misconfiguration or accidental exposure of credentials, any potential impact is limited to that account rather than your main email. You can then **share the specific calendar** from this account to your main account using Google Calendar’s sharing feature.
- For more details on OAuth setup, refer to the official [Google OAuth documentation](https://developers.google.com/identity/protocols/oauth2).
- Sports Calendar only requires read/write access to your calendars; no other permissions are needed.
