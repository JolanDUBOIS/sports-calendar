from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow

from . import logger
from src.config.main import config


class GoogleAuthManager:
    """ TODO """

    SCOPES = ['https://www.googleapis.com/auth/calendar']

    def __init__(self, credentials_file_path: Path | str = None):
        """ TODO """
        self.credentials_file_path = config.credentials.client_secret_path if credentials_file_path is None else Path(credentials_file_path)

    @property
    def credentials(self) -> Credentials:
        """ TODO """
        creds = None
        
        def write_token(token_path: Path, creds: Credentials):
            with token_path.open(mode='w') as token:
                token.write(creds.to_json())

        if config.credentials.token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(config.credentials.token_path, self.SCOPES)
                if creds and creds.valid:
                    return creds
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")
                config.credentials.token_path.unlink(missing_ok=True)

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                write_token(config.credentials.token_path, creds)
                return creds
            except RefreshError:
                logger.warning("Refresh token expired or revoked. Re-authenticating...")
                config.credentials.token_path.unlink(missing_ok=True)

        flow = InstalledAppFlow.from_client_secrets_file(
            self.credentials_file_path,
            self.SCOPES
        )
        creds = flow.run_local_server(port=0, open_browser=False)
        write_token(config.credentials.token_path, creds)

        return creds
