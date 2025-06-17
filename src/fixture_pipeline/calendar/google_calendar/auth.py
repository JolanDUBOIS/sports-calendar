from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google_auth_oauthlib.flow import InstalledAppFlow

from . import logger
from src import ROOT_PATH
from src.config import get_config


class GoogleAuthManager:
    """ TODO """

    SCOPES = ['https://www.googleapis.com/auth/calendar']

    def __init__(self, credentials_file_path: str = None):
        """ TODO """
        self._credentials_file_path = credentials_file_path

    @property
    def credentials_file_path(self) -> str:
        """ TODO """
        if self._credentials_file_path is None:
            self._credentials_file_path = ROOT_PATH / get_config('google.credentials_file_path')
            if self._credentials_file_path is None:
                logger.error('Google credentials file path is not set in the config.')
                raise ValueError('Google credentials file path is not set in the config.')
        
        self._credentials_file_path = Path(self._credentials_file_path)
        if not self._credentials_file_path.exists():
            logger.error(f"Credentials file not found: {self._credentials_file_path}.")
            raise FileNotFoundError(f"Credentials file not found: {self._credentials_file_path}.")

        return self._credentials_file_path

    @property
    def credentials(self) -> Credentials:
        """ TODO """
        creds = None
        token_path = Path('credentials') / 'token.json'
        
        def write_token(token_path: Path, creds: Credentials):
            with token_path.open(mode='w') as token:
                token.write(creds.to_json())

        if token_path.exists():
            try:
                creds = Credentials.from_authorized_user_file(token_path, self.SCOPES)
                if creds and creds.valid:
                    return creds
            except Exception as e:
                logger.warning(f"Failed to load credentials: {e}")
                token_path.unlink(missing_ok=True)

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                write_token(token_path, creds)
                return creds
            except RefreshError:
                logger.warning("Refresh token expired or revoked. Re-authenticating...")
                token_path.unlink(missing_ok=True)

        flow = InstalledAppFlow.from_client_secrets_file(
            self.credentials_file_path,
            self.SCOPES
        )
        creds = flow.run_local_server(port=0, open_browser=False)
        write_token(token_path, creds)

        return creds
