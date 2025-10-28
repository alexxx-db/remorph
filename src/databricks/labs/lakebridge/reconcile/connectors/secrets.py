import logging

from databricks.sdk.errors import NotFound

from databricks.labs.lakebridge.connections.credential_manager import DatabricksSecretProvider

logger = logging.getLogger(__name__)


# TODO use CredentialManager to allow for changing secret provider for tests
class SecretsMixin:
    _secrets: DatabricksSecretProvider
    _secret_scope: str

    def _get_secret_or_none(self, secret_key: str) -> str | None:
        """
        Get the secret value given a secret scope & secret key. Log a warning if secret does not exist
        Used To ensure backwards compatibility when supporting new secrets
        """
        try:
            # Return the decoded secret value in string format
            return self._get_secret(secret_key)
        except NotFound as e:
            logger.warning(f"Secret not found: key={secret_key}")
            logger.debug("Secret lookup failed", exc_info=e)
            return None

    def _get_secret(self, secret_key: str) -> str:
        _secret_full_key = f"{self._secret_scope}/{secret_key}"
        return self._secrets.get_secret(_secret_full_key)
