import base64
from pathlib import Path
import logging
from typing import Any, Protocol

import yaml

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.lakebridge.connections.env_getter import EnvGetter

logger = logging.getLogger(__name__)


class SecretProvider(Protocol):
    def get_secret(self, key: str) -> str:
        pass


class LocalSecretProvider(SecretProvider):
    def get_secret(self, key: str) -> str:
        return key


class EnvSecretProvider(SecretProvider):
    def __init__(self, env_getter: EnvGetter):
        self._env_getter = env_getter

    def get_secret(self, key: str) -> str:
        try:
            return self._env_getter.get(str(key))
        except KeyError:
            logger.debug(f"Environment variable {key} not found. Falling back to actual value")
            return key


class DatabricksSecretProvider(SecretProvider):
    def __init__(self, ws: WorkspaceClient, scope: str):
        self._ws = ws
        self._scope = scope

    def get_secret(self, key: str) -> str:
        try:
            secret = self._ws.secrets.get_secret(self._scope, key)
        except NotFound as e:
            raise NotFound(f"Secret not found in scope '{self._scope}' with key '{key}'") from e
        if secret.value is None:
            raise ValueError(f"Secret '{self._scope}/{key}' has no value")
        try:
            return base64.b64decode(secret.value).decode("utf-8")
        except UnicodeDecodeError as e:
            raise UnicodeDecodeError(
                "utf-8",
                key.encode(),
                0,
                1,
                f"Secret '{self._scope}/{key}' has Base64 bytes that cannot be decoded to utf-8: {e}",
            ) from e


class CredentialManager:
    def __init__(self, credentials: dict, secret_providers: dict[str, SecretProvider]):
        self._credentials = credentials
        self._default_vault = self._credentials.get('secret_vault_type', 'local').lower()
        self._provider = secret_providers.get(self._default_vault)
        if not self._provider:
            raise ValueError(f"Unsupported secret vault type: {self._default_vault}")

    def get_credentials(self, source: str) -> dict[str, Any]:
        if source not in self._credentials:
            raise KeyError(f"Source system: {source} credentials not found")

        value = self._credentials[source]
        if not isinstance(value, dict):
            raise KeyError(f"Invalid credential format for source: {source}")

        # Safe to cast: we verified value is a dict, so _resolve_credentials returns a dict
        return self._resolve_credentials(value)

    def _resolve_credentials(self, value: dict[str, Any]) -> dict[str, Any]:
        """Recursively resolve credentials, handling nested dictionaries and secret values.

        rules:
        - dict: Recursively process each key-value pair
        - list: Recursively process each item
        - str: Apply secret provider (resolve from env vars or return as-is)
        - Other types (int, bool, None, float): Return unchanged

            Processed value with the same structure but string values resolved
        """
        if isinstance(value, dict):
            return {k: self._resolve_credentials(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._resolve_credentials(item) for item in value]
        if isinstance(value, str):
            return self._get_secret_value(value)
        # For int, bool, None, float, etc., return as-is
        return value

    def _get_secret_value(self, key: str) -> str:
        """Apply the configured secret provider to resolve a string value."""
        assert self._provider is not None
        return self._provider.get_secret(key)


def _get_home() -> Path:
    return Path.home()


def cred_file(product_name) -> Path:
    return _get_home() / ".databricks" / "labs" / product_name / ".credentials.yml"


def _load_credentials(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Credentials file not found at {path}") from e


def create_credential_manager(
    product_name: str,
    env_getter: EnvGetter,
    creds_path: Path | None = None,
    ws: WorkspaceClient | None = None,
) -> CredentialManager:
    if creds_path is None:
        creds_path = cred_file(product_name)
    creds = _load_credentials(creds_path)

    secret_providers: dict[str, SecretProvider] = {
        'local': LocalSecretProvider(),
        'env': EnvSecretProvider(env_getter),
    }

    if (creds.get('secret_vault_type') or '').lower() == 'databricks':
        scope = creds.get('secret_vault_name')
        if not scope:
            raise ValueError("secret_vault_name is required when secret_vault_type is 'databricks'")
        secret_providers['databricks'] = DatabricksSecretProvider(ws or WorkspaceClient(), scope)

    return CredentialManager(creds, secret_providers)
