"""
Credential management and scrubbing utilities.

Prevents passwords, API keys, and tokens from leaking into logs,
tracebacks, or configuration dumps. All credentials should be
loaded from environment variables or secure stores, never hardcoded.
"""
from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlparse, urlunparse


# Patterns for detecting sensitive data in text
_SENSITIVE_PATTERNS = [
    (r'(?i)(password|passwd|pwd)\s*[:=]\s*([^\s,};"\]]+)', 'PASSWORD'),
    (r'(?i)(api[_-]?key|apikey|token)\s*[:=]\s*([^\s,};"\]]+)', 'API_KEY'),
    (r'(?i)(secret|secret[_-]?key)\s*[:=]\s*([^\s,};"\]]+)', 'SECRET'),
    (r'(?i)(authorization|auth)\s*[:=]\s*Bearer\s+([^\s,};"\]]+)', 'TOKEN'),
    (r'(?i)(username|user)\s*[:=]\s*([^\s@:,};"\]]+)', 'USERNAME'),
    (r'mssql\+aioodbc://([^:]+):([^@]+)@', 'DB_CREDENTIALS'),
    (r'(https?://)[^:]+:([^@]+)@', 'BASIC_AUTH'),
    (r'(?i)(aws_secret_access_key|aws_session_token)\s*[:=]\s*([^\s,};"\]]+)', 'AWS_SECRET'),
]


# Key-name tokens that denote a credential value. Uses specific substrings
# (e.g. 'access_key', not bare 'key') so non-secret keys like 'key_column'
# are not falsely flagged as credentials.
_SENSITIVE_KEY_SUBSTRINGS = (
    'password', 'passwd', 'pwd', 'passphrase',
    'secret',
    'token',
    'api_key', 'apikey',
    'access_key',
    'private_key',
    'credential',
)


def _is_sensitive_key(key: str) -> bool:
    """Return True if a config/dict key name denotes a credential value."""
    key_lower = key.lower()
    return any(token in key_lower for token in _SENSITIVE_KEY_SUBSTRINGS)


def scrub_secrets(value: str) -> str:
    """Remove sensitive data from a string, replacing with [REDACTED].
    
    Args:
        value: String that may contain credentials
        
    Returns:
        String with credentials replaced by [REDACTED]
    """
    if not isinstance(value, str):
        return value
    
    scrubbed = value
    for pattern, label in _SENSITIVE_PATTERNS:
        scrubbed = re.sub(
            pattern,
            lambda m: m.group(0)[:len(m.group(0)) - len(m.group(2))] + '[REDACTED]' if len(m.groups()) > 1 else '[REDACTED]',
            scrubbed,
            flags=re.IGNORECASE
        )
    
    return scrubbed


def scrub_dict(obj: dict[str, Any]) -> dict[str, Any]:
    """Recursively scrub sensitive values from a dictionary.
    
    Args:
        obj: Dictionary that may contain credentials
        
    Returns:
        Dictionary with credentials replaced by [REDACTED]
    """
    if not isinstance(obj, dict):
        return obj
    
    scrubbed = {}
    for key, value in obj.items():
        # Check if key name suggests credential
        if _is_sensitive_key(key):
            scrubbed[key] = '[REDACTED]'
        elif isinstance(value, dict):
            scrubbed[key] = scrub_dict(value)
        elif isinstance(value, str):
            scrubbed[key] = scrub_secrets(value)
        else:
            scrubbed[key] = value
    
    return scrubbed


def scrub_database_url(url: str) -> str:
    """Remove credentials from a database connection URL.
    
    Args:
        url: Connection string like 'mssql+aioodbc://user:pass@host:port/db'
        
    Returns:
        URL with password replaced: 'mssql+aioodbc://user:***@host:port/db'
    """
    if not url or '://' not in url:
        return url
    
    try:
        parsed = urlparse(url)
        if parsed.password:
            # Reconstruct without password
            netloc = parsed.hostname or ''
            if parsed.port:
                netloc = f"{netloc}:{parsed.port}"
            if parsed.username:
                netloc = f"{parsed.username}:***@{netloc}"
            else:
                netloc = f"***@{netloc}"
            
            scrubbed = urlunparse((
                parsed.scheme,
                netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment
            ))
            return scrubbed
    except Exception:
        pass
    
    return scrub_secrets(url)


def load_from_env(var_name: str, default: str = "") -> str:
    """Load a credential from environment variable.
    
    Args:
        var_name: Environment variable name
        default: Default value if not set
        
    Returns:
        Environment variable value or default
    """
    return os.getenv(var_name, default)


def require_env(var_name: str, description: str = "") -> str:
    """Load a required credential from environment variable.
    
    Args:
        var_name: Environment variable name
        description: Description for error message
        
    Returns:
        Environment variable value
        
    Raises:
        ValueError: If environment variable is not set
    """
    value = os.getenv(var_name)
    if not value:
        msg = f"Required credential '{var_name}' not set"
        if description:
            msg = f"{msg}: {description}"
        raise ValueError(msg)
    return value


# Credentials that should NEVER be in config files
_REQUIRED_ENV_CREDENTIALS = {
    'DB_URL': 'Database connection string (e.g., mssql+aioodbc://user:pass@host/db)',
    'AWS_ACCESS_KEY_ID': 'AWS access key (S3 authentication)',
    'AWS_SECRET_ACCESS_KEY': 'AWS secret key (S3 authentication)',
}


def validate_no_hardcoded_credentials(config_obj: Any) -> None:
    """Scan a configuration object for hardcoded credentials.
    
    Raises:
        ValueError: If hardcoded credentials are detected
    """
    if isinstance(config_obj, dict):
        for key, value in config_obj.items():
            # Check key name
            if _is_sensitive_key(key):
                if isinstance(value, str) and value and value not in ('', '[REDACTED]'):
                    raise ValueError(
                        f"SECURITY VIOLATION: Hardcoded credential found in config.{key}. "
                        f"Move to environment variable. Current value: {scrub_secrets(str(value))}"
                    )
            
            # Check value for patterns
            if isinstance(value, str):
                if scrub_secrets(value) != value:
                    raise ValueError(
                        f"SECURITY VIOLATION: Credential pattern detected in config.{key}. "
                        f"Move to environment variable."
                    )
            elif isinstance(value, dict):
                validate_no_hardcoded_credentials(value)
