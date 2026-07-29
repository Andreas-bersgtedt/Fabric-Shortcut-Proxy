# Security Policy

## Credential Management

**CRITICAL**: All credentials (passwords, API keys, tokens) MUST be loaded from environment variables, never hardcoded in configuration files or code.

### Files with Sensitive Information

The following files may contain credentials and are **never** committed to version control:

```
config.connection.json       # Database connection URL with credentials
config.system.json          # AWS keys for S3 access
config.performance.json     # Optional: reserved for future sensitive settings
.env                        # Local development environment variables
.env.local                  # Local overrides (not committed)
secrets.json               # Any secret material (never commit)
```

All these files are protected by `.gitignore` to prevent accidental commits.

### Required Environment Variables

For production deployments, set these environment variables:

```bash
# Database connection (REQUIRED if DB_URL not in config.connection.json)
export DB_URL="mssql+aioodbc://username:password@host:1433/database?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"

# AWS S3 credentials (REQUIRED for S3 access)
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_SESSION_TOKEN="optional_session_token"  # For temporary credentials

# Optional: override default control port
export CONTROL_PORT="9200"
export PORT="9000"
```

### Configuration File Format

Config files contain only **empty** or **non-sensitive** values:

```json
{
  "connection": {
    "db_url": "",            # Populated from DB_URL environment variable
    "db_max_retries": 3,     # OK: non-sensitive setting
    "db_retry_backoff_seconds": 1
  }
}
```

When the application starts:
1. Load config from JSON file
2. Override `db_url` from `DB_URL` environment variable (if set)
3. Validate no hardcoded credentials remain

### How to Set Credentials Locally

#### Windows PowerShell

```powershell
$env:DB_URL = "mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"
$env:AWS_ACCESS_KEY_ID = "your_key"
$env:AWS_SECRET_ACCESS_KEY = "your_secret"

# Then start the application
python manager.py
```

#### Linux / macOS Bash

```bash
export DB_URL="mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server"
export AWS_ACCESS_KEY_ID="your_key"
export AWS_SECRET_ACCESS_KEY="your_secret"

python manager.py
```

#### Using .env file (development only)

Create `.env.local` in the project root (never commit):

```bash
DB_URL=mssql+aioodbc://user:pass@server:1433/db?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
```

Then load it before running:

```bash
source .env.local          # macOS/Linux
Get-Content .env.local | ForEach-Object { if ($_) { $_ -split '=' | % { $env:$_[0] = $_[1] } } }  # PowerShell
```

### Credential Scrubbing in Logs

All logging automatically scrubs credentials using the `security.credentials` module:

```python
from security import scrub_secrets, scrub_dict

# Scrub individual strings
scrubbed = scrub_secrets("password=mysecret123")  # → "password=[REDACTED]"

# Scrub dictionary recursively
config_copy = scrub_dict(config)  # → all password fields become [REDACTED]
```

**Important**: Never print raw config objects or database URLs to logs. Always use `scrub_dict()` or `scrub_secrets()`.

### Validating Configuration Files

On startup, the application validates that no hardcoded credentials exist:

```python
from security import validate_no_hardcoded_credentials

# Raises ValueError if credentials found
validate_no_hardcoded_credentials(config)
```

This check runs automatically during application startup and will fail loudly if credentials are detected.

## Git History Cleanup

If credentials have been committed to git history, use `git-filter-branch` or `bfg-repo-cleaner` to remove them:

```bash
# Using bfg (simpler, recommended)
bfg --replace-text passwords.txt --no-blob-protection

# Or using git-filter-branch (more control)
git filter-branch --tree-filter 'sed -i "s/mysecret123/[REDACTED]/g"' HEAD
```

After cleaning history, force-push (carefully, on a feature branch only):

```bash
git push origin --force-with-lease
```

## Deployment Checklist

Before deploying to production:

- [ ] All config JSON files have empty credential fields (`""`)
- [ ] All credentials are set via environment variables
- [ ] `.gitignore` includes `config.*.json` entries
- [ ] Run `git log --all -p` to verify no recent commits contain passwords
- [ ] Test that application starts successfully with only env vars (no JSON creds)
- [ ] Review logs for any credential leaks using `scrub_secrets()`
- [ ] Store credentials in:
  - Azure Key Vault (recommended for Azure deployments)
  - AWS Secrets Manager (for AWS)
  - HashiCorp Vault (for hybrid/multi-cloud)
  - Or your organization's secure credential store

## Incident Response

If credentials are exposed:

1. **Immediately rotate** all exposed credentials
2. **Purge git history** using the steps above
3. **Audit logs** for unauthorized access patterns
4. **Update code** to use `scrub_secrets()` in relevant places
5. **Run** `git log --all -p | grep -i password` to verify they're gone
6. **Force-push** cleaned history to all branches
7. **Notify** security team and affected parties

## Security Best Practices

1. **Never commit sensitive files**
   - Use `.gitignore` (already configured)
   - Use pre-commit hooks to catch credentials before commit

2. **Use environment variables for all credentials**
   - Easy to rotate without code changes
   - Secrets never stored in git
   - Different secrets per environment (dev/staging/prod)

3. **Scrub logs and tracebacks**
   - Application uses `scrub_secrets()` automatically
   - Verify sensitive fields in error messages are scrubbed
   - Use `scrub_dict()` before logging config objects

4. **Rotate credentials regularly**
   - S3 keys: at least quarterly
   - Database passwords: as per organizational policy
   - API tokens: per service requirements

5. **Audit access logs**
   - Review failed authentication attempts
   - Monitor S3 access patterns for anomalies
   - Check database connection logs for unauthorized access

## Contact

For security issues, **do not** create public GitHub issues. Instead:
- Contact the security team directly
- Use GitHub's private vulnerability disclosure feature
- Follow responsible disclosure timeline (90 days)

---

**Last Updated**: 2026-07-28  
**Status**: ACTIVE - All config files cleaned, credential scrubbing implemented
