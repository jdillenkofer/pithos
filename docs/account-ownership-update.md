# One-time account ownership update

This update is required when upgrading an existing installation from Pithos
v0.45.x or earlier to v0.46.x or later.

When upgrading a database that already contains buckets, the account-ownership
migration assigns the temporary account ID `legacy` to every existing bucket
and SQL credential. This keeps all rows assigned to an account while the real
ownership mapping is being applied; `legacy` is not intended as a permanent
account ID.

Before serving client traffic with the upgraded installation, update every
existing bucket to its real owning account. If all existing buckets belong to
one account, run this once after the schema migration:

```sql
UPDATE buckets
SET owner_account_id = 'storage-account'
WHERE owner_account_id = 'legacy';
```

For multiple accounts, use separate updates with explicit bucket names (or an
equivalent reviewed mapping) so that every bucket receives the correct owner:

```sql
UPDATE buckets
SET owner_account_id = 'team-a'
WHERE name IN ('team-a-assets', 'team-a-backups');

UPDATE buckets
SET owner_account_id = 'team-b'
WHERE name IN ('team-b-assets');
```

When the SQL credential provider is used, update its migrated credentials to
the same account IDs and choose stable principal IDs as part of the same
maintenance operation:

```sql
UPDATE authentication_credentials
SET account_id = 'storage-account',
    principal_id = access_key_id
WHERE account_id = 'legacy';
```

File and environment credentials must instead be updated in their respective
configuration source. Verify that no placeholder ownership remains before
resuming client traffic:

```sql
SELECT name FROM buckets WHERE owner_account_id = 'legacy';
SELECT access_key_id FROM authentication_credentials WHERE account_id = 'legacy';
```

Both queries must return no rows. Account IDs on credentials must exactly match
the `owner_account_id` values assigned to their buckets.
