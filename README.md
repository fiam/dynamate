# dynamate

DynamoDB swiss‑army knife with a TUI for browsing tables and querying items.

**Create Table (CLI)**

Command:

```bash
# minimal
./target/debug/dynamate create-table --table demo --pk PK:S

# with sort key
./target/debug/dynamate create-table --table demo --pk PK:S --sk SK:S

# with indexes
./target/debug/dynamate create-table \
  --table demo \
  --pk PK:S \
  --sk SK:S \
  --gsi GSI1:GSI1PK:S \
  --gsi GSI2:GSI2PK:N:GSI2SK:S:include=owner,status \
  --lsi LSI1:LSI1SK:S:keys_only
```

Syntax rules:

1. `--pk NAME:TYPE` is required. `TYPE` is `S`, `N`, or `B` (string/number/binary).
2. `--sk NAME:TYPE` is optional.
3. `--gsi NAME:PK:PK_TYPE[:SK:SK_TYPE][:PROJECTION]` can be repeated. `SK` is optional.
4. `--lsi NAME:SK:SK_TYPE[:PROJECTION]` can be repeated. Requires a table sort key.
5. `PROJECTION` tokens are `all`, `keys_only`, or `include=attr1,attr2`.

Notes:

1. GSI sort keys are optional.
2. If `include=...` is used, the list must include at least one attribute.
3. Index names must be unique across GSIs and LSIs.
