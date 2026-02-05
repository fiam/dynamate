#!/usr/bin/env -S uv run --script
# /// script
# dependencies = [
#   "boto3",
# ]
# ///
"""
Seed a DynamoDB table (single-table design) with a Chinook-like dataset.

Item types inserted:
- Artist, Album, Track
- Customer, Invoice, InvoiceLine

Keys:
- PK and SK are strings, using patterns like:
  ARTIST#<id> / ARTIST#<id>
  ARTIST#<id> / ALBUM#<id>
  ALBUM#<id>  / TRACK#<id>
  CUSTOMER#<id> / PROFILE
  CUSTOMER#<id> / INVOICE#<id>
  INVOICE#<id>  / LINE#<n>

Example (DynamoDB Local):
  uv run --script docker/seed.py --endpoint-url http://localhost:8000 --table dyno-music
"""

import argparse
from typing import List, Optional
from decimal import Decimal
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError


def parse_args():
    p = argparse.ArgumentParser(description="Create & seed a Chinook-like dataset in DynamoDB")
    p.add_argument("--endpoint-url", help="DynamoDB endpoint (use for DynamoDB Local)")
    p.add_argument("--table", required=True, help="DynamoDB table name")
    p.add_argument("--region", default="us-east-1", help="AWS region (default: us-east-1)")
    p.add_argument("--recreate", action="store_true", help="Drop table first if it exists")
    p.add_argument(
        "--recreate-if-missing-indexes",
        action="store_true",
        help="Drop/recreate if table exists but is missing required GSIs/LSIs",
    )
    p.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip seeding if the table already exists (ignored with --recreate)",
    )
    # Sizes
    p.add_argument("--artists", type=int, default=60)
    p.add_argument("--albums-per-artist", type=int, default=6)
    p.add_argument("--tracks-per-album", type=int, default=12)
    p.add_argument("--customers", type=int, default=200)
    p.add_argument("--invoices-per-customer", type=int, default=5)
    p.add_argument("--lines-per-invoice", type=int, default=6)
    return p.parse_args()


# ---------- Helpers ----------

ARTIST_NAMES = [
    "The Meteors", "Sonic Avenue", "Neon Fields", "Crimson Tide", "Blue Horizon",
    "The Wanderers", "Velvet Echo", "Golden Owls", "Stone Harbor", "Silver Lining",
    "Paper Tigers", "Echo Beach", "Analog Youth", "Glass Engine", "Static Ritual"
]
ALBUM_WORDS = ["Echoes", "Horizons", "Reflections", "Neon", "Fragments", "Voyager",
               "Midnight", "Catalyst", "Blueprints", "Origins", "Spectra", "Signals"]
GENRES = ["Rock", "Pop", "Indie", "Jazz", "Blues", "Classical", "Electronic", "Hip-Hop"]
INSTRUMENTS = ["Guitar", "Bass", "Drums", "Keys", "Synth", "Violin", "Sax", "Trumpet"]
CITIES = ["Seattle", "Berlin", "Madrid", "Reykjavik", "Austin", "Dublin", "Sydney", "Tokyo"]

def rand_id(prefix: str, n: int = 8) -> str:
    return f"{prefix}#{''.join(random.choices(string.ascii_uppercase + string.digits, k=n))}"

def rand_title() -> str:
    return f"{random.choice(ALBUM_WORDS)} {random.choice(ALBUM_WORDS)}"

def rand_track_name() -> str:
    return f"{random.choice(['Lost','Found','Faded','Bright','Hidden','Open'])} {random.choice(['Waves','Lines','Skies','Rooms','Streets','Windows'])}"

def rand_name() -> str:
    first = random.choice(["Alex","Sam","Charlie","Taylor","Morgan","Riley","Jordan","Casey","Avery","Quinn"])
    last = random.choice(["Smith","Lee","Martinez","Kim","Nguyen","Garcia","Patel","Cohen","Novak","Khan"])
    return f"{first} {last}"

def rand_email(name: str) -> str:
    handle = name.lower().replace(" ", ".")
    domain = random.choice(["example.com","mail.test","demo.local"])
    return f"{handle}@{domain}"

def add_index_keys(item: dict, item_type: str, pk: str, sk: str, lsi1sk: Optional[str] = None) -> None:
    item["GSI1PK"] = f"TYPE#{item_type}"
    item["GSI1SK"] = f"{pk}#{sk}"
    item["LSI1SK"] = lsi1sk if lsi1sk is not None else sk

def index_key_schema(index: dict) -> dict:
    return {k["KeyType"]: k["AttributeName"] for k in index.get("KeySchema", [])}

def missing_indexes(desc: dict) -> List[str]:
    table = desc.get("Table", {})
    gsis = {g["IndexName"]: g for g in table.get("GlobalSecondaryIndexes", [])}
    lsis = {l["IndexName"]: l for l in table.get("LocalSecondaryIndexes", [])}

    missing = []
    gsi1 = gsis.get("GSI1")
    if not gsi1 or index_key_schema(gsi1) != {"HASH": "GSI1PK", "RANGE": "GSI1SK"}:
        missing.append("GSI1")
    lsi1 = lsis.get("LSI1")
    if not lsi1 or index_key_schema(lsi1) != {"HASH": "PK", "RANGE": "LSI1SK"}:
        missing.append("LSI1")
    return missing

def wait_table_active(dynamo, table_name: str, timeout_s: int = 60) -> None:
    start = time.time()
    while True:
        try:
            desc = dynamo.describe_table(TableName=table_name)
            status = desc["Table"]["TableStatus"]
            if status == "ACTIVE":
                return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code != "ResourceNotFoundException":
                raise
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Table {table_name} not ACTIVE after {timeout_s}s")
        time.sleep(0.5)

def ensure_table(dynamo, table_name: str, recreate: bool, recreate_if_missing_indexes: bool) -> bool:
    exists = False
    desc = None
    try:
        desc = dynamo.describe_table(TableName=table_name)
        exists = True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    if exists and not recreate:
        missing = missing_indexes(desc)
        if missing:
            msg = f"Existing table {table_name} missing indexes: {', '.join(missing)}."
            if recreate_if_missing_indexes:
                print(f"{msg} Recreating…")
                dynamo.delete_table(TableName=table_name)
                wait_deleted(dynamo, table_name)
                exists = False
            else:
                raise RuntimeError(f"{msg} Re-run with --recreate (or --recreate-if-missing-indexes).")

    if exists and recreate:
        print(f"Deleting existing table {table_name}…")
        dynamo.delete_table(TableName=table_name)
        wait_deleted(dynamo, table_name)

    if not exists or recreate:
        print(f"Creating table {table_name} (PK, SK, GSI1, LSI1)…")
        dynamo.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
                {"AttributeName": "GSI1PK", "AttributeType": "S"},
                {"AttributeName": "GSI1SK", "AttributeType": "S"},
                {"AttributeName": "LSI1SK", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "GSI1",
                    "KeySchema": [
                        {"AttributeName": "GSI1PK", "KeyType": "HASH"},
                        {"AttributeName": "GSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "LSI1",
                    "KeySchema": [
                        {"AttributeName": "PK", "KeyType": "HASH"},
                        {"AttributeName": "LSI1SK", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                }
            ],
            BillingMode="PAY_PER_REQUEST",
        )
    wait_table_active(dynamo, table_name)
    print("Table is ACTIVE.")
    return exists

def wait_deleted(dynamo, table_name: str, timeout_s: int = 60) -> None:
    start = time.time()
    while True:
        try:
            dynamo.describe_table(TableName=table_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return
            else:
                raise
        if time.time() - start > timeout_s:
            raise TimeoutError(f"Table {table_name} not deleted after {timeout_s}s")
        time.sleep(0.5)

# ---------- Seeding ----------

def seed_data(table, sizes):
    """
    Inserts items in a single-table layout:

    ARTIST#A / ARTIST#A                {type: 'Artist', ...}
    ARTIST#A / ALBUM#B                 {type: 'Album', artist_id: 'ARTIST#A'}
    ALBUM#B  / TRACK#T                 {type: 'Track', album_id: 'ALBUM#B'}
    CUSTOMER#C / PROFILE               {type: 'Customer', ...}
    CUSTOMER#C / INVOICE#I             {type: 'Invoice', total: ..., ts: ...}
    INVOICE#I  / LINE#N                {type: 'InvoiceLine', track_id: 'TRACK#T', price: ...}
    """
    artists_ct = sizes["artists"]
    albums_per_artist = sizes["albums_per_artist"]
    tracks_per_album = sizes["tracks_per_album"]
    customers_ct = sizes["customers"]
    invoices_per_cust = sizes["invoices_per_customer"]
    lines_per_invoice = sizes["lines_per_invoice"]

    artist_ids = []
    album_ids = []
    track_ids = []

    with table.batch_writer(overwrite_by_pkeys=["PK", "SK"]) as b:
        # Artists / Albums / Tracks
        for _ in range(artists_ct):
            artist_id = rand_id("ARTIST")
            artist_ids.append(artist_id)
            item = {
                "PK": artist_id,
                "SK": artist_id,
                "type": "Artist",
                "name": random.choice(ARTIST_NAMES),
                "city": random.choice(CITIES),
                "formed": random.randint(1970, 2020),
                "members": random.sample(INSTRUMENTS, k=random.randint(2, 4)),
            }
            artist_lsi = f"PROFILE#{item['name']}"
            add_index_keys(item, "Artist", item["PK"], item["SK"], artist_lsi)
            b.put_item(Item=item)
            for _ in range(albums_per_artist):
                album_id = rand_id("ALBUM")
                album_ids.append(album_id)
                item = {
                    "PK": artist_id,
                    "SK": album_id,
                    "type": "Album",
                    "album_id": album_id,
                    "title": rand_title(),
                    "year": random.randint(1995, 2024),
                    "genre": random.choice(GENRES),
                }
                album_lsi = f"ALBUM#{item['year']:04d}#{album_id}"
                add_index_keys(item, "Album", item["PK"], item["SK"], album_lsi)
                b.put_item(Item=item)
                for t in range(tracks_per_album):
                    track_id = rand_id("TRACK")
                    track_ids.append(track_id)
                    item = {
                        "PK": album_id,
                        "SK": track_id,
                        "type": "Track",
                        "track_id": track_id,
                        "name": rand_track_name(),
                        "milliseconds": random.randint(90_000, 360_000),
                        "bytes": random.randint(512_000, 8_000_000),
                        "unit_price": Decimal(str(round(random.uniform(0.49, 1.99), 2))),
                    }
                    track_lsi = f"TRACK#{item['name']}#{track_id}"
                    add_index_keys(item, "Track", item["PK"], item["SK"], track_lsi)
                    b.put_item(Item=item)

        # Customers / Invoices / InvoiceLines
        for _ in range(customers_ct):
            cust_id = rand_id("CUSTOMER")
            name = rand_name()
            item = {
                "PK": cust_id,
                "SK": "PROFILE",
                "type": "Customer",
                "name": name,
                "email": rand_email(name),
                "city": random.choice(CITIES),
            }
            customer_lsi = f"PROFILE#{name}"
            add_index_keys(item, "Customer", item["PK"], item["SK"], customer_lsi)
            b.put_item(Item=item)

            for inv_ix in range(invoices_per_cust):
                inv_id = rand_id("INVOICE")
                dt = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
                ts = int(dt.timestamp())
                invoice_lsi = f"INVOICE#{ts:010d}#{inv_id}"
                total = 0.0
                item = {
                    "PK": cust_id,
                    "SK": inv_id,
                    "type": "Invoice",
                    "invoice_id": inv_id,
                    "ts": ts,
                }
                add_index_keys(item, "Invoice", item["PK"], item["SK"], invoice_lsi)
                b.put_item(Item=item)
                for line_ix in range(1, lines_per_invoice + 1):
                    trk = random.choice(track_ids) if track_ids else rand_id("TRACK")
                    price = round(random.uniform(0.49, 1.99), 2)
                    qty = random.randint(1, 3)
                    total += price * qty
                    price_cents = int(round(price * 100))
                    item = {
                        "PK": inv_id,
                        "SK": f"LINE#{line_ix}",
                        "type": "InvoiceLine",
                        "track_id": trk,
                        "unit_price": Decimal(str(price)),
                        "quantity": Decimal(str(qty)),
                    }
                    line_lsi = f"PRICE#{price_cents:05d}#LINE#{line_ix:03d}"
                    add_index_keys(item, "InvoiceLine", item["PK"], item["SK"], line_lsi)
                    b.put_item(Item=item)
                # update invoice total (simple upsert)
                item = {
                    "PK": cust_id,
                    "SK": inv_id,
                    "type": "Invoice",
                    "invoice_id": inv_id,
                    "ts": ts,
                    "total": Decimal(str(round(total, 2))),
                }
                add_index_keys(item, "Invoice", item["PK"], item["SK"], invoice_lsi)
                b.put_item(Item=item)

def main():
    args = parse_args()

    # For DynamoDB Local, provide dummy creds if none are set.
    session_kwargs = {"region_name": args.region}
    resource_kwargs = {}
    if args.endpoint_url:
        resource_kwargs["endpoint_url"] = args.endpoint_url

    # If you run against Local, SDK still wants some credentials (any value works).
    # If real AWS creds are present in env/profile, boto3 will use them.
    session = boto3.Session(**session_kwargs)
    dynamo = session.client("dynamodb", **resource_kwargs)
    resource = session.resource("dynamodb", **resource_kwargs)

    try:
        existed = ensure_table(
            dynamo,
            args.table,
            recreate=args.recreate,
            recreate_if_missing_indexes=args.recreate_if_missing_indexes,
        )
    except ClientError as e:
        print(f"Failed to ensure table: {e}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except TimeoutError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    if existed and args.skip_if_exists and not args.recreate:
        print(f"Table {args.table} already exists; skipping seed.")
        return

    table = resource.Table(args.table)

    sizes = {
        "artists": args.artists,
        "albums_per_artist": args.albums_per_artist,
        "tracks_per_album": args.tracks_per_album,
        "customers": args.customers,
        "invoices_per_customer": args.invoices_per_customer,
        "lines_per_invoice": args.lines_per_invoice,
    }

    print("Seeding data…")
    seed_data(table, sizes)
    print("Done.")

if __name__ == "__main__":
    main()
