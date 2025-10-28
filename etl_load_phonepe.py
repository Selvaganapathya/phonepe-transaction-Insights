# -*- coding: utf-8 -*-
"""
etl_load_phonepe_mysql.py

Discover PhonePe Pulse JSONs and load into MySQL using SQLAlchemy+pymysql.

Usage:
    python etl_load_phonepe_mysql.py --repo-path .\pulse --db-url "mysql+pymysql://root:Admin%40123@localhost:3306/phonepe_db"
"""
import argparse
import json
import logging
from pathlib import Path
from git import Repo, InvalidGitRepositoryError
from sqlalchemy import create_engine, text
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("phonepe-etl-mysql")

# ---- DDL: create tables that match your required schema ----
DDLS = {
    # Aggregated
    "aggregated_transaction": """
    CREATE TABLE IF NOT EXISTS aggregated_transaction (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        transaction_type VARCHAR(200),
        transaction_count BIGINT,
        transaction_amount DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "aggregated_user": """
    CREATE TABLE IF NOT EXISTS aggregated_user (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        registered_users BIGINT,
        app_opens BIGINT,
        active_users BIGINT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "aggregated_insurance": """
    CREATE TABLE IF NOT EXISTS aggregated_insurance (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        insurance_type VARCHAR(200),
        total_policies BIGINT,
        total_premium DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # Map tables (separate per domain)
    "map_user": """
    CREATE TABLE IF NOT EXISTS map_user (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        registered_users BIGINT,
        app_opens BIGINT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "map_map": """
    CREATE TABLE IF NOT EXISTS map_map (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        total_tx_count BIGINT,
        total_tx_amount DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "map_insurance": """
    CREATE TABLE IF NOT EXISTS map_insurance (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        total_policies BIGINT,
        total_premium DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,

    # Top tables
    "top_user": """
    CREATE TABLE IF NOT EXISTS top_user (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        pin_code VARCHAR(20),
        `rank` INT,
        registered_users BIGINT,
        app_opens BIGINT
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "top_map": """
    CREATE TABLE IF NOT EXISTS top_map (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        pin_code VARCHAR(20),
        `rank` INT,
        total_tx_count BIGINT,
        total_tx_amount DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """,
    "top_insurance": """
    CREATE TABLE IF NOT EXISTS top_insurance (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        source_path TEXT,
        country VARCHAR(100),
        state VARCHAR(200),
        district VARCHAR(200),
        year INT,
        quarter INT,
        pin_code VARCHAR(20),
        `rank` INT,
        total_policies BIGINT,
        total_premium DOUBLE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
}

# ---- Helpers ----
def safe_get(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
        if cur is None:
            return None
    return cur

# ---- Parsers adjusted to produce rows matching the new tables ----

def parse_aggregated_transaction(j, country, state, district, year, quarter, source):
    rows = []
    tdata = safe_get(j, "data", "transactionData") or []
    for rec in tdata:
        ttype = rec.get("name")
        instruments = rec.get("paymentInstruments", [])
        total_count = None
        total_amount = None
        for inst in instruments:
            if inst.get("type") == "TOTAL":
                total_count = inst.get("count")
                total_amount = inst.get("amount")
                break
        if total_count is None:
            try:
                total_count = sum(int(i.get("count") or 0) for i in instruments)
            except Exception:
                total_count = None
        if total_amount is None:
            try:
                total_amount = sum(float(i.get("amount") or 0) for i in instruments)
            except Exception:
                total_amount = None
        rows.append({
            "source_path": source,
            "country": country,
            "state": state,
            "district": district,
            "year": year,
            "quarter": quarter,
            "transaction_type": ttype,
            "transaction_count": int(total_count) if total_count not in (None, "") else None,
            "transaction_amount": float(total_amount) if total_amount not in (None, "") else None
        })
    return rows

def parse_aggregated_user(j, country, state, district, year, quarter, source):
    """
    Two common shapes:
      - data contains top-level registeredUsers, appOpens, activeUsers (single summary)
      - data contains usersByDevice list (per-device). We'll sum counts to produce aggregated totals.
    """
    rows = []
    data = safe_get(j, "data") or {}
    # Preferred: top level totals
    if isinstance(data, dict) and any(k in data for k in ("registeredUsers", "appOpens", "activeUsers")):
        rows.append({
            "source_path": source,
            "country": country,
            "state": state,
            "district": district,
            "year": year,
            "quarter": quarter,
            "registered_users": int(data.get("registeredUsers") or 0),
            "app_opens": int(data.get("appOpens") or 0),
            "active_users": int(data.get("activeUsers") or 0) if data.get("activeUsers") is not None else None
        })
        return rows

    # Fallback: sum devices if present
    users_by_device = data.get("usersByDevice") if isinstance(data, dict) else None
    if isinstance(users_by_device, list) and users_by_device:
        total_registered = 0
        total_app_opens = 0
        for rec in users_by_device:
            # rec may have registeredUsers or count or registered
            total_registered += int(rec.get("registeredUsers") or rec.get("count") or 0)
            total_app_opens += int(rec.get("appOpens") or rec.get("appOpensCount") or 0)
        rows.append({
            "source_path": source,
            "country": country,
            "state": state,
            "district": district,
            "year": year,
            "quarter": quarter,
            "registered_users": total_registered,
            "app_opens": total_app_opens,
            "active_users": None
        })
    return rows


def parse_aggregated_insurance(j, country, state, district, year, quarter, source):
    rows = []
    ins_list = safe_get(j, "data", "insurance") or safe_get(j, "data", "insuranceData") or []
    if isinstance(ins_list, dict):
        ins_list = [ins_list]
    for rec in ins_list:
        rows.append({
            "source_path": source,
            "country": country,
            "state": state,
            "district": district,
            "year": year,
            "quarter": quarter,
            "insurance_type": rec.get("name") or rec.get("insuranceType"),
            "total_policies": int(rec.get("count") or rec.get("policies") or 0),
            "total_premium": float(rec.get("amount") or rec.get("premium") or 0.0)
        })
    return rows

def parse_map_json(j, kind, country, state, year, quarter, source):
    """
    Return rows appropriate for the three map tables based on 'kind':
      - kind == 'transaction' -> map_map rows (total_tx_count/amount)
      - kind == 'user' -> map_user rows (registered_users/app_opens)
      - kind == 'insurance' -> map_insurance rows (total_policies/total_premium)
    The PhonePe JSON varies a lot; handle common patterns.
    """
    rows = []
    data = safe_get(j, "data") or {}

    # Transaction map files often have hoverDataList or data -> hoverDataList
    if kind == "transaction":
        candidates = []
        if isinstance(data, dict):
            for k in ("hoverDataList", "data", "hoverData", "states"):
                if k in data and isinstance(data[k], list):
                    candidates.append(data[k])
        if not candidates and isinstance(data, list):
            candidates = [data]
        for cand in candidates:
            for item in cand:
                if not isinstance(item, dict):
                    continue
                name = item.get("name") or item.get("district") or item.get("state") or "Unknown"
                metric = item.get("metric") or item.get("values") or item.get("value") or {}
                total_count = int(metric.get("count") or metric.get("totalCount") or 0) if isinstance(metric, dict) else None
                total_amount = float(metric.get("amount") or metric.get("totalAmount") or 0.0) if isinstance(metric, dict) else None
                rows.append({
                    "source_path": source,
                    "country": country,
                    "state": state,
                    "district": name,
                    "year": year,
                    "quarter": quarter,
                    "total_tx_count": total_count,
                    "total_tx_amount": total_amount
                })
        return rows

    # User map files often have hoverData as a dict: { "DistrictName": {registeredUsers:..., appOpens:...}, ... }
    if kind == "user":
        h = data.get("hoverData") or {}
        if isinstance(h, dict):
            for name, metric in h.items():
                if not isinstance(metric, dict):
                    continue
                registered = int(metric.get("registeredUsers") or metric.get("registered") or 0)
                opens = int(metric.get("appOpens") or metric.get("appOpensCount") or 0)
                rows.append({
                    "source_path": source,
                    "country": country,
                    "state": state,
                    "district": name,
                    "year": year,
                    "quarter": quarter,
                    "registered_users": registered,
                    "app_opens": opens
                })
        return rows

    # Insurance map: similar to transactions but different metric names
    if kind == "insurance":
        # find lists or dicts
        candidates = []
        if isinstance(data, dict):
            for k in ("hoverDataList", "insurance", "data"):
                if k in data:
                    candidates.append(data[k])
        for cand in candidates:
            if isinstance(cand, list):
                for item in cand:
                    if not isinstance(item, dict):
                        continue
                    name = item.get("name") or item.get("district") or "Unknown"
                    policies = int(item.get("count") or item.get("policies") or 0)
                    premium = float(item.get("amount") or item.get("premium") or 0.0)
                    rows.append({
                        "source_path": source,
                        "country": country,
                        "state": state,
                        "district": name,
                        "year": year,
                        "quarter": quarter,
                        "total_policies": policies,
                        "total_premium": premium
                    })
            elif isinstance(cand, dict):
                for name, metric in cand.items():
                    if not isinstance(metric, dict):
                        continue
                    policies = int(metric.get("count") or metric.get("policies") or 0)
                    premium = float(metric.get("amount") or metric.get("premium") or 0.0)
                    rows.append({
                        "source_path": source,
                        "country": country,
                        "state": state,
                        "district": name,
                        "year": year,
                        "quarter": quarter,
                        "total_policies": policies,
                        "total_premium": premium
                    })
        return rows

    return rows

def parse_top_json(j, kind, country, state, year, quarter, source):
    """
    Extract ranked lists from top JSONs.
    For each item, produce rows appropriate for:
      - top_user (registered_users/app_opens)
      - top_map (total_tx_count/amount)
      - top_insurance (total_policies/total_premium)
    """
    rows = []
    data = safe_get(j, "data") or {}

    # Recursive find lists in nested dicts (same helper idea used earlier)
    def find_lists(d):
        found = []
        if isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, list):
                    found.append(v)
                elif isinstance(v, dict):
                    found.extend(find_lists(v))
        return found

    lists = find_lists(data)
    if not lists and isinstance(data, list):
        lists = [data]

    for lst in lists:
        for item in lst:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or item.get("district") or item.get("state")
            pincode = item.get("pincode") or item.get("pin")
            rank = int(item.get("rank") or item.get("position") or 0)
            if kind == "user":
                reg = int(item.get("registeredUsers") or item.get("count") or 0)
                opens = int(item.get("appOpens") or 0)
                rows.append({
                    "source_path": source,
                    "country": country,
                    "state": state,
                    "district": name,
                    "year": year,
                    "quarter": quarter,
                    "pin_code": pincode,
                    "rank": rank,
                    "registered_users": reg,
                    "app_opens": opens
                })
            elif kind == "transaction":
                tx_count = int(item.get("count") or item.get("transactions") or 0)
                tx_amount = float(item.get("amount") or item.get("value") or 0.0)
                rows.append({
                    "source_path": source,
                    "country": country,
                    "state": state,
                    "district": name,
                    "year": year,
                    "quarter": quarter,
                    "pin_code": pincode,
                    "rank": rank,
                    "total_tx_count": tx_count,
                    "total_tx_amount": tx_amount
                })
            elif kind == "insurance":
                pol = int(item.get("count") or item.get("policies") or 0)
                prem = float(item.get("amount") or item.get("premium") or 0.0)
                rows.append({
                    "source_path": source,
                    "country": country,
                    "state": state,
                    "district": name,
                    "year": year,
                    "quarter": quarter,
                    "pin_code": pincode,
                    "rank": rank,
                    "total_policies": pol,
                    "total_premium": prem
                })
    return rows

# ---- File discovery + path parsing ----
def discover_json_files(repo_path):
    root = Path(repo_path) / "data"
    results = {"aggregated": [], "map": [], "top": []}
    for cat in results.keys():
        base = root / cat
        if not base.exists():
            logger.warning("Missing folder: %s", str(base))
            continue
        for p in base.rglob("*.json"):
            results[cat].append(str(p))
    for k in results:
        results[k].sort()
    logger.info("Found json counts: aggregated=%d map=%d top=%d",
                len(results['aggregated']), len(results['map']), len(results['top']))
    return results

def parse_path_context(path_str):
    parts = Path(path_str).parts
    try:
        idx = parts.index('data')
    except ValueError:
        idx = 0
    tail = parts[idx+1:]
    country = None; state = None; district = None; year = None; quarter = None
    if len(tail) >= 4:
        if tail[0] == 'aggregated':
            if 'country' in tail:
                try:
                    ci = tail.index('country'); country = tail[ci+1]
                    if tail[-2].isdigit(): year = int(tail[-2])
                    q = Path(tail[-1]).stem
                    if q.isdigit(): quarter = int(q)
                except Exception: pass
            if 'state' in tail:
                try:
                    si = tail.index('state'); state = tail[si+1]
                    if tail[-2].isdigit(): year = int(tail[-2])
                    q = Path(tail[-1]).stem
                    if q.isdigit(): quarter = int(q)
                except Exception: pass
        else:
            if 'country' in tail and 'india' in tail:
                try:
                    if tail[-2].isdigit(): year = int(tail[-2])
                    q = Path(tail[-1]).stem
                    if q.isdigit(): quarter = int(q)
                except Exception: pass
            if 'state-wise' in tail:
                try:
                    si = tail.index('state-wise'); state = tail[si+1]
                except Exception: pass
    if isinstance(state, str): state = state.replace('%20', ' ')
    return country, state, district, year, quarter

# ---- DB helpers ----
def ensure_tables(engine):
    with engine.begin() as conn:
        for name, ddl in DDLS.items():
            logger.info("Creating table: %s", name)
            conn.execute(text(ddl))

def bulk_insert(df, engine, table):
    if df.empty:
        return 0
    try:
        # to_sql will create parameterized inserts; method='multi' for batch
        df.to_sql(table, engine, if_exists='append', index=False, method='multi', chunksize=1000)
        return len(df)
    except Exception as e:
        logger.exception("bulk insert failed for %s: %s", table, e)
        raise

# ---- Main processing: route rows to correct tables ----
def process_and_load(repo_path, engine):
    files = discover_json_files(repo_path)
    total = 0

    # aggregated
    for p in tqdm(files.get('aggregated', []), desc='aggregated'):
        try:
            with open(p, 'r', encoding='utf-8') as fh:
                j = json.load(fh)
        except Exception:
            with open(p, 'r', encoding='latin-1') as fh:
                j = json.load(fh)
        country, state, district, year, quarter = parse_path_context(p)
        pp = p.replace('\\','/')

        if '/aggregated/transaction/' in pp:
            rows = parse_aggregated_transaction(j, country or 'india', state, district, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'aggregated_transaction')

        elif '/aggregated/user/' in pp:
            rows = parse_aggregated_user(j, country or 'india', state, district, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'aggregated_user')

        elif '/aggregated/insurance/' in pp:
            rows = parse_aggregated_insurance(j, country or 'india', state, district, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'aggregated_insurance')

    # map
    for p in tqdm(files.get('map', []), desc='map'):
        try:
            with open(p, 'r', encoding='utf-8') as fh:
                j = json.load(fh)
        except Exception:
            with open(p, 'r', encoding='latin-1') as fh:
                j = json.load(fh)
        country, state, district, year, quarter = parse_path_context(p)
        pp = p.replace('\\','/')
        # determine sub-kind by path
        if '/map/transaction/' in pp:
            kind = 'transaction'
            rows = parse_map_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'map_map')

        elif '/map/user/' in pp:
            kind = 'user'
            rows = parse_map_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'map_user')

        elif '/map/insurance/' in pp:
            kind = 'insurance'
            rows = parse_map_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'map_insurance')

    # top
    for p in tqdm(files.get('top', []), desc='top'):
        try:
            with open(p, 'r', encoding='utf-8') as fh:
                j = json.load(fh)
        except Exception:
            with open(p, 'r', encoding='latin-1') as fh:
                j = json.load(fh)
        country, state, district, year, quarter = parse_path_context(p)
        pp = p.replace('\\','/')
        if '/top/transaction/' in pp:
            kind = 'transaction'
            rows = parse_top_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'top_map')

        elif '/top/user/' in pp:
            kind = 'user'
            rows = parse_top_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'top_user')

        elif '/top/insurance/' in pp:
            kind = 'insurance'
            rows = parse_top_json(j, kind, country or 'india', state, year, quarter, p)
            df = pd.DataFrame(rows)
            if not df.empty:
                total += bulk_insert(df, engine, 'top_insurance')

    logger.info("Total approx rows inserted: %d", total)
    return total

# ---- CLI / Main ----
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-path", required=True, help="path to cloned pulse repo")
    parser.add_argument("--db-url", required=True, help="SQLAlchemy DB URL e.g. mysql+pymysql://user:pass@host:3306/db")
    parser.add_argument("--clone", type=bool, default=False, help="if True, clone repo into repo-path")
    parser.add_argument("--repo-url", default="https://github.com/PhonePe/pulse.git")
    args = parser.parse_args()

    repo_path = Path(args.repo_path).resolve()
    if args.clone and not repo_path.exists():
        logger.info("Cloning %s into %s", args.repo_url, str(repo_path))
        Repo.clone_from(args.repo_url, str(repo_path))
    if not (repo_path / "data").exists():
        logger.error("Repo path invalid or missing data folder: %s", str(repo_path))
        return

    logger.info("Connecting to DB...")
    engine = create_engine(args.db_url, pool_pre_ping=True)
    # test connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    ensure_tables(engine)
    process_and_load(str(repo_path), engine)

if __name__ == "__main__":
    main()
