#!/usr/bin/env python3
import sqlite3
import configparser
import smtplib
import json
import os
from functools import wraps
from email.message import EmailMessage
from flask import Flask, request, jsonify, render_template
from datetime import datetime, time as dtime, timedelta
import time
from collections import defaultdict
from cache_utils import cache_with_timeout

app = Flask(__name__, template_folder='templates', static_folder='static')
DATABASE = os.getenv('MONITOR_DB', '/opt/proxmox-monitor/backups.db')
CONFIG_FILE = os.getenv('MONITOR_CFG', '/opt/proxmox-monitor/config.ini')
RETENTION_LIMIT = 30 

API_TOKEN = os.getenv('MONITOR_API_TOKEN', '').strip()

def _extract_token_from_request():
    # 1) Authorization: Bearer <token>
    auth = (request.headers.get('Authorization') or '').strip()
    if auth.lower().startswith('bearer '):
        return auth.split(None, 1)[1].strip()
    # 2) X-API-Key: <token>
    return (request.headers.get('X-API-Key') or '').strip()

def require_api_token(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if API_TOKEN:  # só valida se foi configurado
            token = _extract_token_from_request()
            if token != API_TOKEN:
                return jsonify({"error": "unauthorized"}), 401
        return func(*args, **kwargs)
    return wrapper

# --- Anti-cache em todas as respostas ---
@app.after_request
def add_header(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response

def get_db():
    db = sqlite3.connect(DATABASE, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')  # Write-Ahead Logging para melhor concorrência
    db.execute('PRAGMA synchronous=NORMAL')  # Compromisso entre segurança e performance
    return db

def _table_columns(cursor, table):
    rows = cursor.execute(f"PRAGMA table_info({table})").fetchall()
    return { (r["name"] if isinstance(r, sqlite3.Row) else r[1]) for r in rows }

def _ensure_column(cursor, table, col_name, col_def):
    cols = _table_columns(cursor, table)
    if col_name not in cols:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def};")

def init_db():
    with app.app_context():
        db = get_db()
        c = db.cursor()

        # -------- BACKUPS --------
        c.execute('''
            CREATE TABLE IF NOT EXISTS backups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxmox_host TEXT NOT NULL,
                company_name TEXT,
                vmid TEXT,
                vm_name TEXT,
                status TEXT NOT NULL,
                storage_target TEXT,
                start_time INTEGER NOT NULL,
                end_time INTEGER NOT NULL,
                total_size_bytes INTEGER,
                written_size_bytes INTEGER,
                duration_seconds INTEGER,
                speed_mb_s REAL
            );
        ''')
        
        c.execute('CREATE INDEX IF NOT EXISTS idx_backups_company ON backups(company_name);')
        c.execute('CREATE INDEX IF NOT EXISTS idx_backups_start_time ON backups(start_time);')
        c.execute('CREATE INDEX IF NOT EXISTS idx_backups_company_time ON backups(company_name, start_time);')

        _ensure_column(c, "backups", "vmid", "TEXT")
        _ensure_column(c, "backups", "vm_name", "TEXT")

        try:
            c.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS ux_backups_unique
                ON backups(proxmox_host, vmid, start_time, end_time);
            ''')
        except sqlite3.OperationalError as e:
            print(f"[WARN] Não foi possível criar ux_backups_unique: {e}")

        # -------- HEALTH --------
        c.execute('''
            CREATE TABLE IF NOT EXISTS health (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxmox_host TEXT NOT NULL,
                company_name  TEXT,
                payload_json  TEXT NOT NULL,
                received_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # -------- REPLICATION --------
        c.execute('''
            CREATE TABLE IF NOT EXISTS replication (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxmox_host   TEXT NOT NULL,
                company_name   TEXT,
                vmid           TEXT,
                vm_name        TEXT,
                source_node    TEXT,
                target_node    TEXT,
                state          TEXT,
                status         TEXT,
                schedule       TEXT,
                last_sync      INTEGER,
                duration_sec   INTEGER,
                fail_count     INTEGER,
                received_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')
        _ensure_column(c, "replication", "vmid", "TEXT")
        _ensure_column(c, "replication", "vm_name", "TEXT")
        _ensure_column(c, "replication", "source_node", "TEXT")
        _ensure_column(c, "replication", "target_node", "TEXT")
        _ensure_column(c, "replication", "state", "TEXT")
        _ensure_column(c, "replication", "status", "TEXT")
        _ensure_column(c, "replication", "schedule", "TEXT")
        _ensure_column(c, "replication", "last_sync", "INTEGER")
        _ensure_column(c, "replication", "duration_sec", "INTEGER")
        _ensure_column(c, "replication", "fail_count", "INTEGER")

        try:
            c.execute('''
                CREATE UNIQUE INDEX IF NOT EXISTS ux_replication_unique
                ON replication(proxmox_host, vmid, source_node, target_node, last_sync);
            ''')
        except sqlite3.OperationalError as e:
            print(f"[WARN] Não foi possível criar ux_replication_unique: {e}")

        db.commit()

def prune_old_backups(db, company_name):
    if not company_name:
        return
    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        default_retention = 30
        retention_rules = {}
        if 'retention' in config:
            default_retention = config['retention'].getint('default', 30)
            retention_rules = {k.lower(): v for k, v in config.items('retention')}

        cursor = db.cursor()
        cursor.execute("SELECT DISTINCT storage_target FROM backups WHERE company_name = ?", (company_name,))
        targets = [row['storage_target'] for row in cursor.fetchall() if row['storage_target']]
        for target in targets:
            limit = int(retention_rules.get(target.lower(), str(default_retention)))
            sql_prune = """
                DELETE FROM backups
                WHERE id IN (
                    SELECT id FROM backups
                    WHERE company_name = ? AND storage_target = ?
                    ORDER BY end_time DESC
                    LIMIT -1 OFFSET ?
                )
            """
            cursor.execute(sql_prune, (company_name, target, limit))
        db.commit()
    except Exception:
        db.rollback()

def send_alert_email(subject, body):
    try:
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        if 'email' not in config:
            return
        email_config = config['email']
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = email_config['sender_email']
        msg['To'] = email_config['recipient_email']
        server = smtplib.SMTP(email_config['smtp_server'], int(email_config['smtp_port']))
        server.starttls()
        server.login(email_config['sender_email'], email_config['sender_password'])
        server.send_message(msg)
        server.quit()
    except Exception as e:
        print(f"Failed to send email alert: {e}")

# ----------------------- BACKUP API -----------------------
@app.route('/api/v2/summaries', methods=['GET'])
@cache_with_timeout(30)
def list_companies_v2():
    return get_summaries_v2()
def get_summaries_v2():
    try:
        # Parâmetros de paginação
        page = max(1, request.args.get('page', 1, type=int))
        per_page = min(100, max(10, request.args.get('per_page', 50, type=int)))
        offset = (page - 1) * per_page
        
        db = get_db()
        cursor = db.cursor()
        
        # Obtém o total de registros para paginação
        cursor.execute('SELECT COUNT(DISTINCT company_name) as total FROM backups')
        total_records = cursor.fetchone()['total']
        
        # Consulta paginada
        cursor.execute('''
            WITH ranked_backups AS (
                SELECT 
                    company_name,
                    MAX(end_time) as last_backup,
                    COUNT(*) as total_backups,
                    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_backups
                FROM backups 
                GROUP BY company_name
                ORDER BY company_name
                LIMIT ? OFFSET ?
            )
            SELECT * FROM ranked_backups
        ''', (per_page, offset))
        
        companies = cursor.fetchall()
        results = []
        
        for company in companies:
            company_name = company['company_name']
            if not company_name:
                continue
                
            # Obtém últimos backups da empresa (limitado a 10)
            cursor.execute('''
                SELECT * FROM backups 
                WHERE company_name = ? 
                ORDER BY end_time DESC 
                LIMIT 10
            ''', (company_name,))
            
            recent_backups = cursor.fetchall()
            
            results.append({
                'company_name': company_name,
                'last_backup': company['last_backup'],
                'total_backups': company['total_backups'],
                'successful_backups': company['successful_backups'],
                'recent_backups': [{
                    'status': b['status'],
                    'start_time': b['start_time'],
                    'end_time': b['end_time'],
                    'total_size_bytes': b['total_size_bytes'],
                    'written_size_bytes': b['written_size_bytes'],
                    'duration_seconds': b['duration_seconds'],
                    'speed_mb_s': b['speed_mb_s']
                } for b in recent_backups]
            })
        
        return jsonify({
            'data': results,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total_records,
                'pages': (total_records + per_page - 1) // per_page
            }
        })
        
    except Exception as e:
        print(f"Error in get_summaries_v2: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/backup', methods=['POST'])
@require_api_token
def receive_backup_data():
    print("\n--- [DEBUG] /api/backup endpoint triggered ---")
    data = request.get_json(silent=True)
    print(f"[DEBUG] Raw data received: {data}")

    if not isinstance(data, dict):
        return jsonify({"error": "invalid JSON"}), 400

    db = None
    try:
        def to_int(x, default=0):
            try:
                return int(x)
            except (TypeError, ValueError):
                return default

        start_time = to_int(data.get('start_time'))
        end_time   = to_int(data.get('end_time'))
        written_size = data.get('written_size_bytes')
        total_size   = data.get('total_size_bytes')

        duration = end_time - start_time if end_time > start_time else 0
        try:
            w = float(written_size) if written_size is not None else 0.0
        except (TypeError, ValueError):
            w = 0.0
        speed_mb_s = (w / 1024.0 / 1024.0) / duration if duration > 0 else 0.0

        status   = (data.get('status') or '').upper()
        company  = data.get('company_name')
        vmid     = data.get('vmid')
        vm_name  = data.get('vm_name')
        host     = data.get('proxmox_host')
        storage  = data.get('storage_target')

        if status == 'SUCCESS' and duration <= 0:
            print(f"[DEBUG] Ignored zero-duration SUCCESS (host={host}, vmid={vmid})")
            return jsonify({"ignored": "zero-duration-success"}), 200

        db = get_db()
        cursor = db.cursor()

        print("[DEBUG] Attempting to INSERT data into the database...")
        cursor.execute(
            '''
            INSERT INTO backups
              (proxmox_host, company_name, vmid, vm_name, status, storage_target,
               start_time, end_time, total_size_bytes, written_size_bytes,
               duration_seconds, speed_mb_s)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (host, company, vmid, vm_name, status, storage,
             start_time, end_time, total_size, written_size, duration, speed_mb_s)
        )
        print("[DEBUG] INSERT executed. Committing...")
        db.commit()
        print("[DEBUG] Commit OK.")

        print("[DEBUG] Pruning old backups...")
        prune_old_backups(db, company)
        print("[DEBUG] Pruning finished.")

        if status != 'SUCCESS':
            subject = f"Alerta de Backup: {status} no cliente {company}"
            body = (
                "Um backup necessita de atenção.\n\n"
                f"- Cliente: {company}\n"
                f"- Host: {host}\n"
                f"- VM: {vm_name} ({vmid})\n"
                f"- Status: {status}"
            )
            send_alert_email(subject, body)

        print("--- [DEBUG] Returning 201. ---\n")
        return jsonify({"message": "Data received"}), 201

    except Exception as e:
        print(f"!!! [ERROR] {e} !!!\n")
        if db:
            db.rollback()
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


# ----------------------- HEALTH API -----------------------
@app.route('/api/health', methods=['POST'])
@require_api_token
def api_health():
    try:
        if not request.is_json:
            return jsonify({"error": "Content-Type must be application/json"}), 400

        data = request.get_json(silent=True, force=True) or {}
        proxmox_host = str(data.get('proxmox_host') or '').strip()
        company_name = str(data.get('company_name') or '').strip()
        if not proxmox_host:
            return jsonify({"error": "Missing 'proxmox_host' in payload"}), 400

        raw_pools = data.get('pools') or data.get('zfs_pools') or []
        raw_disks = data.get('disks') or data.get('smart') or []

        pools = []
        if isinstance(raw_pools, list):
            for p in raw_pools:
                if isinstance(p, dict):
                    pools.append({
                        "name":   str(p.get("name", "")),
                        "status": str(p.get("status", "")).upper()
                    })

        disks = []
        if isinstance(raw_disks, list):
            for d in raw_disks:
                if isinstance(d, dict):
                    disks.append({
                        "name":     str(d.get("name", "")),
                        "smart_ok": bool(d.get("smart_ok", True)),
                        "temp":     d.get("temp")
                    })

        canonical = {
            "proxmox_host": proxmox_host,
            "company_name": company_name,
            "pools": pools,
            "disks": disks
        }
        payload_json = json.dumps(canonical, ensure_ascii=False)

        db = get_db()
        c = db.cursor()
        c.execute("""
            INSERT INTO health (proxmox_host, company_name, payload_json)
            VALUES (?, ?, ?)
        """, (proxmox_host, company_name, payload_json))
        db.commit()

        return jsonify({"status": "ok", "id": c.lastrowid}), 201

    except Exception as e:
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500

@app.route('/health', methods=['GET'])
def health_list_page():
    db = get_db()
    rows = db.execute("""
        SELECT id, proxmox_host, company_name, received_at, payload_json
        FROM health
        ORDER BY id DESC LIMIT 100
    """).fetchall()

    html = ["<h1>Últimos health reports</h1><table border=1 cellpadding=6>"]
    html.append("<tr><th>ID</th><th>Host</th><th>Empresa</th><th>Recebido</th><th>Resumo</th></tr>")
    for r in rows:
        try:
            payload = json.loads(r["payload_json"] or "{}")
            pools = payload.get("pools") or payload.get("zfs_pools") or []
            disks = payload.get("disks") or payload.get("smart") or []
            pools_txt = ", ".join(
                f"{(p or {}).get('name','?')}:{(p or {}).get('status','?')}"
                for p in pools if isinstance(p, dict)
            )
            disks_txt = ", ".join(
                (d or {}).get('name','?')
                for d in disks if isinstance(d, dict)
            )
            resumo = f"pools=[{pools_txt}] disks=[{disks_txt}]"
        except Exception:
            resumo = "(payload não parseável)"

        html.append(
            f"<tr><td>{r['id']}</td>"
            f"<td>{r['proxmox_host']}</td>"
            f"<td>{r['company_name'] or ''}</td>"
            f"<td>{r['received_at']}</td>"
            f"<td>{resumo}</td></tr>"
        )
    html.append("</table>")
    return "\n".join(html), 200

# ----------------------- REPLICATION API -----------------------
@app.route('/api/replication', methods=['POST'])
@require_api_token
def api_replication():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    data = request.get_json(silent=True, force=True) or {}

    def to_int(x, default=0):
        try:
            return int(x)
        except (TypeError, ValueError):
            return default

    proxmox_host = str(data.get('proxmox_host') or '').strip()
    company_name = str(data.get('company_name') or '').strip()
    vmid         = str(data.get('vmid') or '').strip()
    vm_name      = str(data.get('vm_name') or '').strip()
    source_node  = str(data.get('source_node') or '').strip()
    target_node  = str(data.get('target_node') or '').strip()
    state        = str(data.get('state') or '').strip()
    status       = (data.get('status') or '').upper().strip()
    schedule     = str(data.get('schedule') or '').strip()
    last_sync    = to_int(data.get('last_sync'), 0)
    duration_sec = to_int(data.get('duration_sec'), 0)
    fail_count   = to_int(data.get('fail_count'), 0)

    if not proxmox_host:
        return jsonify({"error": "Missing 'proxmox_host'"}), 400

    db = get_db()
    c = db.cursor()
    try:
        c.execute("""
            INSERT OR IGNORE INTO replication
              (proxmox_host, company_name, vmid, vm_name, source_node, target_node,
               state, status, schedule, last_sync, duration_sec, fail_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (proxmox_host, company_name, vmid, vm_name, source_node, target_node,
              state, status, schedule, last_sync, duration_sec, fail_count))
        db.commit()
        return jsonify({"status": "ok"}), 201
    except Exception as e:
        db.rollback()
        return jsonify({"error": f"An unexpected error occurred: {e}"}), 500


# ----------------------- LIMPEZA/VIEW -----------------------
@app.route('/api/clear_logs', methods=['POST'])
@require_api_token
def clear_logs():
    data = request.get_json()
    start_date_str = data.get('start_date')
    end_date_str   = data.get('end_date')
    try:
        start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
        start_ts = int(start_dt.timestamp())
        end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
        end_dt = datetime.combine(end_dt.date(), dtime.max)
        end_ts = int(end_dt.timestamp())
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM backups WHERE end_time >= ? AND end_time <= ?", (start_ts, end_ts))
        deleted_rows = c.rowcount
        db.commit()
        message = f"{deleted_rows} registro(s) foram excluídos." if deleted_rows > 0 else "Nenhum log encontrado para excluir."
        return jsonify({"message": message}), 200
    except Exception as e:
        return jsonify({"error": f"Ocorreu un erro: {e}"}), 500

@app.route('/api/clear_all_logs', methods=['POST'])
@require_api_token
def clear_all_logs():
    try:
        db = get_db()
        c = db.cursor()
        c.execute("DELETE FROM backups")
        deleted_rows = c.rowcount
        db.commit()
        message = f"Histórico completo ({deleted_rows} registros) excluído." if deleted_rows > 0 else "O dashboard já estava limpo."
        return jsonify({"message": message}), 200
    except Exception as e:
        return jsonify({"error": f"Ocorreu um erro: {e}"}), 500

@app.route('/', methods=['GET'])
@app.route('/backups', methods=['GET'])
def view_backups():
    db = get_db()
    c = db.cursor()
    c.execute('SELECT * FROM backups ORDER BY end_time DESC')
    backups_by_company = defaultdict(list)
    all_companies = set()

    for row in c.fetchall():
        b = dict(row)
        b['start_time_str'] = datetime.fromtimestamp(b['start_time']).strftime('%Y-%m-%d %H:%M:%S') if b.get('start_time') else 'N/A'
        b['end_time_str']   = datetime.fromtimestamp(b['end_time']).strftime('%Y-%m-%d %H:%M:%S') if b.get('end_time') else 'N/A'
        b['duration_str']   = str(timedelta(seconds=b['duration_seconds'])) if b.get('duration_seconds') else '0:00:00'
        company_name = (b.get('company_name') or '').strip() or "Cliente Indefinido"
        backups_by_company[company_name].append(b)
        all_companies.add(company_name)

    sorted_companies = sorted(list(all_companies))

    # Health mais recente
    health_by_company = defaultdict(dict)
    rows = db.execute("""
        SELECT h1.*
        FROM health h1
        JOIN (
          SELECT proxmox_host, MAX(id) AS max_id
          FROM health
          GROUP BY proxmox_host
        ) x ON x.max_id = h1.id
        ORDER BY h1.company_name, h1.proxmox_host
    """).fetchall()

    for r in rows:
        comp = (r['company_name'] or '').strip() or "Cliente Indefinido"
        try:
            raw = json.loads(r['payload_json'])
        except Exception:
            raw = {}

        raw_pools = raw.get('pools') or raw.get('zfs_pools') or []
        raw_disks = raw.get('disks') or raw.get('smart') or []

        norm_pools = []
        for p in raw_pools:
            if isinstance(p, dict):
                norm_pools.append({
                    "name": str(p.get("name", "")),
                    "status": str(p.get("status", "")).upper()
                })

        norm_disks = []
        for d in raw_disks:
            if isinstance(d, dict):
                norm_disks.append({
                    "name": str(d.get("name", "")),
                    "smart_ok": bool(d.get("smart_ok", True)),
                    "temp": d.get("temp")
                })

        payload = {
            "proxmox_host": raw.get("proxmox_host"),
            "company_name": raw.get("company_name"),
            "pools": norm_pools,
            "disks": norm_disks,
            "zfs_pools": norm_pools,
            "smart": norm_disks
        }

        health_by_company[comp][r['proxmox_host']] = {
            "received_at": r['received_at'],
            "payload": payload
        }

    return render_template(
        'dashboard.html',
        backups=[],
        all_companies=sorted_companies,
        backups_by_company=backups_by_company,
        health_by_company=health_by_company
    )

def _row_to_dict(row):
    r = dict(row)
    return {
        "id": r.get("id"),
        "proxmox_host": r.get("proxmox_host"),
        "company_name": r.get("company_name"),
        "vmid": r.get("vmid"),
        "vm_name": r.get("vm_name"),
        "status": r.get("status"),
        "storage_target": r.get("storage_target"),
        "start_time": r.get("start_time"),
        "end_time": r.get("end_time"),
        "total_size_bytes": r.get("total_size_bytes"),
        "written_size_bytes": r.get("written_size_bytes"),
        "duration_seconds": r.get("duration_seconds"),
        "speed_mb_s": r.get("speed_mb_s"),
        "received_at": r.get("received_at"),
    }

@app.route("/api/companies", methods=["GET"])
def list_companies():
    try:
        limit = int(request.args.get("limit", 6))
    except ValueError:
        limit = 6
    limit = max(1, min(limit, 50))

    db = get_db()
    db.row_factory = sqlite3.Row
    cur = db.cursor()

    # 1) Empresas 
    companies = [
        ((r["company_name"] or "").strip())
        for r in cur.execute(
            "SELECT DISTINCT company_name FROM backups ORDER BY company_name ASC"
        ).fetchall()
    ]

    now = int(time.time())
    since_24h = now - 24 * 3600

    # 2) Health (snapshot) 
    health_rows = cur.execute(
        """
        SELECT h1.*
        FROM health h1
        JOIN (
          SELECT company_name, proxmox_host, MAX(id) AS max_id
          FROM health
          GROUP BY company_name, proxmox_host
        ) x ON x.max_id = h1.id
        """
    ).fetchall()

    health_by_company = {}
    for r in health_rows:
        comp_key = ((r["company_name"] or "").strip())  
        try:
            payload = json.loads(r["payload_json"]) or {}
        except Exception:
            payload = {}
        raw_pools = payload.get("pools") or payload.get("zfs_pools") or []
        norm_pools = []
        for p in raw_pools:
            if isinstance(p, dict):
                norm_pools.append({
                    "name": str(p.get("name") or p.get("pool_name") or "?"),
                    "status": str(p.get("status") or p.get("health") or "UNKNOWN").upper()
                })
        health_by_company.setdefault(comp_key, {})[r["proxmox_host"]] = {
            "received_at": r["received_at"],
            "pools": norm_pools,
        }

    # 3) Replicação 
    repl_by_company = {}
    repl_rows = cur.execute(
        """
        SELECT r1.*
        FROM replication r1
        JOIN (
          SELECT company_name, vmid, source_node, target_node, MAX(id) AS max_id
          FROM replication
          GROUP BY company_name, vmid, source_node, target_node
        ) x ON x.max_id = r1.id
        """
    ).fetchall()

    for r in repl_rows:
        comp_key = ((r["company_name"] or "").strip())
        repl_by_company.setdefault(comp_key, []).append(dict(r))

    payload = []

    for c in companies:
        # 4) Último update 
        last = cur.execute(
            "SELECT end_time FROM backups WHERE IFNULL(company_name,'')=? ORDER BY end_time DESC LIMIT 1",
            (c,),
        ).fetchone()

        last_update = int(last["end_time"]) if last and last["end_time"] is not None else None
        last_update_str = (
            datetime.fromtimestamp(last_update).strftime("%Y-%m-%d %H:%M:%S")
            if last_update else None
        )

        # 5) Recentes (limit)
        rows = cur.execute(
            """
            SELECT * FROM backups
            WHERE IFNULL(company_name,'')=?
            ORDER BY end_time DESC
            LIMIT ?
            """,
            (c, limit),
        ).fetchall()
        recent = [_row_to_dict(r) for r in rows]

        # 6) Stats 24h
        s24 = cur.execute(
            """
            SELECT
               SUM(CASE WHEN status='SUCCESS' THEN 1 ELSE 0 END) AS ok,
               SUM(CASE WHEN status!='SUCCESS' THEN 1 ELSE 0 END) AS fail,
               COUNT(*) AS total
            FROM backups
            WHERE IFNULL(company_name,'')=? AND end_time>=?
            """,
            (c, since_24h),
        ).fetchone()

        stats_24h = {
            "ok": int(s24["ok"] or 0),
            "fail": int(s24["fail"] or 0),
            "total": int(s24["total"] or 0),
        }

        # 7) Replicação (resumo + jobs)
        repl_jobs = repl_by_company.get(c, [])
        repl_ok = sum(1 for j in repl_jobs if (j.get("status") or "").upper() == "SUCCESS")
        repl_fail = sum(1 for j in repl_jobs if (j.get("status") or "").upper() != "SUCCESS")

        if repl_jobs:
            last_sync_epoch = max(int(j.get("last_sync") or 0) for j in repl_jobs)
            last_sync_str = (
                datetime.fromtimestamp(last_sync_epoch).strftime("%Y-%m-%d %H:%M:%S")
                if last_sync_epoch else None
            )
        else:
            last_sync_epoch = None
            last_sync_str = None

        repl_jobs_out = []
        for j in repl_jobs:
            ls = j.get("last_sync")
            ls_str = datetime.fromtimestamp(int(ls)).strftime("%Y-%m-%d %H:%M:%S") if ls else None
            repl_jobs_out.append({
                "vmid": j.get("vmid"),
                "vm_name": j.get("vm_name"),
                "source_node": j.get("source_node"),
                "target_node": j.get("target_node"),
                "state": j.get("state"),
                "status": j.get("status"),
                "last_sync": j.get("last_sync"),
                "last_sync_str": ls_str,
                "duration_sec": j.get("duration_sec"),
                "fail_count": j.get("fail_count"),
                "schedule": j.get("schedule"),
            })

        repl_summary = {
            "ok": repl_ok,
            "fail": repl_fail,
            "last_sync": last_sync_epoch,
            "last_sync_str": last_sync_str,
            "jobs": repl_jobs_out,
        }

        # 8) Nome para exibição 
        display_name = c if c else "Cliente Indefinido"

        payload.append({
            "company_name": display_name, 
            "company_key": c,              
            "last_update": last_update,
            "last_update_str": last_update_str,
            "stats_24h": stats_24h,
            "recent": recent,
            "health": health_by_company.get(c, {}),
            "replication": repl_summary,
        })

    return jsonify(payload), 200


@app.route("/api/company/<company>/recent", methods=["GET"])
def company_recent(company):
    page = max(1, request.args.get("page", 1, type=int))
    per_page = min(100, max(10, request.args.get("per_page", 20, type=int))) # Limite padrão de 20 por página
    offset = (page - 1) * per_page

    db = get_db()
    db.row_factory = sqlite3.Row
    
    # Obter o total de backups para o cliente para calcular o número total de páginas
    total_backups_cursor = db.execute(
        "SELECT COUNT(*) FROM backups WHERE company_name=?",
        (company,)
    )
    total_backups = total_backups_cursor.fetchone()[0]

    rows = db.execute(
        """SELECT * FROM backups
           WHERE company_name=?
           ORDER BY end_time DESC
           LIMIT ? OFFSET ?""",
        (company, per_page, offset)
    ).fetchall()
    
    backups_data = [_row_to_dict(r) for r in rows]

    return jsonify({
        "backups": backups_data,
        "pagination": {
            "total_items": total_backups,
            "per_page": per_page,
            "current_page": page,
            "total_pages": (total_backups + per_page - 1) // per_page
        }
    }), 200

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
