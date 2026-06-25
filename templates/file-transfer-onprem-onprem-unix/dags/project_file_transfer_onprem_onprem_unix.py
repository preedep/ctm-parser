# Apache Airflow DAG: ##COMPANY##-##PROJECT##-##APP_CODE##-##DAG_NAME##-##ENV##
# Converted by Control-M 2 Airflow Skill (Developed by NiX)
import logging
import pendulum

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator

###################### logging ######################
import smtplib
logging.getLogger("smtplib").setLevel(logging.DEBUG)
logging.getLogger("airflow.utils.email").setLevel(logging.DEBUG)
_ = smtplib
logging.getLogger("airflow.providers.ssh.operators.ssh").setLevel(logging.DEBUG)
logging.getLogger("airflow.providers.ssh.hooks.ssh").setLevel(logging.DEBUG)

###################### variables zone ######################

_company  = "##COMPANY##"
_project  = "##PROJECT##"
_app_code = "##APP_CODE##"
_env      = "##ENV##"
_dag_name = "##DAG_NAME##"

_active   = ##ACTIVE##
_schedule = "##SCHEDULE##"
_tags     = ##TAGS##
_email_list             = ##EMAIL_LIST##
_enable_email_notification_success = ##ENABLE_EMAIL_NOTIFICATION_SUCCESS##
_enable_email_notification_fail    = ##ENABLE_EMAIL_NOTIFICATION_FAIL##

# ── Agent / operator node ──────────────────────────────────────────────────────
_ssh_conn_id  = "##SSH_CONN_ID##"   # Airflow SSH connection ID to the agent/operator node
_remote_host  = "##REMOTE_HOST##"   # agent node hostname (used in email body only)

# ── Transfer parameters ────────────────────────────────────────────────────────
_protocol     = "##PROTOCOL##"      # ftp | ftps | sftp
_direction    = "##DIRECTION##"     # upload | upload_delete | upload_rename | upload_move
                                    # download | download_delete | download_rename | download_move
                                    # download_archive

# ── Source ─────────────────────────────────────────────────────────────────────
_source_path  = "##SOURCE_PATH##"   # file path or glob on the agent node (upload) or remote server (download)

# ── Destination ────────────────────────────────────────────────────────────────
_dest_host    = "##DEST_HOST##"     # destination server hostname or IP
_dest_port    = "##DEST_PORT##"     # 21 (ftp/ftps) | 22 (sftp)
_dest_user    = "##DEST_USER##"     # FTP/SFTP username (use DOMAIN\\user for Windows domain accounts)
_dest_path    = "##DEST_PATH##"     # destination path (remote dir for upload; local dir for download)
_password_var = "##PASSWORD_VAR##"  # Airflow Variable key holding destination password

# lftp needs \\ for literal \ — pre-escape domain usernames automatically
_dest_user_lftp = _dest_user.replace('\\', '\\\\')

# ── Post-transfer source handling (SRCOPT) ─────────────────────────────────────
_new_name       = "##NEW_NAME##"      # new filename (*_rename) or full path (*_move); empty = unused
_archive_path   = "##ARCHIVE_PATH##"  # base dir for date sub-folders (download_archive); empty = dest_path/archive
_retention_days = "##RETENTION_DAYS##"  # days to keep archives (cleanup_only direction); e.g. "30"

# ── Pre / Post commands ────────────────────────────────────────────────────────
# Set to empty string to skip; chmod gets special glob-safe handling.
_pre_command       = "##PRE_COMMAND##"        # shell command to run before transfer
_pre_command_args  = "##PRE_COMMAND_ARGS##"   # space-separated args (e.g. "644 /path/*.*")
_post_command      = "##POST_COMMAND##"       # shell command to run after transfer
_post_command_args = "##POST_COMMAND_ARGS##"  # space-separated args

###################### callbacks ######################

def success_callback(context):
    dag_id         = context['dag'].dag_id
    task_id        = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')

    subject = f"DAG {dag_id} - Task {task_id} succeeded"
    body = f"""
    <h3>File Transfer Task Succeeded</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Agent Host:</strong> {_remote_host}</p>
    <p><strong>Source Path:</strong> {_source_path}</p>
    <p><strong>Destination:</strong> {_dest_user}@{_dest_host}:{_dest_path}</p>
    """

    if _enable_email_notification_success:
        from airflow.utils.email import send_email
        send_email(to=_email_list, subject=subject, html_content=body)


def failure_callback(context):
    dag_id         = context['dag'].dag_id
    task_id        = context['task_instance'].task_id
    execution_date = pendulum.now('Asia/Bangkok')
    exception      = context.get('exception', 'Unknown error')

    subject = f"DAG {dag_id} - Task {task_id} failed"
    body = f"""
    <h3>File Transfer Task Failed</h3>
    <p><strong>DAG:</strong> {dag_id}</p>
    <p><strong>Task:</strong> {task_id}</p>
    <p><strong>Execution Time:</strong> {execution_date}</p>
    <p><strong>Agent Host:</strong> {_remote_host}</p>
    <p><strong>Source Path:</strong> {_source_path}</p>
    <p><strong>Destination:</strong> {_dest_user}@{_dest_host}:{_dest_path}</p>
    <p><strong>Error:</strong> {exception}</p>
    """

    if _enable_email_notification_fail:
        from airflow.utils.email import send_email
        send_email(to=_email_list, subject=subject, html_content=body)

###################### DAG configuration ######################

local_tz = pendulum.timezone('Asia/Bangkok')

default_args = {
    'owner': _company,
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz=local_tz),
    'timezone': 'Asia/Bangkok',
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': pendulum.duration(minutes=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    _company + '-' + _project + '-' + _app_code + '-' + _dag_name + '-' + _env,
    default_args=default_args,
    schedule=_schedule,
    tags=_tags,
    catchup=False,
    is_paused_upon_creation=not _active,
    start_date=pendulum.datetime(2025, 1, 1, tz=local_tz),
    on_success_callback=success_callback if _enable_email_notification_success else None,
    on_failure_callback=failure_callback if _enable_email_notification_fail else None,
    doc_md=f"""
# File Transfer: On-Premise to On-Premise (Unix)

Transfers files between on-premise Unix/Linux servers using **lftp** via SSHOperator.

| Field | Value |
|---|---|
| SSH Connection | `{_ssh_conn_id}` |
| Agent Host | `{_remote_host}` |
| Protocol | `{_protocol}` |
| Direction | `{_direction}` |
| Source Path | `{_source_path}` |
| Destination | `{_dest_user}@{_dest_host}:{_dest_path}` |
| Schedule | `{_schedule}` |
| Environment | `{_env}` |

## Transfer pattern
```
Airflow → SSHOperator → agent node ({_remote_host}) → lftp → destination ({_dest_host})
```

## Direction reference
- `upload` / `upload_delete` / `upload_rename` / `upload_move` — agent → remote
- `download` / `download_delete` / `download_rename` / `download_move` — remote → agent
- `download_archive` — download then archive locally by date

## Notes
- lftp must be installed on the agent node
- Password retrieved at runtime from Airflow Variable `{_password_var}`
- FTPS uses explicit TLS (AUTH TLS); port 21 or 991
- MVS dataset names (no leading `/`) are single-quoted automatically
    """,
)

###################### tasks ######################

with dag:

    start = EmptyOperator(task_id='start')
    end   = EmptyOperator(task_id='end')

    # ── validate_params ────────────────────────────────────────────────────────
    def _validate_params(**context):
        log = logging.getLogger(__name__)
        VALID_PROTOCOLS  = {'ftp', 'ftps', 'sftp'}
        VALID_DIRECTIONS = {
            'upload', 'upload_delete', 'upload_rename', 'upload_move',
            'download', 'download_delete', 'download_rename', 'download_move',
            'download_archive',
        }
        errors = []
        if _protocol not in VALID_PROTOCOLS:
            errors.append(f"protocol '{_protocol}' invalid — must be one of {sorted(VALID_PROTOCOLS)}")
        if _direction not in VALID_DIRECTIONS:
            errors.append(f"direction '{_direction}' invalid — must be one of {sorted(VALID_DIRECTIONS)}")
        if not _source_path.strip():
            errors.append("source_path is empty")
        if not _dest_host.strip():
            errors.append("dest_host is empty")
        if not _dest_path.strip():
            errors.append("dest_path is empty")
        if _direction in ('upload_rename', 'upload_move', 'download_rename', 'download_move') and not _new_name.strip():
            errors.append(f"new_name is required for direction '{_direction}'")
        if errors:
            raise ValueError("[PARAM ERROR]\n" + "\n".join(f"  - {e}" for e in errors))
        log.info("[INFO] validate_params: protocol=%s direction=%s — all checks passed", _protocol, _direction)

    task_validate_params = PythonOperator(
        task_id='validate_params',
        python_callable=_validate_params,
        on_failure_callback=failure_callback,
    )

    # ── pre_command ────────────────────────────────────────────────────────────
    # Self-skips when _pre_command is empty string.
    # chmod gets special handling: skips gracefully when no files match the glob.
    task_precomm = SSHOperator(
        task_id='pre_command',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] pre_command failed at line $LINENO — exit $?"' ERR
CMD="{_pre_command}"
if [ -z "$CMD" ]; then
    echo "[INFO] pre_command: skipped (empty)"
    exit 0
fi
ARGS="{_pre_command_args}"
FULL_CMD="$CMD${{ARGS:+ $ARGS}}"
echo "[INFO] pre_command: $FULL_CMD"
if [ "$CMD" = "chmod" ] && [ -n "$ARGS" ]; then
    GLOB_PATH=$(echo "$ARGS" | awk '{{print $NF}}')
    MATCH_COUNT=$(ls -1 $GLOB_PATH 2>/dev/null | wc -l | tr -d ' ') || MATCH_COUNT=0
    if [ "$MATCH_COUNT" -eq 0 ]; then
        echo "[WARN] pre_command: chmod skipped — no files matched: $GLOB_PATH"
        exit 0
    fi
    echo "[INFO] pre_command: chmod target — $MATCH_COUNT file(s) matched"
fi
bash -c "$FULL_CMD"
BASH
""",
        cmd_timeout=300,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    # ── validate_source_files ──────────────────────────────────────────────────
    # Direction-aware:
    #   upload   → ls the local source path on the agent node
    #   download → connect via lftp to ls the remote source path (catches auth errors early)
    task_validate_source = SSHOperator(
        task_id='validate_source_files',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] validate_source failed at line $LINENO — exit $?"' ERR

DIRECTION="{_direction}"
SRC_PATH="{_source_path}"
PROTOCOL="{_protocol}"
DEST_HOST="{_dest_host}"
DEST_PORT="{_dest_port}"
DEST_USER="{_dest_user_lftp}"
REMOTE_PASS=$(airflow variables get "{_password_var}" 2>/dev/null || echo "")

echo "[INFO] === Source File Validation: $DIRECTION ==="
echo "[DEBUG] Agent host : $(hostname)"
echo "[DEBUG] Agent user : $(whoami)"

case "$DIRECTION" in
    upload|upload_delete|upload_rename|upload_move)
        # Source is local on the agent node — ls the path directly
        echo "[INFO] Mode    : upload — checking local source path"
        echo "[INFO] Path    : $SRC_PATH"
        if ls $SRC_PATH 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls $SRC_PATH 2>/dev/null | wc -l | tr -d ' ')
            TOTAL_SIZE=$(du -sh $SRC_PATH 2>/dev/null | awk '{{print $1}}' || echo 'n/a')
            ls -lh $SRC_PATH | awk '{{print "[DEBUG]   " $0}}'
            echo "[INFO] Validation passed — $FILE_COUNT local file(s), size: $TOTAL_SIZE"
        else
            echo "[ERROR] No source files found locally: $SRC_PATH"
            exit 1
        fi
        ;;
    download|download_delete|download_rename|download_move|download_archive)
        # Source is on the remote server — connect via lftp to verify existence
        echo "[INFO] Mode    : download — checking remote source path via lftp"
        echo "[INFO] Remote  : $DEST_USER@$DEST_HOST:$DEST_PORT$SRC_PATH"
        PROTO_SCHEME="ftp"
        [ "$PROTOCOL" = "sftp" ] && PROTO_SCHEME="sftp"

        LFTP_RC=$(mktemp /tmp/lftprc_val.XXXXXX)
        case "$PROTOCOL" in
            ftps)
                cat > "$LFTP_RC" << 'LFTPRC'
set ssl:verify-certificate false
set ssl:ca-file ""
set ftp:ssl-force true
set ftp:ssl-auth TLS
set ftp:ssl-protect-data true
set ftp:ssl-protect-list true
set ftp:passive-mode yes
set cmd:verbose true
LFTPRC
                ;;
            ftp)
                cat > "$LFTP_RC" << 'LFTPRC'
set ftp:passive-mode yes
set cmd:verbose true
LFTPRC
                ;;
            sftp)
                cat > "$LFTP_RC" << 'LFTPRC'
set sftp:auto-confirm true
set cmd:verbose true
LFTPRC
                ;;
        esac

        set +e
        CHECK_OUT=$(lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS $PROTO_SCHEME://$DEST_HOST:$DEST_PORT; \
                ls $SRC_PATH; \
                bye" 2>&1)
        LFTP_RC_EXIT=$?
        set -e
        rm -f "$LFTP_RC"

        echo "$CHECK_OUT"
        if echo "$CHECK_OUT" | grep -qiE "Login failed|Access denied|Connection refused|Auth failed"; then
            echo "[ERROR] Connection or authentication failed to $DEST_HOST"
            exit 1
        elif [ "$LFTP_RC_EXIT" -ne 0 ]; then
            echo "[ERROR] lftp validation failed (exit $LFTP_RC_EXIT)"
            exit 1
        fi
        FILE_COUNT=$(echo "$CHECK_OUT" | grep -cv '^[[:space:]]*$' || true)
        echo "[INFO] Validation passed — $FILE_COUNT remote file(s) found at $SRC_PATH"
        ;;
esac
BASH
""",
        cmd_timeout=300,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    # ── transfer_files ─────────────────────────────────────────────────────────
    # Full protocol × direction dispatch via lftp rc file pattern.
    # Password retrieved from Airflow Variable at runtime — never hardcoded.
    task_transfer_files = SSHOperator(
        task_id='transfer_files',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] transfer_files failed at line $LINENO — exit $?"' ERR

PROTOCOL="{_protocol}"
DIRECTION="{_direction}"
SRC_PATH="{_source_path}"
DEST_HOST="{_dest_host}"
DEST_PORT="{_dest_port}"
DEST_USER="{_dest_user_lftp}"
DEST_PATH="{_dest_path}"
REMOTE_PASS=$(airflow variables get "{_password_var}" 2>/dev/null || echo "")

mvs_quote() {{
    local p="$1"
    [[ "$p" != /* && "$p" != \\* ]] && echo "'$p'" || echo "$p"
}}

echo "[INFO] === File Transfer ==="
echo "[INFO] Protocol    : $PROTOCOL"
echo "[INFO] Direction   : $DIRECTION"
echo "[INFO] Source path : $SRC_PATH"
echo "[INFO] Dest host   : $DEST_HOST:$DEST_PORT"
echo "[INFO] Dest user   : $DEST_USER (password suppressed)"
echo "[INFO] Dest path   : $DEST_PATH"
echo "[DEBUG] Agent host : $(hostname)"
echo "[DEBUG] lftp ver   : $(lftp --version 2>&1 | head -1)"
echo "[DEBUG] DNS        : $(getent hosts $DEST_HOST 2>/dev/null | awk '{{print $1}}' || echo 'n/a')"
echo "[DEBUG] Port check : $(bash -c 'echo > /dev/tcp/$DEST_HOST/$DEST_PORT' 2>/dev/null && echo reachable || echo unreachable)"

LFTP_RC=$(mktemp /tmp/lftprc.XXXXXX)

case "$PROTOCOL" in
    ftps)
        cat > "$LFTP_RC" << 'LFTPRC'
set ssl:verify-certificate false
set ssl:ca-file ""
set ftp:ssl-force true
set ftp:ssl-auth TLS
set ftp:ssl-protect-data true
set ftp:ssl-protect-list true
set ftp:passive-mode yes
set cmd:verbose false
set xfer:log true
set xfer:clobber true
LFTPRC
        ;;
    ftp)
        cat > "$LFTP_RC" << 'LFTPRC'
set ftp:passive-mode yes
set cmd:verbose false
set xfer:log true
set xfer:clobber true
LFTPRC
        ;;
    sftp)
        cat > "$LFTP_RC" << 'LFTPRC'
set sftp:auto-confirm true
set cmd:verbose false
set xfer:log true
set xfer:clobber true
LFTPRC
        ;;
    *)
        echo "[ERROR] Unsupported protocol: $PROTOCOL"
        exit 1
        ;;
esac

PROTO_SCHEME="ftp"
[ "$PROTOCOL" = "sftp" ] && PROTO_SCHEME="sftp"

case "$DIRECTION" in
    upload|upload_delete|upload_rename|upload_move)
        RPATH=$(mvs_quote "$DEST_PATH")
        echo "[INFO] Action : put $SRC_PATH → $DEST_HOST:$RPATH"
        lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS $PROTO_SCHEME://$DEST_HOST:$DEST_PORT; \
                put -a \"$SRC_PATH\" -o \"$RPATH\"; \
                bye"
        echo "[INFO] Upload completed"
        ;;
    download|download_delete|download_rename|download_move|download_archive)
        echo "[INFO] Action : mget $DEST_HOST:$SRC_PATH → $DEST_PATH"
        mkdir -p "$DEST_PATH"
        BEFORE=$(ls "$DEST_PATH" 2>/dev/null | wc -l | tr -d ' ')
        lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS $PROTO_SCHEME://$DEST_HOST:$DEST_PORT; \
                lcd $DEST_PATH; \
                mget $SRC_PATH; \
                bye"
        AFTER=$(ls "$DEST_PATH" 2>/dev/null | wc -l | tr -d ' ')
        echo "[INFO] Files downloaded: $(( AFTER - BEFORE ))"
        ls -lh "$DEST_PATH" 2>/dev/null | grep -v '^total' | awk '{{print "[DEBUG]   " $0}}' || true
        ;;
    *)
        echo "[ERROR] Unknown direction: $DIRECTION"
        rm -f "$LFTP_RC"; exit 1
        ;;
esac

rm -f "$LFTP_RC"
echo "[INFO] Transfer completed successfully"
BASH
""",
        cmd_timeout=3600,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    # ── verify_transfer ────────────────────────────────────────────────────────
    # Glob-aware: uses ls + wc -l rather than single-file [ -f ] check.
    task_verify_transfer = SSHOperator(
        task_id='verify_transfer',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] verify_transfer failed at line $LINENO — exit $?"' ERR

DIRECTION="{_direction}"
SRC_PATH="{_source_path}"
DEST_PATH="{_dest_path}"

echo "[INFO] === Verify Transfer: $DIRECTION ==="

case "$DIRECTION" in
    upload|upload_delete|upload_rename|upload_move)
        # After upload verify source still exists on agent node
        if ls $SRC_PATH 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls $SRC_PATH 2>/dev/null | wc -l | tr -d ' ')
            ls -lh $SRC_PATH | awk '{{print "[DEBUG]   " $0}}'
            echo "[INFO] Verification passed — $FILE_COUNT source file(s) present on agent"
        else
            echo "[ERROR] Source file(s) not found after upload: $SRC_PATH"
            exit 1
        fi
        ;;
    download|download_delete|download_rename|download_move|download_archive)
        SRC_PATTERN=$(basename "$SRC_PATH")
        if ls "$DEST_PATH"/$SRC_PATTERN 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls "$DEST_PATH"/$SRC_PATTERN 2>/dev/null | wc -l | tr -d ' ')
            TOTAL_SIZE=$(du -sh "$DEST_PATH"/$SRC_PATTERN 2>/dev/null | awk '{{print $1}}' || echo 'n/a')
            ls -lh "$DEST_PATH"/$SRC_PATTERN | awk '{{print "[DEBUG]   " $0}}'
            echo "[INFO] Verification passed — $FILE_COUNT file(s) at $DEST_PATH, size: $TOTAL_SIZE"
        else
            echo "[ERROR] No files matching $SRC_PATTERN found at $DEST_PATH"
            exit 1
        fi
        ;;
esac
BASH
""",
        cmd_timeout=300,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    # ── post_transfer_action ───────────────────────────────────────────────────
    # Handles source cleanup / rename / move / archive by direction (SRCOPT mapping).
    # upload      : SRCOPT=0 keep | SRCOPT=1 delete | SRCOPT=2 rename | SRCOPT=3 move
    # download    : SRCOPT=0 keep | SRCOPT=1 delete remote | SRCOPT=2 rename remote | SRCOPT=3 move remote
    # download_archive : move downloaded files to _archive_path/YYYYMMDD/
    task_post_transfer = SSHOperator(
        task_id='post_transfer_action',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] post_transfer_action failed at line $LINENO — exit $?"' ERR

DIRECTION="{_direction}"
SRC_PATH="{_source_path}"
DEST_PATH="{_dest_path}"
DEST_HOST="{_dest_host}"
DEST_PORT="{_dest_port}"
DEST_USER="{_dest_user_lftp}"
NEW_NAME="{_new_name}"
ARCHIVE_BASE="{_archive_path}"
RETENTION_DAYS="{_retention_days}"
REMOTE_PASS=$(airflow variables get "{_password_var}" 2>/dev/null || echo "")
DATE="{{ ds_nodash }}"

[ -z "$ARCHIVE_BASE" ] && ARCHIVE_BASE="$DEST_PATH/archive"
ARCHIVE_DIR="$ARCHIVE_BASE/$DATE"

echo "[INFO] === Post-Transfer Action: $DIRECTION ==="

LFTP_RC=$(mktemp /tmp/lftprc_post.XXXXXX)
cat > "$LFTP_RC" << 'LFTPRC'
set ssl:verify-certificate false
set ssl:ca-file ""
set ftp:ssl-force true
set ftp:ssl-auth TLS
set ftp:ssl-protect-data true
set ftp:passive-mode yes
set cmd:verbose false
LFTPRC

mvs_quote() {{
    local p="$1"
    [[ "$p" != /* && "$p" != \\* ]] && echo "'$p'" || echo "$p"
}}

case "$DIRECTION" in

    upload)
        echo "[INFO] Action: none — local source kept (SRCOPT=0)"
        ;;

    upload_delete)
        FILE_COUNT=$(ls $SRC_PATH 2>/dev/null | wc -l | tr -d ' ')
        rm -f $SRC_PATH
        echo "[INFO] Action: local source deleted — $FILE_COUNT file(s) removed (SRCOPT=1)"
        ;;

    upload_rename)
        [ -z "$NEW_NAME" ] && {{ echo "[ERROR] new_name required for upload_rename"; rm -f "$LFTP_RC"; exit 1; }}
        SRC_DIR=$(dirname "$SRC_PATH")
        mv "$SRC_PATH" "$SRC_DIR/$NEW_NAME"
        echo "[INFO] Action: local source renamed → $SRC_DIR/$NEW_NAME (SRCOPT=2)"
        ;;

    upload_move)
        [ -z "$NEW_NAME" ] && {{ echo "[ERROR] new_name required for upload_move"; rm -f "$LFTP_RC"; exit 1; }}
        mkdir -p "$(dirname "$NEW_NAME")"
        mv "$SRC_PATH" "$NEW_NAME"
        echo "[INFO] Action: local source moved → $NEW_NAME (SRCOPT=3)"
        ;;

    download)
        echo "[INFO] Action: none — remote source kept (SRCOPT=0)"
        ;;

    download_delete)
        RPATH=$(mvs_quote "$SRC_PATH")
        lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS ftp://$DEST_HOST:$DEST_PORT; \
                rm \"$RPATH\"; bye"
        echo "[INFO] Action: remote source deleted — $SRC_PATH (SRCOPT=1)"
        ;;

    download_rename)
        [ -z "$NEW_NAME" ] && {{ echo "[ERROR] new_name required for download_rename"; rm -f "$LFTP_RC"; exit 1; }}
        SRC_DIR=$(dirname "$SRC_PATH")
        lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS ftp://$DEST_HOST:$DEST_PORT; \
                mv \"$(mvs_quote "$SRC_PATH")\" \"$(mvs_quote "$SRC_DIR/$NEW_NAME")\"; bye"
        echo "[INFO] Action: remote source renamed → $SRC_DIR/$NEW_NAME (SRCOPT=2)"
        ;;

    download_move)
        [ -z "$NEW_NAME" ] && {{ echo "[ERROR] new_name required for download_move"; rm -f "$LFTP_RC"; exit 1; }}
        lftp -f "$LFTP_RC" \
            -e "open -u $DEST_USER,$REMOTE_PASS ftp://$DEST_HOST:$DEST_PORT; \
                mv \"$(mvs_quote "$SRC_PATH")\" \"$(mvs_quote "$NEW_NAME")\"; bye"
        echo "[INFO] Action: remote source moved → $NEW_NAME (SRCOPT=3)"
        ;;

    download_archive)
        SRC_PATTERN=$(basename "$SRC_PATH")
        mkdir -p "$ARCHIVE_DIR"
        if ls "$DEST_PATH"/$SRC_PATTERN 1>/dev/null 2>&1; then
            FILE_COUNT=$(ls "$DEST_PATH"/$SRC_PATTERN 2>/dev/null | wc -l | tr -d ' ')
            mv "$DEST_PATH"/$SRC_PATTERN "$ARCHIVE_DIR/"
            echo "[INFO] Action: archived $FILE_COUNT file(s) → $ARCHIVE_DIR"
        else
            echo "[INFO] Action: no files matching $SRC_PATTERN to archive"
        fi

        # Cleanup archives older than retention period
        if [ -n "$RETENTION_DAYS" ] && [ "$RETENTION_DAYS" -gt 0 ] 2>/dev/null; then
            if [ -d "$ARCHIVE_BASE" ]; then
                REMOVED=$(find "$ARCHIVE_BASE" -type f -mtime +"$RETENTION_DAYS" | wc -l | tr -d ' ')
                find "$ARCHIVE_BASE" -type f -mtime +"$RETENTION_DAYS" -delete
                find "$ARCHIVE_BASE" -mindepth 1 -type d -empty -delete 2>/dev/null || true
                echo "[INFO] Cleanup: $REMOVED archive file(s) older than $RETENTION_DAYS days removed"
            fi
        fi
        ;;

    *)
        echo "[ERROR] Unknown direction: $DIRECTION"
        rm -f "$LFTP_RC"; exit 1
        ;;
esac

rm -f "$LFTP_RC"
echo "[INFO] Post-transfer action completed"
BASH
""",
        cmd_timeout=600,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    # ── post_command ───────────────────────────────────────────────────────────
    # Self-skips when _post_command is empty string.
    # chmod gets special handling: skips gracefully when no files match the glob.
    task_postcomm = SSHOperator(
        task_id='post_command',
        ssh_conn_id=_ssh_conn_id,
        command=f"""
bash -s << 'BASH'
set -euo pipefail
trap 'echo "[ERROR] post_command failed at line $LINENO — exit $?"' ERR
CMD="{_post_command}"
if [ -z "$CMD" ]; then
    echo "[INFO] post_command: skipped (empty)"
    exit 0
fi
ARGS="{_post_command_args}"
FULL_CMD="$CMD${{ARGS:+ $ARGS}}"
echo "[INFO] post_command: $FULL_CMD"
if [ "$CMD" = "chmod" ] && [ -n "$ARGS" ]; then
    GLOB_PATH=$(echo "$ARGS" | awk '{{print $NF}}')
    MATCH_COUNT=$(ls -1 $GLOB_PATH 2>/dev/null | wc -l | tr -d ' ') || MATCH_COUNT=0
    if [ "$MATCH_COUNT" -eq 0 ]; then
        echo "[WARN] post_command: chmod skipped — no files matched: $GLOB_PATH"
        exit 0
    fi
    echo "[INFO] post_command: chmod target — $MATCH_COUNT file(s) matched"
fi
bash -c "$FULL_CMD"
BASH
""",
        cmd_timeout=300,
        conn_timeout=60,
        on_failure_callback=failure_callback,
    )

    ###################### task dependencies ######################

    start >> task_validate_params >> task_precomm >> task_validate_source >> task_transfer_files >> task_verify_transfer >> task_post_transfer >> task_postcomm >> end
