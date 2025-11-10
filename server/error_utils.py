import logging
import traceback
import os
import requests
import json
import time
import threading
from collections import deque
from config import CONFIG

_SLACK_RATE_LOCK = threading.Lock()
_SLACK_SEND_TIMES = deque()  # unix timestamps of successful sends


def _slack_rate_allow(now_ts: float | None = None) -> bool:
    """
    Allow at most N Slack messages per rolling hour.
    N defaults to 3 and can be overridden via env SLACK_MAX_PER_HOUR.
    Thread-safe.
    """
    max_per_hour_env = os.getenv("SLACK_MAX_PER_HOUR")
    try:
        max_per_hour = int(max_per_hour_env) if max_per_hour_env else 3
    except Exception:
        max_per_hour = 3

    now = now_ts if now_ts is not None else time.time()
    one_hour_ago = now - 3600
    with _SLACK_RATE_LOCK:
        # prune old timestamps
        while _SLACK_SEND_TIMES and _SLACK_SEND_TIMES[0] < one_hour_ago:
            _SLACK_SEND_TIMES.popleft()
        if len(_SLACK_SEND_TIMES) >= max_per_hour:
            return False
        # Reserve a slot; we record immediately to guard against bursts
        _SLACK_SEND_TIMES.append(now)
        return True


def _post_to_slack(webhook_url: str, payload: dict) -> None:
    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )
    if response.status_code != 200 or response.text != "ok":
        raise Exception(
            f"Failed to send message to Slack. "
            f"Status code: {response.status_code}, Response: {response.text}"
        )


def _send_to_slack(message: str, title: str, rate_limit: bool = True) -> None:
    """
    Core Slack send function with optional rate limiting.

    Args:
        message (str): The message to send.
        title (str): A short title for the message.
        rate_limit (bool): If True, apply hourly rate limiting; otherwise send always.
    """
    # Replace this with your actual Slack webhook URL
    webhook_url = CONFIG.slack_webhook_url

    # Format payload using Block Kit so long details are collapsible in Slack UI.
    # Keep a concise top-level text for notifications, and move details into a block
    # which Slack will truncate with “Show more” when lengthy.
    header_text = f"{title}"
    detail_text = message if isinstance(message, str) else str(message)
    # Use regular section blocks (not header) so title isn’t oversized.
    # Slack will still collapse long code blocks with “Show more”.
    payload = {
        "text": f"*{header_text}*",  # Fallback text for notifications
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*{header_text}*"},
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"```{detail_text}```"},
            },
        ],
    }

    # Rate limiting: allow only a limited number per hour if enabled
    if rate_limit and not _slack_rate_allow():
        logging.getLogger(__name__).debug("Slack rate limit reached; dropping message '%s'", title)
        return

    _post_to_slack(webhook_url, payload)


def send_error_to_slack(error_message: str, title: str) -> None:
    """
    Send an error message to Slack (rate limited).
    """
    _send_to_slack(error_message, title, rate_limit=True)


def send_info_to_slack(message: str, title: str) -> None:
    """
    Send an informational message to Slack (not rate limited).
    """
    _send_to_slack(message, title, rate_limit=False)


def log_error(title: str, err: Exception | str, context: dict | None = None, also_raise: bool = False) -> None:
    """
    Log an error locally and attempt to notify Slack.

    - title: short summary for Slack and logs
    - err: Exception or string message
    - context: optional key/values to include in the Slack payload
    - also_raise: re-raise the exception after logging (when err is Exception)
    """
    msg = str(err)
    tb = ""
    if isinstance(err, Exception):
        tb = "\n" + traceback.format_exc()
    log_line = f"{title}: {msg}{tb}"
    logging.error(log_line)
    # Prepare Slack text; include simple context if provided
    slack_text = msg
    if context:
        try:
            extra = json.dumps(context, ensure_ascii=False)
            # _send_to_slack wraps the message in a code block; no need for backticks here
            slack_text += f"\n{extra}"
        except Exception:
            pass
    # Best-effort Slack notification; never raise from here
    try:
        send_error_to_slack(slack_text, title)
    except Exception as slack_err:
        logging.debug(f"Failed to send Slack error notification: {slack_err}")
    if also_raise and isinstance(err, Exception):
        raise err


def log_analysis_error(
    title: str,
    err: Exception | str,
    analysis: object | None = None,
    sub_id: int | None = None,
    analysis_id: int | None = None,
    extra: dict | None = None,
) -> None:
    """
    Convenience wrapper to log errors related to a specific analysis/sub-analysis.
    - Attempts to populate sub_id and analysis_id from the provided analysis
      object if not explicitly given.
    - Merges optional extra context.
    - Delegates to log_error.
    """
    ctx: dict = {}
    # Populate from explicit args first
    if sub_id is not None:
        ctx["sub_id"] = sub_id
    if analysis_id is not None:
        ctx["analysis_id"] = analysis_id

    # Try to derive from analysis object if missing
    if analysis is not None:
        try:
            if "sub_id" not in ctx and hasattr(analysis, "sub_id"):
                ctx["sub_id"] = getattr(analysis, "sub_id")
            if "analysis_id" not in ctx and hasattr(analysis, "id"):
                ctx["analysis_id"] = getattr(analysis, "id")
        except Exception:
            pass

    # Merge extra
    if extra:
        try:
            ctx.update(extra)
        except Exception:
            pass

    log_error(title, err, ctx)


class SlackLogHandler(logging.Handler):
    """
    Logging handler that forwards ERROR+ records to Slack via send_error_to_slack.

    - Includes traceback if exc_info is present.
    - Uses record.getMessage() as body and record.name/levelname for context.
    """
    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            app_name = os.getenv("APP_NAME", "cppipeline2")
            title = f"{app_name} {record.levelname} - {record.name}-logger"
            # If exception info is attached, include traceback
            if record.exc_info:
                tb = "\n" + "".join(traceback.format_exception(*record.exc_info))
                msg = f"{msg}{tb}"
            # Rate-limit only ERROR and above; allow INFO/WARNING without limit
            rate_limit = record.levelno >= logging.ERROR
            _send_to_slack(msg, title, rate_limit=rate_limit)
        except Exception as e:
            # Avoid recursive logging storms; just print to stderr via logging module
            logging.getLogger(__name__).debug(f"SlackLogHandler emit failed: {e}")


def install_slack_logging(level: int = logging.ERROR, logger: logging.Logger | None = None) -> None:
    """
    Install a Slack logging handler on the given logger (root by default).

    - level: minimum level to forward (default ERROR)
    - logger: target logger; defaults to root logger
    """
    target = logger or logging.getLogger()
    # Prevent duplicates if called multiple times
    for h in target.handlers:
        if isinstance(h, SlackLogHandler):
            h.setLevel(level)
            return
    slack_handler = SlackLogHandler(level=level)
    # Reuse console formatter style if present
    fmt = logging.Formatter('%(asctime)s,%(msecs)d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s')
    slack_handler.setFormatter(fmt)
    target.addHandler(slack_handler)

# Example usage:
if __name__ == "__main__":
    send_error_to_slack(
        error_message="Something unexpected happened!",
        title="Server Warning"
    )
