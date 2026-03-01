from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def lambda_handler(event, context):
    london = ZoneInfo("Europe/London")
    today = datetime.now(london).date()
    dt = (today - timedelta(days=1)).isoformat()
    return {"dt": dt}
