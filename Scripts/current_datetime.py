from datetime import datetime
import pytz

tz = pytz.timezone('Asia/Kolkata')
# datetime object containing current date and time
now = datetime.now().astimezone(tz)

# dd/mm/YY H:M:S
dt_string = now.strftime("%d/%m/%Y %H:%M")
print(dt_string)