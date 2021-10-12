from datetime import date, datetime, timedelta
from pprint import pprint
import re

import telegram
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine, text

import config

bot = telegram.Bot(config.PINGUSE_TOKEN)
scheduler = BlockingScheduler()

acc_to_omit = ["Test2@test.com",
            "tengxinhui73@gmail.com",
            "testtest@mail.com",
            "saraikmalia.vicuna@gmail.com"]

MONTH_RANGE = 3   # number of months to track
DEFINE_ACTIVE = 5   # define active accounts as downloads within past 5 days

SQL_STATEMENT = f"SELECT histories.user_email, histories.no_of_question, histories.created_at, users.name FROM histories LEFT JOIN users on users.email = histories.user_email WHERE MONTH(histories.created_at) >= MONTH(CURRENT_DATE()) - {MONTH_RANGE} AND YEAR(histories.created_at) = YEAR(CURRENT_DATE()) AND histories.user_email NOT IN ('Test2@test.com', 'tengxinhui73@gmail.com', 'testtest@mail.com', 'saraikmalia.vicuna@gmail.com');"

GET_ACTIVE_USERS_SQL = f"SELECT count(distinct(histories.user_email)) FROM histories WHERE created_at >= DATE_ADD( CURDATE(), INTERVAL -{DEFINE_ACTIVE} DAY) AND user_email NOT IN ('Test2@test.com', 'tengxinhui73@gmail.com', 'testtest@mail.com', 'saraikmalia.vicuna@gmail.com');"

TOTAL_USERS_SQL = f"SELECT COUNT(*) FROM USERS;"

def get_daily_usage():
    '''
    Notify usage stats on daily basis
    1. active accounts : x/total (active within past 5 days i.e. generated docs)
        - name (email) : 
            num q downloads today: x
            total q downloaded for month: x
            total q downloaded for past 3 months : x
    2. inactive accounts (inactive for more than 5 days)
        - name (email)
            num q downloaded for month: x
            total q downloaded for past 3 months : x 
    '''
    engine = create_engine(config.SQLALCHEMY_DATABASE_URI, echo=False, future=True)
    with engine.connect() as conn:
        user_stats = {}

        all_users = conn.execute(text(SQL_STATEMENT))
        total_users = conn.execute(text("SELECT COUNT(*) FROM users;")).first()[0]
        for email, nqn, created_at, name in all_users:
            user_stats[email] = [name, 0,0,0, False] # [user name, downloads today, downloads for month, downloads for 3 months, active account]
            dld_count = parse_number(nqn)
            user_stats[email][3] += dld_count
            
            if created_at > datetime.today()-timedelta(days=DEFINE_ACTIVE):
                user_stats[email][4] = True
                
            if created_at.date == datetime.today().date:
                user_stats[email][1] += dld_count
            if created_at.month == created_at.strptime(str(created_at), "%Y-%m-%d %H:%M:%S").month:
                user_stats[email][2] += dld_count

        # create message
        active_template = "{1}    ({0}):\n    today : {2}\n    this month : {3}\n    past 3 months : {4}"
        inactive_template = "{1}    ({0}):\n    this month : {3}\n    past 3 months : {4}"
        
        active_count=0
        active_list = []
        inactive_list = []
        for email in user_stats:
            if user_stats[email][4]:
                active_list.append(active_template.format(email, *user_stats[email]))
                active_count += 1
            else:
                inactive_list.append(inactive_template.format(email, *user_stats[email]))

        summary_msg = f"Summary (Downloads within the last 5 DAYS):\n---------------\nActive Accounts: {active_count}/{total_users}\nInactive Accounts:{total_users-active_count}/{total_users}"
        
        active_str = "\n".join(active_list)
        inactive_str = "\n".join(inactive_list)

        if not active_str:
            active_str = "--N/A--"
        if not inactive_str:
            inactive_str = "--N/A--"

        # send message 
        bot.sendMessage(chat_id = config.TELE_CHAT_ID, text=summary_msg)
        bot.sendMessage(chat_id=config.TELE_CHAT_ID, text=active_str)
        bot.sendMessage(chat_id=config.TELE_CHAT_ID, text=inactive_str)
                
        # print(message)

def parse_number(num):
    if type(num)==str:
        num = num.strip('][').split(', ')
        num = sum([int(x.strip("'")) for x in num])
    return num

if __name__ == "__main__":
    get_daily_usage()
    # start job at 8pm everyday
    scheduler.add_job(get_daily_usage, 'interval', hours=24, start_date=datetime.today().replace(hour=20, minute=0, second=0, microsecond=0))

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
