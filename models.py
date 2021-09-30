from datetime import date, datetime, timedelta
from pprint import pprint
import re

import telegram
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine, text

import config

bot = telegram.Bot(config.PINGUSE_TOKEN)
scheduler = BlockingScheduler()

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
        '''
        Check downloads
        '''
        total_users = conn.execute(text("SELECT COUNT(*) FROM users;"))
        total_users = total_users.first()[0]
        user_stats = {}

        # different time ranges
        month_range=3   # number of months to track
        define_active = 5   # define active accounts as downloads within past 5 days
        this_month = datetime.strptime(str(date.today()), "%Y-%m-%d")
        this_month = this_month.month
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        last_five = today - timedelta(days=define_active)
        
        active_count = 0 # active within past 5 days
        downld_data = conn.execute(
            text(f"SELECT * FROM histories WHERE MONTH(created_at) = MONTH(CURRENT_DATE()) - {month_range} AND YEAR(created_at) = YEAR(CURRENT_DATE());")
        )

        # column indexes for histories
        email_col, date_col, nqn_col = 1, 8, 7
        for d in downld_data.all():
            email = d[1]
            if email not in user_stats:
                user_stats[email] = ["--N/A--", 0,0,0, None] # [user name, downloads today, downloads for month, downloads for 3 months, active account]
            
            data_date = d[date_col]
            data_month = d[date_col].strptime(str(d[date_col]), "%Y-%m-%d %H:%M:%S").month

            if data_date == today: # number of q downloaded today
                user_stats[email][1] += d[nqn_col]
            if data_month == this_month: # number of q downloaded this month
                user_stats[email][2] += d[nqn_col]

            if data_date>=last_five and user_stats[email][4]==None:
                user_stats[email][4] = True
                active_count+=1
            elif data_date < last_five and user_stats[email][4]==None:
                user_stats[email][4] = False
            # number of q downloaded past 3 months
            try:    # integer
                user_stats[email][2] += d[nqn_col]
            except Exception:   # handle old data format
                qnstr = re.sub("[\['\]]", "", d[nqn_col])
                qnstr = sum([int(x) for x in qnstr.split(", ")])
                user_stats[email][3] += qnstr
        
        # get username
        all_users = conn.execute(text("SELECT * FROM users;"))
        for users in all_users.all():
            email = users[3]
            if email in user_stats:
                user_stats[email][0] = users[2]

        # create message
        summary = f"Summary (Downloads within the last 5 DAYS):\n---------------\nActive Accounts: {active_count}/{total_users}\nInactive Accounts:{total_users-active_count}/{total_users}"
        active_template = "{1}    ({0}):\n    today : {2}\n    this month : {3}\n    past 3 months : {4}"
        inactive_template = "{1}    ({0}):\n    this month : {3}\n    past 3 months : {4}"
        active_list = []
        inactive_list = []
        for email in user_stats:
            if user_stats[email][4]:
                active_list.append(active_template.format(email, *user_stats[email]))
            else:
                inactive_list.append(inactive_template.format(email, *user_stats[email]))
        
        active_str = "\n".join(active_list)
        inactive_str = "\n".join(inactive_list)
        if not active_str:
            active_str = "--N/A--"
        if not inactive_str:
            inactive_str = "--N/A--"
        message = f'{summary}\n\nActive Accounts (Questions downloaded)\n-------------\n{active_str}\n\nInactive Accounts (Questions downloaded)\n-------------\n{inactive_str}'
        
        print(message)
        # send message
        # bot.sendMessage(chat_id=config.TELE_CHAT_ID, text=message)
    
if __name__ == "__main__":
    get_daily_usage()
    # start job at 8pm everyday
    # scheduler.add_job(get_daily_usage, 'interval', hours=24, start_date=datetime.today().replace(hour=20, minute=0, second=0, microsecond=0))

    # try:
    #     scheduler.start()
    # except (KeyboardInterrupt, SystemExit):
    #     pass
