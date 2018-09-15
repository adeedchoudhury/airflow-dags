from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sensors import TimeSensor
from airflow.models import Variable
from datetime import datetime, timedelta, time
import numpy as np
import datetime
from datetime import datetime, timedelta
from pandas.io.formats.style import Styler
import StringIO
import io
from os import path
from lib.presto_tools import presto_query
from pyhive import presto
import pyhive
import pandas as pd

OWNER = 'adeed.choudhury'

default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'catchup': False,
    'start_date': datetime(2018, 9, 6),
    'email': ['adeed.choudhury@smartnews.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

#Query for last 14 days performance data for network reports.
sql = """
SELECT *
FROM rds_dmp.us_marketing.daily_stats_growth_network_funnel
where date between date(date_add('day',-15,date('{execution_date}'))) and date(date_add('day',-1,date('{execution_date}')))
"""

csv_sql = """
SELECT *
FROM rds_dmp.us_marketing.daily_stats_growth_network_funnel
where date between date(date_add('day',-15,date('{execution_date}'))) and date(date_add('day',-1,date('{execution_date}')))
and network NOT LIKE '%liftoff%'
"""

#Query for latest mature D7 cohort

sql_d7 = """
SELECT network,
       sum(spend) as spend,
       sum(CAST(installs as bigint)) as installs,
       sum(CAST(d7_retained as bigint)) as d7_retained,
       sum(CAST(total_predicted_value as real)) as total_predicted_value
FROM rds_dmp.us_marketing.daily_stats_growth_network_funnel
WHERE date between date(date_add('day',-25,date('{execution_date}'))) and date(date_add('day',-8,date('{execution_date}')))
GROUP BY 1
ORDER BY 2 desc
"""


#Query for KPI performance over last 14 days.

sql_kpi = """
with t_a AS (select
dt "date",
sum(dau) "DAU",
sum(CASE WHEN fq28_segment IN ('Medium','Heavy') THEN four_wau ELSE 0 END) "MHU",
sum(four_wau) "28DAU",
sum(CASE WHEN fq28_segment IN ('Medium','Heavy') THEN dau ELSE 0 END) "MHDAU",
CAST(sum(standard_ad_revenue) AS INTEGER) "Revenue",
ROUND(sum(standard_ad_revenue)/sum(CASE WHEN edition='ja_JP' THEN dau else 0 END),2) "ARPDAU",
ROUND(sum(CASE WHEN fq28_segment IN ('Medium','Heavy') THEN standard_ad_revenue ELSE 0 END) / 
 sum(CASE WHEN fq28_segment IN ('Medium','Heavy') and edition='ja_JP' THEN dau ELSE 0 END),2) "ARPMHDAU",
 CAST(sum(time_spent_minutes) AS INT) "Time spent (min)",
 sum(sessions) "Sessions",
 sum(installs) "Installs"
 FROM
hive.business_kpi.daily_agg_fq_segment "a"
 WHERE edition='en_US'
 AND CAST(dt as date) BETWEEN date_add('day',-15,date('{execution_date}')) and date_add('day',-1,date('{execution_date}'))
 group by 1
 ),
 t_b AS
 (
   select
   CAST(date_add('day',-27,CAST(dt as DATE)) AS VARCHAR) "date",
    sum(CASE WHEN days_since_install = 'exactly_27d' and fq28_segment IN ('Medium','Heavy') then four_wau ELSE 0 END) "MHU installs"
  FROM
hive.business_kpi.daily_agg_fq_segment "a"
 WHERE edition='en_US'
 AND CAST(dt as date) BETWEEN date_add('day',-15,date('{execution_date}')) and date_add('day',-1,date('{execution_date}'))
   group by 1
   ),
   t_joined AS (
   SELECT
   "t_a".*,"t_b"."MHU installs" from
   t_a LEFT JOIN t_b USING ("date")
 ), t_unioned AS (
 select *,'daily' as granularity from t_joined
 UNION ALL
 select cast(date_trunc('week',cast("date" as DATE)) AS VARCHAR) as "date",
 avg("DAU") "DAU",
avg("MHU") "MHU",
avg("28DAU") "28DAU",
avg("MHDAU") "MHDAU",
SUM("Revenue") "Revenue",
avg("ARPDAU") "ARPDAU",
AVG("ARPMHDAU") "ARPMHDAU",
SUM("Time spent (min)") "Time spent (min)",
sum("Sessions") "Sessions",
 sum("Installs") "Installs",
  sum("MHU Installs") "MHU Installs",
   'weekly' as granularity
   from t_joined
 group by 1
   UNION ALL
   select cast(date_trunc('month',cast("date" as DATE)) AS VARCHAR) as "date",
 avg("DAU") "DAU",
avg("MHU") "MHU",
avg("28DAU") "28DAU",
avg("MHDAU") "MHDAU",
SUM("Revenue") "Revenue",
avg("ARPDAU") "ARPDAU",
AVG("ARPMHDAU") "ARPMHDAU",
SUM("Time spent (min)") "Time spent (min)",
sum("Sessions") "Sessions",
 sum("Installs") "Installs",
  sum("MHU Installs") "MHU Installs",
   'monthly' as granularity
   from t_joined
 group by 1
)
select 
"date",
CAST("MHU" AS INT) "MHU",
CAST("28DAU" AS INT) "28DAU",
CAST("MHDAU" AS INT) "MHDAU",
CAST("DAU" AS INT) "DAU",
"Installs"
from t_unioned WHERE granularity='daily' ORDER BY "date" desc"""


#Creating Global HTML Variables that are placeholders for the information that go into the final email output. The output of the functions below change these variables.
t=''
s=''
r=''
q=''
o=''
u=''
concat=''
name=''


def create_yesterday_df(**kwargs):
	
	#Creating important date variables to be used in function
	today = kwargs['execution_date']+timedelta(days=1)
	today = today.strftime("%Y-%m-%d")

	yesterday = kwargs['execution_date']
	yesterday1 = yesterday.strftime("%Y-%m-%d")

	d7_cohort_start = kwargs['execution_date'] - timedelta(days=14)
	d7_cohort_start = d7_cohort_start.strftime("%Y-%m-%d")

	d7_cohort_end = kwargs['execution_date'] - timedelta(days=6)
	d7_cohort_end = d7_cohort_end.strftime("%Y-%m-%d")

	two_days_ago = kwargs['execution_date'] - timedelta(days=1)
	two_days_ago = two_days_ago.strftime("%Y-%m-%d")

	#Running Query to pull all US Marketing last 14 days
	cursor = presto.connect('presto.smartnews.internal', port=8081, username=OWNER).cursor()
	cursor.execute(sql.format(execution_date=today))
	ret = cursor.fetchall()
	columns = [c[0] for c in cursor.description]
	df = pd.DataFrame(ret, columns=columns)
	#Converting install data to integer
	df['installs'] = pd.to_numeric(df['installs'], errors='coerce', downcast='integer')
	#Create summarized dataframe and calculate yesterday's spend and install counts
	yesterday_df = df[df['date'] == yesterday1]
	yesterday_df = yesterday_df.groupby(['network'])['spend','installs'].agg('sum')
	total_spend = sum(yesterday_df['spend'])
	total_installs = sum(yesterday_df['installs'])
	yesterday_df = yesterday_df.reset_index()
	yesterday_df = yesterday_df.sort_values(by='spend', ascending=False)

	#Create totals_df

	d = {'network': 'Total',
	     'spend': [total_spend],
	     'installs': [total_installs]}
	df1 = pd.DataFrame(data=d)
	df1 = df1[['network','spend','installs']]
	#Append totals_df to yesterday_df
	yesterday_df = yesterday_df.append(df1)
	yesterday_df['cpi'] = yesterday_df['spend']/yesterday_df['installs'] 
	yesterday_df['cpi'] = yesterday_df['cpi'].round(decimals=2)
	yesterday_df['spend'] = yesterday_df['spend'].round(decimals=2)
	yesterday_df['installs'] = yesterday_df['installs'].astype(int)
	yesterday_df = yesterday_df.reset_index()
	yesterday_df[["spend","cpi"]] = yesterday_df[["spend", "cpi"]].applymap(lambda x: '${:,.2f}'.format(x))
	yesterday_df['installs'] = yesterday_df[["installs"]].applymap(lambda x: '{:,}'.format(x))
	yesterday_df = yesterday_df[['network','spend','installs','cpi']]
	
	#Rendering HTML
	global t
	t = yesterday_df.to_html(index=False, justify='left')


	#Creating ROAS DF

	roas_pivot = df[['date', 'network', 'campaign_name', 'adset_name', 'spend', 'installs', 'total_predicted_value']]
	roas_pivot = roas_pivot[(roas_pivot['date'] >= d7_cohort_start) & (roas_pivot['date'] <= d7_cohort_end)]
	roas_pivot['total_predicted_value'] = pd.to_numeric(roas_pivot['total_predicted_value'], errors='coerce')
	roas_pivot['adset_name'] = roas_pivot['adset_name'].replace([None], ['-'])

	roas_pivot = roas_pivot.groupby(['network','campaign_name','adset_name'])['spend','installs','total_predicted_value'].agg('sum')
	roas_pivot['cpi'] = roas_pivot['spend']/roas_pivot['installs']
	roas_pivot['uLTV'] = (roas_pivot['total_predicted_value']*4.17)/roas_pivot['installs']

	roas_pivot['roas'] = roas_pivot['uLTV'] - roas_pivot['cpi']
	roas_pivot = roas_pivot.reset_index()

	roas_pivot_best = roas_pivot.sort_values(by='roas', ascending=False)
	roas_pivot_best = roas_pivot_best[roas_pivot_best['installs'] > 30]
	roas_pivot_best = roas_pivot_best[:10]
	roas_pivot_best = roas_pivot_best[['network', 'campaign_name', 'adset_name','spend','cpi','uLTV','roas']]
	roas_pivot_best[["spend","cpi","uLTV","roas"]] = roas_pivot_best[["spend","cpi","uLTV","roas"]].applymap(lambda x: '${:,.2f}'.format(x))


	roas_pivot_worst = roas_pivot.sort_values(by='roas', ascending=True)
	roas_pivot_worst = roas_pivot_worst[roas_pivot_worst['installs'] > 30]
	roas_pivot_worst = roas_pivot_worst[:10]
	roas_pivot_worst = roas_pivot_worst[['network', 'campaign_name', 'adset_name','spend','cpi','uLTV','roas']]
	roas_pivot_worst[["spend","cpi","uLTV","roas"]] = roas_pivot_worst[["spend","cpi","uLTV","roas"]].applymap(lambda x: '${:,.2f}'.format(x))

	#Rendering HTML
	global o
	o = roas_pivot_best.to_html(index=False, justify='left', col_space='200')
	global u
	u = roas_pivot_worst.to_html(index=False, justify='left', col_space='200')

	#Creating Insights DF

	insights_df = df[['date','network','campaign_name','adset_name','spend','installs']]
	insights_df['adset_name'] = insights_df['adset_name'].replace([None], ['-'])
	insights_df = insights_df[insights_df['date'] >= two_days_ago]
	insights_pivot = insights_df.pivot_table(index =['network','campaign_name','adset_name'],
	                  columns='date',
	                  values = 'spend',
	                  aggfunc=np.sum)
	insights_pivot['DoD Spend Change'] = insights_pivot[yesterday1] - insights_pivot[two_days_ago]
	insights_pivot = insights_pivot.sort_values(by='DoD Spend Change', ascending=False)
	insights_pivot['abs'] = insights_pivot['DoD Spend Change'].abs()
	insights_pivot = insights_pivot.sort_values(by='abs', ascending=False)
	insights_pivot = insights_pivot.drop(columns=['abs'])
	insights_pivot[[two_days_ago,yesterday1,"DoD Spend Change"]] = insights_pivot[[two_days_ago,yesterday1,"DoD Spend Change"]].applymap(lambda x: '${:,.2f}'.format(x))
	insights_pivot = insights_pivot.reset_index()
	insights_head = insights_pivot[:10]
	
	#Rendering HTML
	global q
	q = insights_head.to_html(index=False, justify='left', col_space='200')



def create_kpi_df(**kwargs):
	today = kwargs['execution_date']+timedelta(days=1)
	today = today.strftime("%Y-%m-%d")
	cursor = presto.connect('presto.smartnews.internal', port=8081, username=OWNER).cursor()
	cursor.execute(sql_kpi.format(execution_date=today))
	ret = cursor.fetchall()
	columns = [c[0] for c in cursor.description]
	kpi_df = pd.DataFrame(ret, columns=columns)
	#Creating KPI DF

	kpi_df['28DAU'] = kpi_df[['28DAU']].applymap(lambda x: '{:,}'.format(x))
	kpi_df['MHU'] = kpi_df[['MHU']].applymap(lambda x: '{:,}'.format(x))
	kpi_df['MHDAU'] = kpi_df[['MHDAU']].applymap(lambda x: '{:,}'.format(x))
	kpi_df['DAU'] = kpi_df[['DAU']].applymap(lambda x: '{:,}'.format(x))
	kpi_df['Installs'] = kpi_df[['Installs']].applymap(lambda x: '{:,}'.format(x))
	
	#Rendering HTML
	global s
	s = kpi_df.to_html(index=False, justify='left')


def matured_kpi_df(**kwargs):
	today = kwargs['ds']
	cursor = presto.connect('presto.smartnews.internal', port=8081, username=OWNER).cursor()
	cursor.execute(sql_d7.format(execution_date=today))
	ret = cursor.fetchall()
	columns = [c[0] for c in cursor.description]
	d7_df = pd.DataFrame(ret, columns=columns)

	#Adding 2 Year LTV coefficient

	d7_df['2 Year LTV'] = d7_df['total_predicted_value'] * 3.04

	#Adding in CPI and uLTV

	d7_df['cpi'] = d7_df['spend']/d7_df['installs']
	d7_df['uLTV'] = d7_df['2 Year LTV'] / d7_df['installs']

	#Adding in ROAS value

	d7_df['ROAS'] = d7_df['uLTV']-d7_df['cpi']

	#Add in D7 RR % and CPD7
	d7_df['D7 RR %'] = d7_df['d7_retained']/d7_df['installs']
	d7_df['CPD7'] = d7_df['spend']/d7_df['d7_retained']

	d7_df = d7_df[['network', 'spend', 'installs', 'cpi', 'uLTV', 'ROAS', 'D7 RR %', 'CPD7']]

	#Update formatting on d7_df

	d7_df['installs'] = d7_df[['installs']].applymap(lambda x: '{:,}'.format(x))
	d7_df[["spend","cpi","CPD7","uLTV","ROAS"]] = d7_df[["spend","cpi","CPD7","uLTV","ROAS"]].applymap(lambda x: '${:,.2f}'.format(x))
	d7_df['D7 RR %'] = d7_df[['D7 RR %']].applymap(lambda x: "{0:.2f}%".format(x * 100))

	#Rendering HTML
	global r
	r = d7_df.to_html(index=False, justify='left', col_space='200')



def generate_HTML_and_send_email(**kwargs):
	
	#Generate CSV data
	today = kwargs['execution_date']+timedelta(days=1)
	today = today.strftime("%Y-%m-%d")
	cursor = presto.connect('presto.smartnews.internal', port=8081, username=OWNER).cursor()
	cursor.execute(csv_sql.format(execution_date=today))
	ret = cursor.fetchall()
	columns = [c[0] for c in cursor.description]
	df = pd.DataFrame(ret, columns=columns)
	#Removing JP FB ad accounts
	df = df[(df['campaign_name'] !='CyberZ_facebook') & (df['campaign_name'] !='SN_MP_DynamicAd_Grape_Articles')]
	global name
	name = 'master_network_data.csv'
	csv_file = df.to_csv(name, encoding='utf-8', index=False)

	create_yesterday_df(**kwargs)
	create_kpi_df(**kwargs)
	matured_kpi_df(**kwargs)

	html_v2 = """
	<head>
	<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
	<meta charset="utf-8"/>
	<title><u>Yesterday's UA Performance</u></title>
	<style type="text/css" media="screen">
	table {
	    border: 1px solid black;
	    border-collapse: collapse;
	    border-spacing: 5px;
	    width:100%%;
	    text-align: center;
	    border-spacing: 2px;
	    padding: 5px;
	 }
	    
	th {
	    text-align: center;
	    background-color: #194196;
	    color: white;
	    height: 50px;
	 }
	 
	</style>
	</head>
	<html><body><h1><u>Yesterday's UA Performance</u></h1>
	<h2>Network Overview</h2>
	<table> 
	%s 
	</table>
	<h2>KPI Overview</h2>
	<table> 
	%s
	</table>
	<h2>Latest Matured D7 Weekly Cohort</h2>
	<table> 
	%s
	</table>
	<h2>Top DoD Spend Changes in Adsets </h2>
	<table> 
	%s
	</table>
	<h2>Best Campaigns by ROAS (using latest matured week data) </h2>
	<table> 
	%s
	</table>
	<h2>Worst Campaigns by ROAS (using latest matured week data) </h2>
	<table> 
	%s
	</table>
	</body></html>""" % (t,s,r,q,o,u)


	attachments = concat
	html_v3 = html_v2.encode('utf-8').strip()


	import smtplib
	import mimetypes
	import datetime
	from email.mime.multipart import MIMEMultipart
	from email import encoders
	from email.message import Message
	from email.mime.audio import MIMEAudio
	from email.mime.base import MIMEBase
	from email.mime.image import MIMEImage
	from email.mime.text import MIMEText

	now = datetime.datetime.now()
	emailfrom = 'marketing_data_reports@smartnews.com'
	emailto = ['adeed.choudhury@smartnews.com',
		   'andrew.kim@smartnews.com',
		   'fabien.nicolas@smartnews.com',
		   'jessica.wang@smartnews.com',
		   'jerry.chi@smartnews.com',
		   'marketing_data_reports@smartnews.com',
		   'shigeharu.kaku@smartnews.com',
		   'genie.ko@smartnews.com']
	fileToSend = name
	username = 'marketing_data_reports@smartnews.com'
	password = Variable.get('us_marketing_email_secret')
	    
	msg = MIMEMultipart()
	msg["From"] = emailfrom
	msg["To"]= ", ".join(emailto)
	msg["Subject"] = 'US Master Network Data - ' + str(today)
	msg.preamble = 'csv contained'

	body = html_v3
	msg.attach(MIMEText(body,'html'))
	    
	ctype, encoding = mimetypes.guess_type(fileToSend)
	if ctype is None or encoding is not None:
	    ctype = "application/octet-stream"

	maintype, subtype = ctype.split("/", 1)

	if maintype == "text":
	    fp = open(fileToSend)
	    # Note: we should handle calculating the charset
	    attachment = MIMEText(fp.read(), _subtype=subtype)
	    fp.close()
	elif maintype == "image":
	    fp = open(fileToSend, "rb")
	    attachment = MIMEImage(fp.read(), _subtype=subtype)
	    fp.close()
	elif maintype == "audio":
	    fp = open(fileToSend, "rb")
	    attachment = MIMEAudio(fp.read(), _subtype=subtype)
	    fp.close()
	else:
	    fp = open(fileToSend, "rb")
	    attachment = MIMEBase(maintype, subtype)
	    attachment.set_payload(fp.read())
	    fp.close()
	    encoders.encode_base64(attachment)
	attachment.add_header("Content-Disposition", "attachment", filename=fileToSend)
	msg.attach(attachment)

	server = smtplib.SMTP("smtp.gmail.com:587")
	server.starttls()
	server.login(username,password)
	server.sendmail(emailfrom, emailto, msg.as_string())
	server.quit()
with DAG('etl_us_marketing_daily_email', default_args=default_args, schedule_interval='@daily', concurrency=10, max_active_runs=1) as dag:

	 
	hold_on = TimeSensor(
    		task_id='hold_on',
    		target_time=time(hour=12, minute=30),
    		dag=dag
		)


	send_email = PythonOperator(
            task_id='generate_and_send_email',
            python_callable=generate_HTML_and_send_email,
            op_args=[
            ],
            # The following is required to pass macros to the PythonOperator
            # See https://stackoverflow.com/a/45870153
            provide_context=True,
            retries=0,
            dag=dag
        )

	remove_tmp_file = BashOperator(
            task_id='remove_fb_tmp_file',
            bash_command='rm -f {}'.format(name),
            dag=dag
        )

hold_on >> send_email >> remove_tmp_file





		


