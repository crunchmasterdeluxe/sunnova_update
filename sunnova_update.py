import pymysql
import pandas as pd
import datetime
import requests
from base64 import b64encode
from customerio import APIClient, SendEmailRequest, CustomerIOException
# SEND STATUS UPDATES TO SUNNOVA

# Update statuses in Sunnova portal
db = pymysql.connect(
    host="ember-leads.cfsri1njcuza.us-east-2.rds.amazonaws.com",
    user="kevin_e_flynn",
    passwd="eu3oBc$IA8Vzccfv",
    database="leads"
)
cursor = db.cursor()
cursor.execute(f"""SELECT l.first_name,l.last_name,l.phone,l.email,
l.id,l.external_id,l.street,l.city,s.state,l.zip,st.status,l.sat,l.signed,
l.appt_date,l.appt_time,l.canvass_appt_id,l.canvass_address_id,l.updated marketplace_modified,l.created marketplace_created
FROM leads l 
LEFT JOIN market_state s ON s.id = l.state_id 
LEFT JOIN market_status st ON st.id = l.status_id 
WHERE l.provider_id IN (81,82,83,84,85,86,96,99,100,101) AND external_id IS NOT NULL AND external_id != 'None'""")
table_rows = cursor.fetchall()
df = pd.DataFrame(table_rows,columns=[i[0] for i in cursor.description]) #reps are active
db.close()

# # df = list(appts['canvass_appt_id'])


username = "lgcy.andygannaway@lgcypower.com"
password = "Iu5!3^dGF5RP"
url = "https://dealerapi.sunnova.com/services/v1.0/authentication"
response = requests.get(url, auth=(username, password))
response = response.json()
token = response['token']
sunnova_headers = {"Authorization": f"Bearer {token}"}

df = df[647:]

for _, row in df.iterrows():

    # Construct payload
    p = {}
    p['external_id'] = row['external_id']
    status = row['status']
    # Statuses that are not statuses in our system
    if row['signed'] != None:
        p['Status'] = 'Contract Signed'
    elif row['sat'] == 1:
        p['Status'] = 'Appointment Completed by Dealer'

    # Mapping sheet: https://docs.google.com/spreadsheets/d/1lUJDCoNf211BoP7tYVPIbtzsKunm10s5eSz010_v4AI/edit#gid=0
    elif status in ('Not Interested','Duplicate','Expired'):
        p['Status'] = 'Lost'
    elif status in ('Proposal Created','Multiple Contact Attempts Failed','Progressing, Not Signed','Called/Texted #1','Interested, Call Back After 30 Days','Interested, Follow Up in 48 Hours'):
        p['Status'] = 'Pending / Working'
    elif status in ('Already Solar','Not Qualified','Non-Serviceable Area'):
        p['Status'] = 'Rejected'
    elif status in ('Rep Rescheduled','Rep Missed Appointment','Provider to Reschedule','Rescheduled by Customer'):
        p['Status'] = 'Rescheduled Appointment'
    elif status == 'Bad Contact Info':
        p['Status'] = 'Unable to Contact'
    else:
        p['Status'] = 'Appointment Set with Dealer'
    sunnova = requests.patch(url=f"https://dealerapi.sunnova.com/services/v1.0/leads/{row['external_id']}",json=p,headers=sunnova_headers)
    print(sunnova.status_code,sunnova.json())
