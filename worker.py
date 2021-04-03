import mimetypes
import base64
import traceback
import time
from contextlib import closing
import re
import json
import os

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from cgi import parse_header

from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request

from dotenv import load_dotenv

import sqlite3
from cuttlepool import CuttlePool
import schedule

authorized_senders = []
def build_authorized_senders():
    global authorized_senders
    authorized_senders = os.environ["AUTHORIZED_SENDERS"].split()

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class SQLitePool(CuttlePool):
  def normalize_resource(self, resource):
      resource.row_factory = dict_factory
  def ping(self, resource):
      try:
          rv = resource.execute('SELECT 1').fetchall()
          return (1,) in rv
      except sqlite3.Error:
          return False

def gmail_login():
    SCOPES = ['https://mail.google.com/']
    SERVICE_KEY = "ec2-mailer-service.json"
    alias = os.environ["EMAIL_ADDRESS"]
    
    # I followed that tutorial to obtain a service account file
    # https://developers.google.com/identity/protocols/oauth2/service-account
    # Created a service account and delegated authority to it
    creds = Credentials.from_service_account_file(SERVICE_KEY, scopes=SCOPES) \
            .with_subject(alias)

    # We can use this over and over again without logging out
    return build('gmail', 'v1', credentials=creds)

def build_mail(to, subject, body, attachments=[]):
    sender = f"QTurkey <{os.environ["EMAIL_ADDRESS"]}>"

    mail = MIMEMultipart()
    mail['to'] = to
    mail['from'] = sender
    mail['subject'] = subject

    body = MIMEText(body.decode('utf-8'), 'html')
    mail.attach(body)

    for attachment in attachments:
        content_type, type_args = parse_header(attachment['content_type'])
        main_type, sub_type = content_type.split('/', 1)
        decoded_content = base64.urlsafe_b64decode(attachment['b64_content'])
        if main_type == "application":
            mime_attachment = MIMEApplication(decoded_content, _subtype=sub_type)
        elif main_type == "image":
            mime_attachment = MIMEImage(decoded_content, _subtype=sub_type)
        else:
            # TODO: Support more mime types
            raise ValueError("unsupported mime type")

        match = re.match(r'^<(.*)>$', attachment['content_id'])
        bare_content_id = match.group(1)

        mime_attachment.add_header('Content-ID', attachment['content_id'])
        mime_attachment.add_header('X-Attachment-Id', bare_content_id)
        mime_attachment.add_header('Content-Disposition', attachment['content_disposition'])
        
        mail.attach(mime_attachment)
    
    return mail

def send_mail(client, mail):
    mail_data = {'raw': base64.urlsafe_b64encode(mail.as_string().encode('utf-8')).decode('ascii')}
    # Should there be some sort of retry logic?
    print(f"Sending a mail to {mail['to']}")
    response = client.users().messages().send(userId='me', body=mail_data).execute()

def get_addresses(conn):
    cur = conn.cursor()
    cur.execute("select email from addresses where unsubscribed_at is null")
    res = cur.fetchall()
    cur.close()
    return res

def all_matching_mail_ids(client):
    results = client.users().messages().list(userId='me', q='subject:"[mail-list]"').execute()
    messages = results.get('messages', [])
    return [msg['id'] for msg in messages]

def find_if_fetched_template(conn, mail_id):
    with closing(conn.cursor()) as cur:
        cur.execute("select count(id) from templates where gmail_id = ?", (mail_id, ))
        return cur.fetchone()["count(id)"] != 0

def get_mail_body(html_part):
    b64_body = html_part['body']['data']
    body = base64.urlsafe_b64decode(b64_body)
    return body

def get_mail_body_multipart(multipart):
    html_parts = [part for part in multipart['parts'] if part['mimeType'] == "text/html"]
    return get_mail_body(html_parts[0])
            
    return None

def add_attachment_to_db(conn, attachment):
    with closing(conn.cursor()) as cur:
        cur.execute("""insert into attachments
            (template_id, content_id, content_type, content_disposition, b64_content)
            values
            (:template_id, :content_id, :content_type, :content_disposition, :b64_content)
        """, attachment)
        conn.commit()

def get_attachment(client, part, mail_id):
    def header_value(name):
        return next(h['value'] for h in part['headers'] if h['name'] == name)

    content_type = header_value('Content-Type')
    content_id = header_value('Content-ID')
    content_disposition = header_value('Content-Disposition')
    if 'attachmentId' in part['body']:
        attachment_id = part['body']['attachmentId']
        result = client.users().messages().attachments() \
            .get(userId='me', messageId=mail_id, id=attachment_id).execute()
        b64_content = result['data']
    else:
        b64_content = part['body']['data']

    return { "content_type": content_type, "content_id": content_id,
        "content_disposition": content_disposition, "b64_content": b64_content}

def get_mail_information(client, mail_id):
    message = client.users().messages().get(userId='me', id=mail_id).execute()

    headers = message['payload']['headers']
    subject_header = next(filter(lambda kv: kv['name']=='Subject', headers))['value']
    from_header = next(filter(lambda kv: kv['name']=='From', headers))['value']

    match = re.match(r"^\[.*\]\s*(.+)$", subject_header)
    subject = match.group(1)

    match = re.match(r"^.* <(.*)>$", from_header)
    sender = match.group(1)

    parts = message['payload']['parts']
    attachments = []
    body = None
    for part in parts:
        if part['mimeType'] == "text/html":
            body = get_mail_body(part)
        elif part['mimeType'] == "multipart/alternative":
            body = get_mail_body_multipart(part)
        else:
            print("Handling attachment")
            attachment = get_attachment(client, part=part, mail_id=mail_id)
            attachments.append(attachment)

    return {"gmail_id": mail_id, "sender": sender, "subject": subject, 
        "body": body, "attachments": attachments}

def add_template_to_db(conn, template):
    print(f"Adding mail from {template['sender']} to db")
    with closing(conn.cursor()) as cur:
        cur.execute("""insert into templates 
            (gmail_id, original_sender, subject, body)
            values (:gmail_id, :sender, :subject, :body);""", template)

        cur.execute("select last_insert_rowid()")
        template["id"] = cur.fetchone()["last_insert_rowid()"]

        conn.commit()


def get_mail_to_send(conn, client):
    ids = all_matching_mail_ids(client)
    # TODO: I think we can do this somewhat more effectively
    ids_not_in_db = [id for id in ids if not find_if_fetched_template(conn, id)]
    mail_infos = [get_mail_information(client, id) for id in ids_not_in_db]
    for info in mail_infos:
        add_template_to_db(conn, info)
    authorized_mails = [mail for mail in mail_infos if mail['sender'] in authorized_senders]
    for mail in authorized_mails:
        for attachment in mail['attachments']:
            attachment['template_id'] = mail['id']
            add_attachment_to_db(conn, attachment)

    # We fail when we get 2 authorized mails at the same time
    # But in that case; we fail elsewhere too
    if len(authorized_mails) == 0:
        return None
    else:
        return authorized_mails[0]

def create_db_job(conn, template_id, start_index=0, schedule_offset='+0 minutes'):
    with closing(conn.cursor()) as cur:
        print(f"tid {template_id}")
        cur.execute("""insert into jobs 
            (status, scheduled_to, template_id, address_start_index)
            values ('pending', strftime('%Y-%m-%d-%H-%M', datetime('now', :offset)), :tid, :idx)""",
            ({ "offset": schedule_offset, "tid": template_id, "idx": start_index}))
        conn.commit()

def get_job_from_db(conn):
    with closing(conn.cursor()) as cur:
        cur.execute("""select * from jobs 
            where status='pending' 
            and scheduled_to <= strftime('%Y-%m-%d-%H-%M', 'now')""")
        dbres = cur.fetchall()
        if len(dbres) == 0:
            return None
        else:
            return dbres[0]

def mark_job_as_started(conn, job):
    with closing(conn.cursor()) as cur:
        print(f"Marking {job['id']} as started")
        cur.execute("""update jobs
            set status = 'started', started_at = strftime('%Y-%m-%d-%H-%M', 'now')
            where id = :id""", job)
        conn.commit()

def mark_job_as_finished(conn, job):
    with closing(conn.cursor()) as cur:
        cur.execute("""update jobs
            set status = 'finished', finished_at = strftime('%Y-%m-%d-%H-%M', 'now')
            where id = :id""", job)
        conn.commit()

def get_addresses(conn, start_idx, limit):
    with closing(conn.cursor()) as cur:
        cur.execute("select * from addresses where id > ? limit ?",
            (start_idx, limit))
        return cur.fetchall()

def get_template(conn, id):
    with closing(conn.cursor()) as cur:
        cur.execute("select * from templates where id = ? ", (id, ))
        return cur.fetchone()

def add_sent_mail(conn, job_id, template_id, address, error):
    with closing(conn.cursor()) as cur:
        success = int(error == '')
        cur.execute("""insert into sent_mails 
            (job_id, template_id, address, sent_at, success, traceback)
            values (?, ?, ?, strftime('%Y-%m-%d-%H-%M', 'now'), ?, ?)""",
            (job_id, template_id, address, success, error))
        conn.commit()

def get_attachments_for_template(conn, template_id):
    with closing(conn.cursor()) as cur:
        cur.execute("select * from attachments where template_id = ?", (template_id, ))

        return cur.fetchall()

def run_scheduled_job(pool):
    limit_per_batch = 600
    next_batch_offset = '+1 day'
    sleep_after_each = 0.5

    client = gmail_login()

    with pool.get_resource() as conn:
        # TODO: Break up this function
        job = get_job_from_db(conn)
        if job == None: 
            return
        mark_job_as_started(conn, job)
        start_idx = job['address_start_index']
        addresses = get_addresses(conn, start_idx, limit_per_batch)
        if len(addresses) == 0:
            mark_job_as_finished(conn, job)
            return
        last_addr_idx = addresses[-1]['id']
        template = get_template(conn, job['template_id'])
        attachments = get_attachments_for_template(conn, job['template_id'])
        for addr in addresses:
            mail = build_mail(to=addr['address'], subject=template['subject'], 
                body=template['body'], attachments=attachments)
            error_msg = ''
            try:
                send_mail(client, mail)
            except:
                error_msg = traceback.format_exc()
            add_sent_mail(conn, job_id = job['id'], template_id=template['id'], 
                address=addr['address'], error=error_msg)
            #TODO: Write out a sent log with error message
            time.sleep(sleep_after_each)

        if len(addresses) == limit_per_batch:
            print("Scheduling new job")
            print(f"templ {template}")
            create_db_job(conn, template_id=template['id'], start_index=last_addr_idx,
                schedule_offset=next_batch_offset)
        
        mark_job_as_finished(conn, job)

        # TODO: Send a batch done report

def create_job_from_gmail(pool):
    client = gmail_login()

    with pool.get_resource() as conn:
        mail_to_send = get_mail_to_send(conn, client)
        if mail_to_send != None:
            print("Adding db job for mail")
            create_db_job(conn, template_id=mail_to_send['id'])

if __name__ == "__main__":
    load_dotenv()
    build_authorized_senders()
    pool = SQLitePool(factory=sqlite3.connect, capacity=5, database="mailer.db")
    # run_scheduled_job(pool)
    # schedule.every().minute.do(create_job_from_gmail, pool)
    # schedule.every().minute.do(run_scheduled_job, pool)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
