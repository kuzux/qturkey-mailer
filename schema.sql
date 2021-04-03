create table addresses(id integer primary key, address text not null, unsubscribed integer default 0 not null);
create table templates(id integer primary key, gmail_id text not null, original_sender text not null, subject text not null, body text not null);
create table jobs(id integer primary key, status text not null, scheduled_to text notn null, started_at text, finished_at text, template_id integer not null references templates(id), address_start_index integer not null);
create table sent_mails(id integer primary key, job_id integer not null references jobs(id), template_id integer not null references templates(id), address_id integer not null references addresses(id), sent_at text not null, success integer not null default 1, traceback text);
