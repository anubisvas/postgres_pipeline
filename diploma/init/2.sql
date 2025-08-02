CREATE TABLE if not exists ga_hits 
(
    hit_id bigint primary key,
    session_id varchar(255),
    hit_date date,
    hit_time bigint,
    hit_number int,
    hit_type varchar(255),
    hit_referer varchar(255),
    hit_page_path varchar(10000),
    event_category varchar(255),
    event_action varchar(255),
    event_label varchar(255)
);

