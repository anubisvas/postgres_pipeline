CREATE TABLE if not exists ga_sessions 
(
    session_id varchar(255) primary key,
    client_id varchar(255),
    visit_date date,
    visit_time time,
    visit_number int,
    utm_source varchar(255),
    utm_medium varchar(255),
    utm_campaign varchar(255),
    utm_adcontent varchar(255),
    utm_keyword varchar(255),
    device_category varchar(255),
    device_os varchar(255),
    device_brand varchar(255),
    device_model varchar(255),
    device_screen_resolution varchar(255),
    device_browser varchar(255),
    geo_country varchar(255),
    geo_city varchar(255)
);