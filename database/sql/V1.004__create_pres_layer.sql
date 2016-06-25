-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-------------------------------------- Presentation layer -----------------------------------------
---------------------------------------------------------------------------------------------------
-- Goals:   - Layer for data access done by tools and user ad-hoc queries
--          - physical design is targeting Redshift backend
--              (denormalized of version of integraton, replace unavailable data type, e.g. uuid)
--          - Other delivery:  the sparse Matrix...built for recommending app
--              (efficiently stored in relation model) (that should be an sql extract)
---------------------------------------------------------------------------------------------------



-- Site and Language tables created only for possible lookup at client application side
-- but names are used directly as FK (compressed by RS anyway)

---------------------------------------------------------------
create table presentation.dim_site (
    name varchar(20) primary key,
    hostname varchar(25) not null
);
-- For RS distribution:
--diststyle ALL

-- DML given to create the table/flat file for Redshift
insert into presentation.dim_site
values
('librarything','www.librarything.com'),
,('goodreads','www.goodreads.com'),
,('babelio','www.babelio.com');

---------------------------------------------------------------
create table presentation.dim_language (
    name varchar(??30) primary key,
    french_name varchar(30) not null --etc..
);
--diststyle ALL

insert into presentation.dim_language
select english_name, french_name
from integration.language ;

---------------------------------------------------------------
create table presentation.dim_date (
    id date primary key
    year smallint,
    month smallint,
    month_name varchar(10),
    day smallint,
    day_of_year smallint,
    day_name varchar(10),
    calendar_week smallint,
    format_date char(10),
    quarter char(2),
    year_quarter char(7),
    year_month char(7),
    year_iso_week char(7),
    weekend char(7),
    iso_start_week date,
    iso_end_week date,
    month_start date,
    month_end date
);
--diststyle ALL

insert into presentation.dim_date
SELECT
	datum,
	EXTRACT(YEAR FROM datum) as year,
	EXTRACT(MONTH FROM datum) as month,
	-- Localized month name
	to_char(datum, 'TMMonth') as month_name,
	EXTRACT(DAY FROM datum) as day,
	EXTRACT(doy FROM datum) as day_of_year,
	-- Localized weekday
	to_char(datum, 'TMDay') AS day_name,
	-- ISO calendar week
	EXTRACT(week FROM datum) as calendar_week,
	to_char(datum, 'dd.mm.yyyy') as format_date,
	'Q' || to_char(datum, 'Q') as quarter,
	to_char(datum, 'yyyy/"Q"Q') as year_quarter,
	to_char(datum, 'yyyy/mm') as year_month,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') as year_iso_week,
	-- Weekend
	CASE WHEN EXTRACT(isodow FROM datum) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END as weekend,
	-- ISO start and end of the week of this date
	datum + (1 - EXTRACT(isodow FROM datum))::INTEGER as iso_start_week,
	datum + (7 - EXTRACT(isodow FROM datum))::INTEGER as iso_end_week,
	-- Start and end of the month of this date
	datum + (1 - EXTRACT(DAY FROM datum))::INTEGER as month_start,
	(datum + (1 - EXTRACT(DAY FROM datum))::INTEGER + '1 month'::INTERVAL)::DATE - '1 day'::INTERVAL as month_end
FROM (
	-- starting from min review_date + 50 year + 12..missing days for leap year: 365*50+12=18262
	SELECT '1969-01-01'::DATE + SEQUENCE.DAY AS datum
	FROM generate_series(0,18262) AS SEQUENCE(DAY)
	GROUP BY SEQUENCE.DAY
     ) DQ
ORDER BY 1;


---------------------------------------------------------------
create table presentation.dim_mds (
    code varchar(30) primary key,
    parent_code varchar(30),
    original_code varchar(35),
    value varchar(100),
    foreign key parent_code references presentation.dim_mds(code)
)
--diststyle ALL
;


---------------------------------------------------------------
create table presentation.dim_tag (
    id varchar(??) primary key, --as lower case?
    --etc..
)
--diststyle ALL
;

--colocate rel_tag with its book
create table presentation.rel_tag (
    tag_id varchar(??),
    book_id bigint,
    primary key (tag_id, book_id),
    foreign key (tag_id) references presentation.tag(id),
    foreign key (book_id) references presentation.book(id)
)
diststyle key distkey (book_id)
;


---------------------------------------------------------------
create table presentation.dim_author (
    id int primary key,
    --etc..
)
--diststyle ALL
;

--colocate rel_author with its book
create table presentation.rel_author (
    author_id varchar(??),
    book_id bigint,
    primary key (author_id, book_id),
    foreign key (author_id) references presentation.author(id),
    foreign key (book_id) references presentation.book(id)
)
diststyle key distkey (book_id)
;


---------------------------------------------------------------
create table presentation.book (
    id bigint primary key,
    title_ori text,
    original_lang varchar(??),
    mds_code varchar(30),
    --calculate pop based on nb_of_reviews loaded
    --pivot 10th most popular lang
    english_title varchar(500),
    french_title varchar(500),
    german_title varchar(500),
    dutch_title varchar(500),
    spanish_title varchar(500),
    italian_title varchar(500),
    japenese_title varchar(500),
    finish_title varchar(500),
    portuguese_title varchar(500)
)
diststyle key distkey (id);
;

--TODO: validate this
select coalesce(s.master_refid, w.work_refid) as id
        --make sure only master title and mds are used
        , max(case when s.master_refid is null then w.title else NULL end) as title_ori
        , max(case when s.master_refid is null then w.mds_code else NULL end) as mds_code
        , max(case when s.master_refid is null then w.title else NULL end) as english_title
        , max(case when wt.lang_code = 'fre' then wt.title else NULL end) as french_title
        , max(case when wt.lang_code = 'ger' then wt.title else NULL end) as german_title
from integration.work_info w
left join integration.work_sameas s on w.work_refid = s.work_refid
left join integration.work_title wt on wt.work_refid = w.work_refid
group by coalesce(s.master_refid, w.work_refid)


---------------------------------------------------------------
create table presentation.reviewer (
    id serial,
    id_uuid uuid unique,  -- for lookup only (not exported for RS)
    username varchar(200),
    gender char(1),
    birth_year smallint,
    status varchar(20),
    occupation varchar(100),
    country varchar(100),
    region varchar(200),
    city varchar(200),
    site_name varchar(20) not null
)
diststyle key distkey (id);

--TODO: validate if something similar can be done
--need to be populated prior to review (to get id)
insert into presentation.reviewer(id_uuid, username, gender, birth_year, status, site_name)
select u.id as id_uuid
        , username
        , case when random() < 0.4 then 'F' when random() < 0.6 then 'M' else 'U' end as gender
        , cast(random() * 50 + 1950 as smallint) as birth_year
        , s.logical_name
from integration.user u
join integration.site s on u.site_id = s.id
;


---------------------------------------------------------------
create table presentation.review (
    id bigint primary key,
    id_similarto bigint,
    book_id int not null,
    reviewer_id int not null,
    site_name varchar(20) not null,
    date_id date not null,
    -- all facts
    rating smallint,
    nb_likes int,
    lang_code char(3),
    -- review varchar(30000),  --based on max currently found
    foreign key (book_id) references presentation.book(id),
    foreign key (reviewer_id) references presentation.reviewer(id),
    foreign key (site_id) references presentation.site(id),
    foreign key (date_id) references presentation.dim_date(id),
)
diststyle key distkey (book_id)
;


insert into presentation.review
select r.id
        , s.other_review_id as id_similarto
        , coalesce(ws.master_refid, r.work_refid) as book_id
        , pr.id as reviewer_id
        , si.logical_name as site_name
        , r.review_date as date_id
        , r.parsed_rating as rating
        , r.likes as nb_likes
        , r.review_lang as lang_id
from integration.review r
left join integration.review_similarto s on s.review_id = r.id
left join integration.work_sameas ws on ws.work_refid = r.work_refid
join presentation.reviewer pr on pr.id_uuid = r.user_id
join integration.site si on si.id = r.site_id
;


