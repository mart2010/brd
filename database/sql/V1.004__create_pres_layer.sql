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
('Librarything','www.librarything.com'),
,('Goodreads','www.goodreads.com'),
,('Babelio','www.babelio.com');

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


create table presentation.book (
    id int primary key,
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
    swedish_title varchar(500),
    finish_title varchar(500),
    portuguese_title varchar(500)
);

--TODO: validate this
select coalesce(s.master_refid, w.work_refid) as id
        --make sure only master title and mds are used
        , max(case when s.master_refid is null then w.title else NULL end) as title_ori
        , max(case when s.master_refid is null then w.mds_code else NULL end) as mds_code
        , max(case when s.master_refid is null then w.title else NULL end) as english_title
        , max(case when wt.lang_code = 'fre' then wt.title else NULL end) as french_title
from integration.work_info w
left join integration.work_sameas s on w.work_refid = s.work_refid
left join integration.work_title wt on wt.work_refid = w.work_refid



group by coalesce(s.master_refid, w.work_refid)






create table presentation.reviewer (
    id int primary key,
    username varchar(200),
    gender char(1),
    birth_year smallint,
    status varchar(20),
    occupation varchar(100),
    country varchar(100),
    region varchar(200),
    city varchar(200),
    site_id smallint not null,
    foreign key(site_id) references presentation.site(id)
);





create table presentation.review (
    id bigint primary key,
    id_similarto bigint,
    book_id int not null,
    reviewer_id int not null,
    site_id smallint not null,
    date_id smallint not null, --smart-key
    -- all facts
    rating smallint,
    nb_likes int,
    lang_code char(3),
    review varchar(30000),  --based on max currently found
    foreign key (book_id) references presentation.book(id),
    foreign key (reviewer_id) references presentation.reviewer(id),
    foreign key (site_id) references presentation.site(id),
    foreign key (date_id) references presentation.dim_date(id),
);

-- Loading DML
insert into presentation.review
select r.id
        , s.other_review_id as id_similarto
        , coalesce(ws.work_refid, w.refid) as book_id
        , w.refid as book_id
        , w.refid as book_id

from integration.review r
left join integration.review_similarto s on s.review_id = r.id
join integration.work w on w.refid = r.work_refid
left join integration.work_sameas ws on ws.work_refid = r.work_refid





--check.. if I could add this so to track more than one duplciates (the fill dupes review with master being a reference (minimum) id?)
create table presentation.review_similarto (
    review_id bigint,
    master_review_id bigint,

);


create table presentation.dim_mds (
    id int primary key

);

--see how to best manage the many-to-many...
create table presentation.dim_tag (
    id int primary key
);




