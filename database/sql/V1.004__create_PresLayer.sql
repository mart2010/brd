-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"

-------------------------------------- Presentation layer -----------------------------------------
---------------------------------------------------------------------------------------------------
-- Goals:   - Layer for data access either for tools or user ad-hoc queries
--          - physical design is targeting Redshift backend
--              (denormalized) and replace unavailable data type (ex. uuid)
--          - Other delivery:  the sparse Matrix...built for recommending app
--              (efficiently stored in relation model) (that should be an sql extract)
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------
create table presentation.dim_site (
    id smallint primary key,
    name varchar(20) not null,
    hostname varchar(30) not null
)
--diststyle ALL
;


---------------------------------------------------------------
create table presentation.dim_language (
    code char(3) primary key,
    name varchar(85) not null,
    french_name varchar(65) not null --etc..
)
--diststyle ALL
;


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
)
--diststyle ALL
;


---------------------------------------------------------------
create table presentation.dim_mds (
    code varchar(15) primary key,
    parent_code varchar(15),
    original_code varchar(20),
    text varchar(420),
    foreign key parent_code references presentation.dim_mds(code)
)
--diststyle ALL
;


---------------------------------------------------------------
create table presentation.dim_tag (
    id int primary key,
    -- aggregated by capitalized form (to reduce duplications)
    tag varchar(255) unique,
    lang_code char(3)
)
--diststyle ALL
;


--colocate rel_tag with its book
create table presentation.rel_tag (
    tag_id int not null,
    book_id bigint not null,
    primary key (tag_id, book_id),
    foreign key (tag_id) references presentation.tag(id),
    foreign key (book_id) references presentation.book(id)
)
--diststyle key distkey (book_id)
;


---------------------------------------------------------------
create table presentation.dim_author (
    id int primary key,
    code varchar(100??) unique,
    name varchar(250) not null
)
--diststyle ALL
;

--colocate rel_author with its book
create table presentation.rel_author (
    author_id int not null,
    book_id bigint not null,
    primary key (author_id, book_id),
    foreign key (author_id) references presentation.author(id),
    foreign key (book_id) references presentation.book(id)
)
--diststyle key distkey (book_id)
;


---------------------------------------------------------------
create table presentation.book (
    id bigint primary key,
    title_ori text,
    original_lang char(3),
    mds_code varchar(15),
    --calculate pop based on nb_of_reviews loaded
    --pivot 10th most popular lang
    english_title varchar(550),
    french_title varchar(430),
    german_title varchar(480),
    dutch_title varchar(450),
    spanish_title varchar(360),
    italian_title varchar(460),
    swedish_title varchar(290),
    finish_title varchar(360),
    danish_title varchar(320),
    portuguese_title varchar(350)
)
--diststyle key distkey (id);
;


---------------------------------------------------------------
create table presentation.reviewer (
    id serial,
    id_uuid uuid unique,  -- for lookup only (not exported to RS)
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
--diststyle key distkey (id);
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
--diststyle key distkey (book_id)
;



