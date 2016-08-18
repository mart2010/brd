-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"

-------------------------------------- Presentation layer -----------------------------------------
---------------------------------------------------------------------------------------------------
-- Goals:   - Layer for data access either for tools or user ad-hoc queries
--          - physical design is geared toward Redshift backend where many tables are
--            denormalized and data type unavailbale are replaced (ex. uuid)
--          - Other delivery:  the sparse Matrix...built for recommending app
--              (efficiently stored in relation model) (that should be an sql extract)
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------
create table presentation.dim_site (
    site_id smallint primary key,
    name varchar(20) not null,
    hostname varchar(30) not null
)
diststyle ALL
;

---------------------------------------------------------------
create table presentation.dim_language (
    code char(3) primary key,
    name varchar(85) not null,
    french_name varchar(85) not null --etc..
)
diststyle ALL
;


--------------------------------------------------------------
create table presentation.dim_date (
    date_id date primary key,
    year smallint not null,
    month smallint not null,
    month_name varchar(10) not null,
    day smallint not null,
    day_of_year smallint not null,
    day_name varchar(10) not null,
    calendar_week smallint not null,
    format_date char(10) not null,
    quarter char(2) not null,
    year_quarter char(7) not null,
    year_month char(7) not null,
    year_iso_week char(7) not null,
    weekend char(7) not null,
    iso_start_week date not null,
    iso_end_week date not null,
    month_start date not null,
    month_end date not null
)
diststyle ALL
;

---------------------------------------------------------------
create table presentation.dim_mds (
    code varchar(15) primary key,
    parent_code varchar(15),
    original_code varchar(20),
    text varchar(450) not null
)
diststyle ALL
;


---------------------------------------------------------------
create table presentation.dim_book (
    book_id bigint primary key,
    title_ori varchar(600) not null,
    lang_ori char(3) not null,
    mds_code varchar(30),
    --pivot most popular lang
    english_title varchar(600) not null,
    french_title varchar(600),
--    german_title varchar(480),
--    dutch_title varchar(450),
--    spanish_title varchar(360),
--    italian_title varchar(460),
--    swedish_title varchar(290),
--    finish_title varchar(360),
--    danish_title varchar(320),
--    portuguese_title varchar(350),
    foreign key (mds_code) references presentation.dim_mds(code),
    foreign key (lang_ori) references presentation.dim_language(code)
)
diststyle key distkey (book_id)
;

---------------------------------------------------------------
--Redshift chokes on the tag field name (so change to tag_name)
create table presentation.dim_tag (
    tag_id int primary key,
    tag_name varchar(80) unique not null,
    lang_code char(3) not null,
    foreign key (lang_code) references presentation.dim_language(code)
)
diststyle ALL
;

--colocate rel_tag with its book
create table presentation.rel_tag (
    tag_id int not null,
    book_id bigint not null,
    primary key (tag_id, book_id),
    foreign key (tag_id) references presentation.dim_tag(tag_id),
    foreign key (book_id) references presentation.dim_book(book_id)
)
diststyle key distkey (book_id)
;

---------------------------------------------------------------
create table presentation.dim_author (
    author_id int primary key,
    code varchar(100) unique not null,
    name varchar(250) not null
)
diststyle ALL
;

--co-locate rel_author with book
create table presentation.rel_author (
    author_id int not null,
    book_id bigint not null,
    primary key (author_id, book_id),
    foreign key (author_id) references presentation.dim_author(author_id),
    foreign key (book_id) references presentation.dim_book(book_id)
)
diststyle key distkey (book_id)
;


---------------------------------------------------------------
create table presentation.dim_reviewer (
    reviewer_id bigint primary key,
    --reviewer_uuid uuid unique,  -- for lookup only (not exported to RS)
    username varchar(350) not null,
    gender char(1) not null,
    birth_year smallint not null,
    status varchar(20) not null,
    occupation varchar(100),
    city varchar(200) not null,
    lati float,
    longi float,
    site_name varchar(20) not null
)
diststyle key distkey (reviewer_id);
;


---------------------------------------------------------------
create table presentation.review (
    id bigint primary key,
    similarto_id bigint,
    book_id int not null,
    reviewer_id int not null,
    site_id smallint not null,
    date_id date not null,
    -- all facts
    rating smallint,
    nb_likes int,
    lang_code char(3),
    review varchar(66000),  --based on max bytes not character (octet_length(review))
    foreign key (book_id) references presentation.dim_book(book_id),
    foreign key (reviewer_id) references presentation.dim_reviewer(reviewer_id),
    foreign key (site_id) references presentation.dim_site(site_id),
    foreign key (date_id) references presentation.dim_date(date_id),
    foreign key (lang_code) references presentation.dim_language(code)
)
diststyle key distkey (book_id)
;
