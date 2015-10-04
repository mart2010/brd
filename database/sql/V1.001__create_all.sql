/*
TODO:
    - add a global "Audit" table to leave audit_key in all tables for metadata info such as source-sys, even creation (but not update ?), ... could be used to manage load process as well

*/



-------------------------------------- Schema creation -------------------------------------
--It seems flyway picks up and create the schema automatically from table prefix!
--create schema staging;
--create schema integration;
--create schema delivery;


-------------------------------------- Staging layer -------------------------------------

-- Goals:   - Layer where raw data is loaded straight so remaining processes is done by DB-engine (ELT)
--            using sql and possible stored proc with minimum resorting to external app process

--reviews per source better...
-------------------------------------- Staging layer -------------------------------------

create table staging.load_audit (
    id serial primary key,
    batch_job varchar(100),
    status varchar(500),  -- will store err msg when fail, other wise OK for each step.
    step_name varchar(50),
    start_dts timestamp,
    finish_dts timestamp,
    rows_impacted int,
    period_begin date,
    period_end date,   --excluded (12/11/15 -> 11/11/15 23:59:59)
);

comment on table staging.load_audit is 'Metadata used to manage each job that insert/update batch of records';


create table staging.review (
        id serial primary key,
		hostname varchar(50) not null,
		site_logical_name varchar(50),
        reviewer_pseudo varchar(25) not null,
        reviewer_uid varchar(50),
		review_rating varchar(10) not null,
        review_date varchar(50) not null,
        review_text varchar(500),
		book_title varchar(200) not null,
		book_lang varchar(3) not null,
        book_uid varchar(100),
        book_isbn varchar(40),
		derived_title_sform varchar(200),
		derived_book_id uuid,   -- to ease the integgration bit, we derive and store the uuid hash during the load here
		derived_review_date date,
		loading_dts timestamp,
		load_audit_id int   -- to link to the Load_Audit metadata table
);

comment on table staging.review is 'Review data scraped from website with derived data done in app-layer (ex. derived_title_sform)';
comment on column staging.review.book_uid is 'Unique identifier of the book as used by website';


create table staging.rejected_review (
        id serial primary key,
		hostname varchar(50) not null,
        reviewer_pseudo varchar(25) not null,
        reviewer_uid varchar(50),
		review_rating varchar(10) not null,
        review_date varchar(50) not null,
        review_text varchar(500),
		book_title varchar(200) not null,
		book_lang varchar(3) not null,
        book_uid varchar(100),
        book_isbn varchar(40),
		derived_title_sform varchar(200),
		derived_review_date timestamp,
		loading_dts timestamp,
		load_audit_id int   -- to link to the Load_Audit metadata table
)


comment on table staging.review is 'Review record not loaded into integration layer (ex. because book data could not be linked) and store for future processing';



-------------------------------------- Integration layer -------------------------------------

-- Principles:  - goal is to capture 1) all web scraper app data as-is without applying any business rules
--              - 2) add business-integration: some transformation to integrate/harmonize/standardize data (key integration, dedup...)
--              - should not try to store all parameters, config stuff here (these can be managed at app level)
--

-------------------------------------- Integration layer -------------------------------------


create table integration.site (
    id int primary key,
    logical_name varchar(50) unique,
    status varchar(10),
    create_dts timestamp not null
);

comment on table integration.site is 'Website with book reviews scraped.  Design as Anchor model for greater flexibility (only a few expected)';
comment on column integration.site.logical_name is 'Name defined independently from evolving domain/url and used as a lookup for site_id';
comment on column integration.site.status is 'Flag used to get status for sites';


create table integration.site_identifier (
    site_id int not null,
    hostname varchar(100) not null,
    full_url varchar(250),
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    primary key (site_id, valid_from),
    foreign key (site_id) references integration.site(id) on delete cascade
);

comment on table integration.site_identifier is 'Natural-key <hostname> is decoupled from site_id to accommodate change in time';
comment on column integration.site_identifier.hostname is 'Hostname such as www.amazon.fr, www.amazon.com, www.thelibrary.fr';


create table integration.language (
    lang_code varchar(2) primary key,
    lang_name varchar(30),
    --add more info and code , etc...
    create_dts timestamp
);

comment on table integration.language is 'Language look-up using immutable language_code (ISO?) as PK';

/*
--Choice for Book row definition:
--1- one row per distinct value of title_sform resulting after conversion of titles scrapped in websites (using fct: convert_to_sform)
--2- one row per distinct book

-- 1: is easier to implement but will result in duplicates to be managed downstream
-- 2: involves more managed/controlled integration workflow (potentionally involving manual interactions) to avoid generating duplicates originating from slight differences in title  spelling)

-- For now: let's use #2, so Book will be fed from different jobs, and no REviewScaper will add new title (simply log title not found)!!

*/

create table integration.book (
    id uuid primary key,
    title_sform varchar(200) unique,
    lang_code varchar(2) not null,
    source_site_id int not null,
    create_dts timestamp,
    load_audit_id int
);

comment on table integration.book is 'Book reviewed as a single piece of "work" identified by its offical title (regardless of editions)';
comment on column integration.book.id is 'Primary id generated by the MD5 hashing of title_sform';
--Ignore title collision as book title are copyright so conflict should be very rare.
comment on column integration.book.title_sform is 'Std title form with capitalisation and (redundant) blanks replaced by single dash: -';
comment on column integration.book.lang_code is 'Book and reviews in different languages considered distinctly to accounted for cultural and language specific';
comment on column integration.book.source_site_id is 'Site for which a first review was loaded for that book';



create table integration.book_detail (
    book_id uuid,
    category varchar(100),
    nb_pages int,  --this could vary by editions
    editor varchar(100),
    load_audit_id int
    --etc...
);


create table integration.author (
    id serial primary key,
    create_dts timestamp,
    load_audit_id int
);


create table integration.book_edition (
    book_id uuid not null,
    ean_13 bigint not null,
    isbn_13 varchar(13) not null,
    isbn_10 varchar(10),
    load_audit_id int,
    primary key (book_id, ean_13),
    foreign key (book_id) references integration.book(id) on delete cascade
);


comment on table integration.book_edition is 'Different editions of a book for different format (paperback, ebook, pocket, ..) or country';
comment on column integration.book_edition.ean_13 is 'Numerical representation norm of ISBN-13 (ex. 9782868890061 )';
comment on column integration.book_edition.isbn_13 is 'ISBN text representation (ex. 978-2-86889-006-1 )';


create table integration.reviewer (
    id uuid primary key,
    site_id int not null,
    pseudo varchar(100) not null,
    last_seen_date date,
    create_dts timestamp not null,
    load_audit_id int,
    foreign key (site_id) references integration.site(id) on delete cascade
);

comment on table integration.reviewer is 'Reviewer entity identified from website and pseudo';
comment on column integration.reviewer.id is 'Primary-key generated by MD5 hashing of string <site_id|pseudo> (1-to-1 map function)';
--here, If I wanted to allow for change in pseudo, need to externalize a reviewer_identifer table for natural-key tracking.
--maybe safer... as some site would probably allow for change in pseudo and link back original reviews
-- howver this poses the issue of how to recognise and link previous to new pseudo ..most likekly in the website,
-- there will no longer be any info/data attached to previous one, and a new one will be created  but may be hard to map ?!?
-- could be easier simply to expire the previous one and re-scrap the new one entirely.

create table integration.reviewer_info (
    reviewer_id uuid,
    full_name varchar(100),
    birthdate date,
    any_other_demogr varchar(100),
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (reviewer_id, valid_from),
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade
);

--Potential usage : here I could use reviewer info to find "same-as" reviewer from different site
create table integration.reviewer_sameas (
    reviewer_id uuid not null,
    same_reviewer_id uuid not null,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (reviewer_id, same_reviewer_id, valid_from),
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade,
    foreign key (same_reviewer_id) references integration.reviewer(id) on delete cascade
);


create table integration.review (
    id bigserial primary key,
    book_id uuid not null,
    reviewer_id uuid not null,
    rating_code varchar(20) not null,
    review_date date not null,
    create_dts timestamp,
    load_audit_id int,
    foreign key (book_id) references integration.book(id) on delete cascade,
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade
);

--for now this table is not a-la-DV style.  It contains rating to be loaded after it can no longer be updated from reviewer (ex. some site offering update has an expiration date to allow for these updates..)
comment on table integration.review is 'A review done by one reviewer for a given book';


create table integration.book_site_review (
    book_id uuid not null,
    site_id int not null,
    book_uid varchar(200) not null,
    title_text varchar(200) not null,
    last_seen_date timestamp,
    create_dts timestamp,
    load_audit_id int,
    primary key (book_id, site_id),
    foreign key (book_id) references integration.book(id) on delete cascade,
    foreign key (site_id) references integration.site(id) on delete cascade
);

comment on table integration.book_site_review is 'Book with review scrapped by site';
comment on column integration.book_site_review.book_uid is 'Book identifier managed by website (ex. critiqueslibres used integer)';
comment on column integration.book_site_review.title_text is 'Title in website as they appear, except for removal of any leading/trailing blanks';




create table integration.rating_def (
    code varchar(10) not null,
    description varchar(100),
    class varchar(50) not null,
    value int not null,
    normal_value int not null,
    create_dts timestamp,
    load_audit_id int,
    primary key (code, class)
);

comment on table integration.rating_def is 'Simple rating code with its hierarchy class';



---------------------- View -------------------------

-- View that return how many reviews already persisted per website logical name (i.e. defined in site_info.logical_name)

create or replace view integration.reviews_persisted_lookup  as
    select   logical_name
            ,book_uid
            ,1 as one_review
    from integration.book_with_review b
    join integration.site s on (s.id = b.site_id)
    join integration.review r on (r.book_id = b.book_id);

comment on view integration.reviews_persisted_lookup is 'Report the number of reviews currently persisted by website (logical_name)' ;




-------------------------------------- Delivery layer -------------------------------------

-- Goals:   - Layer where data is exposed for consumption by external users
--          - the main delievery is the sparse Matrix... so this is easily/efficiently stored in relation representation
--          - see other altenative solutions?? for greater flexibility




-------------------------------------- Delivery layer -------------------------------------
