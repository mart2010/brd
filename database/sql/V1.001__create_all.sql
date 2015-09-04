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

-- Goals:   - Layer where raw data is loaded as fast as possible, so that ETL process can leverage
--            as much RDBMS functions with minimum resorting to external app process

--reviews per source better...
-------------------------------------- Staging layer -------------------------------------

create table staging.reviews (
        id bigserial primary key,
		hostname varchar(100),
        reviewer_pseudo varchar(100),
		review_rating_code varchar(20),
		book_siteid int,
		book_title_text varchar(200),
		book_title_sform varchar(200),
        review_dts timestamp,
        --here maybe add a bnch of generic fields? maybo not worth it as data here is volatile (no history kept)
		loading_dts timestamp,
		batch_id int   -- to link to a Audit_metadata table
);


--note: critiqueslibres is loaded in 2 stages:
-- 1) flag all newly reviewed books from global list: take all books with nb_reviews > 1, either not yet staged or having new reviews (staging.nb_review is less)
-- 2) for all flag books, fetch reviews dating inside logical date period : LOAD_START_DATE -> LOAD_END_DATE (could be date of review or date of review with no more edit possible)

create table staging.critiqueslibres_lookup (
        bookid_site int,                   -- book-id as used by the site
        title  varchar(200) unique,        -- as found in website
        title_sform varchar(200),          -- transform form from title
        title_sform_cor varchar(200),      -- the corrected std form if_sform lead to an unrecognized uuid
        book_id uuid,    -- the lookup form must match book table, can easily spot issue with the _sform when uuid is NULL (to correct manually/automatically)
        nb_reviews int,
        creation_dts timestamp,
        update_dts timestamp
);

comment on table staging.critiqueslibres_lookup is 'Used to find new/update book/reviews to load (having 2 or more reviews) with the title_sform';




-------------------------------------- Integration layer -------------------------------------

-- Principles:  - should not try to store all parameters, config stuff here (these can be managed at app level)
--              - goal is to capture all elements tracked down by the scrapy app, as-is without applying any business rules
--              - also, here we could add business-integration:  add rules and transfor for data harmoinzation, standardisation...
--              - or else, we could have a separate/dedicated layer for that needs?

-------------------------------------- Integration layer -------------------------------------



create table integration.site (
    id int primary key,
    last_seen_date date,
    creation_dts timestamp not null
);

comment on table integration.site is 'Website with book reviews scraped.  Design as Anchor model for greater flexibility (only a few of these expected anyway)';
comment on column integration.site.last_seen_date is 'Flag used for sites that have disappeared';

create table integration.site_identifier (
    site_id int not null,
    hostname varchar(100) not null,
    valid_from timestamp not null,
    valid_to timestamp,
    creation_dts timestamp,
    update_dts timestamp,
    primary key (site_id, valid_from),
    foreign key (site_id) references integration.site(id) on delete cascade
);

comment on table integration.site_identifier is 'Natural-key <hostname> is decoupled from site_id to accommodate change in time';
comment on column integration.site_identifier.hostname is 'Hostname such as www.amazon.fr, www.amazon.com, www.thelibrary.fr';

create table integration.site_info (
    site_id int not null,
    url varchar(250),
    --other info..
    valid_from timestamp not null,
    valid_to timestamp,
    creation_dts timestamp,
    update_dts timestamp,
    primary key (site_id, valid_from),
    foreign key (site_id) references integration.site(id) on delete cascade
);


create table integration.language (
    lang_code varchar(3) primary key,
    lang_name varchar(30),
    --add more info and code , etc...
    creation_dts timestamp
);

comment on table integration.language is 'Language look-up using immutable language_code (ISO?) as PK';


create table integration.book (
    id uuid primary key,
    title_sform varchar(200) unique,
    title_text varchar(200) unique,
    lang_code varchar(3),
    creation_dts timestamp
);

comment on table integration.book is 'Book reviewed as a single piece of "work" and having possibly different editions';
comment on column integration.book.id is 'Primary id generated by the MD5 hashing of title_sform';
--exact rule to transform title_text to title_sform should be stored somewhere
comment on column integration.book.title_sform is 'Capitalised and std form of title: remove one or more blanks by a single dash: -';
comment on column integration.book.lang_code is 'Book and reviews in different languages considered distinctly to accounted for cultural and language specific';



create table integration.book_detail (
    book_id uuid,
    category varchar(100),
    nb_pages int,  --this could vary by editions
    editor varchar(100)
    --etc...
);


create table integration.author (
    id serial primary key,
    creation_dts timestamp
);



create table integration.book_edition (
    book_id uuid not null,
    ean_13 bigint not null,
    isbn_13 varchar(13) not null,
    isbn_10 varchar(10),
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
    creation_dts timestamp not null,
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
    creation_dts timestamp,
    update_dts timestamp,
    primary key (reviewer_id, valid_from),
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade
);

--here I could use reviewer info to find "same-as" reviewer from different site
create table integration.reviewer_sameas (
    reviewer_id uuid not null,
    same_reviewer_id uuid not null,
    valid_from timestamp not null,
    valid_to timestamp,
    creation_dts timestamp,
    update_dts timestamp,
    primary key (reviewer_id, same_reviewer_id, valid_from),
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade,
    foreign key (same_reviewer_id) references integration.reviewer(id) on delete cascade
);


create table integration.review (
    id bigserial primary key,
    book_id uuid not null,
    reviewer_id uuid not null,
    rating_code varchar(20) not null,
    last_seen_date date,
    creation_dts timestamp,
    foreign key (book_id) references integration.book(id) on delete cascade,
    foreign key (reviewer_id) references integration.reviewer(id) on delete cascade
);

--for now this table is not a-la-DV style.  It contains rating that should be ingested after it can no longer be updated from user (ex. some site offering update has an expiration date to allow for these updates..)
comment on table integration.review is 'An association representing review for one book given by one reviewer';


create table integration.rating_def (
    code varchar(10) not null,
    description varchar(100),
    class varchar(50) not null,
    value int not null,
    normal_value int not null,
    creation_dts timestamp,
    primary key (code, class)
);

comment on table integration.rating_def is 'Simple rating code with its hierarchy class';






-------------------------------------- Delivery layer -------------------------------------

-- Goals:   - Layer where data is exposed for consumption by external users
--          - the main delievery is the sparse Matrix... so this is easily/efficiently stored in relation representation
--          - see other altenative solutions?? for greater flexibility




-------------------------------------- Delivery layer -------------------------------------
