

-------------------------------------- Schema creation -------------------------------------
--It seems flyway picks up and create the schema automatically from table prefix!
--create schema staging;
--create schema integration;
--create schema delivery;


-------------------------------------- Staging layer -------------------------------------

-- Goals:   - Layer where raw data is bulk loaded straight from source, so remaining
--            integration steps done by DB-engine (ELT).
-------------------------------------- Staging layer -------------------------------------

create table staging.load_audit (
    id serial primary key,
    batch_job text,
    step_name text,
    step_no int,
    status text,
    run_dts timestamp,
    elapse_sec int,
    rows_impacted int,
    output text
);

comment on table staging.load_audit is 'Metadata to report on running batch_job/steps';
comment on column staging.load_audit.status is 'Status of step';
comment on column staging.load_audit.run_dts is 'Timestamp when step run (useful for things like limiting harvest period)';
comment on column staging.load_audit.output is 'Output produced by a step like error msg when failure or additional info';


create or replace view staging.handy_load_audit as
    select id, batch_job, step_name, status, run_dts, rows_impacted
    from staging.load_audit order by 1;

-- no primary key constraint since there are duplicates <work-id,isbn> in xml feed!
create table staging.thingisbn (
    work_refid bigint,
    isbn_ori text,
    isbn13 char(13),
    isbn10 char(10),
    loading_dts timestamp
);

comment on table staging.thingisbn is 'Data from thingISBN.xml to load occasionally to refresh reference work/isbn data';


create table staging.review (
    id bigserial primary key,
    site_logical_name text not null,
    username text,
    user_uid text,
    rating text,
    review text,
    review_date text,
    review_lang char(3),
    likes text,
    work_refid bigint not null,
    dup_refid text,
    work_uid text,
    book_title text,
    book_author text,
    parsed_review_date date,
    parsed_rating text,
    loading_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table staging.review is 'Review and/or Rating harvested from site';
comment on column staging.review.work_refid is 'Unique identifier of the book (a piece of work) as referenced in lt';
comment on column staging.review.dup_refid is 'Duplicate id associated to a unique "master" work_refid (duplicates exist in lt)';
comment on column staging.review.work_uid is 'Work id used in other site; to map with lt''s work_refid during harvest';
comment on column staging.review.parsed_review_date is 'Spider knows how to parse date from raw string';
comment on column staging.review.likes is 'Nb of users having appreciated the review (concetp likes, or green flag). Implies incremental-update the review';


create or replace view staging.handy_review as
    select id, work_refid, dup_refid, work_uid, site_logical_name, username, user_uid, rating,  load_audit_id
    from staging.review order by 1;





--ex. of book with multiple authors:
-- https://www.librarything.com/work/26628
-- https://www.librarything.com/work/989447
-- https://www.librarything.com/work/5072307
-- https://www.librarything.com/work/2156700

-- taken from .../work/####/details (to get correct title) and ok since only 1 ISBN taken here: from <meta property="books.isbn" content="xxxxxxxx">

create table staging.work_ref (
    work_refid bigint unique,
    title text,
    original_lang text,
    authors_name text[],
    authors_disamb_id text[],
    isbn varchar(13),
    lc_subjects text[],
    ddc_mds text[],
    load_audit_id int not null,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table staging.work_ref is 'Staging for reference <static> features harvested from work';


create table staging.work_update (
    id serial primary key,
    work_refid bigint unique,
    tags text[],
    tags_freq int[],
    popularity int,
    load_audit_id int not null,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table staging.work_update is 'Staging for evolving features harvested from work';


-------------------------------------- Integration layer -------------------------------------

-- Principles:  - 1) raw-layer: harvest and integrate website data as-is without applying any business rules
--              - 2) business-layer: apply some transformation to integrate/harmonize/standardize data (key integration, dedup...)
--

-------------------------------------- Integration layer -------------------------------------


create table integration.site (
    id int primary key,
    logical_name text unique,
    status text,
    create_dts timestamp not null
);

comment on table integration.site is 'Website with book reviews scraped model as hub for greater flexibility';
comment on column integration.site.logical_name is 'Name for lookup site_id and defined independently from evolving domain/url';
comment on column integration.site.status is 'Flag used to get status for sites';


create table integration.site_identifier (
    site_id int not null,
    hostname text not null,
    full_url text,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    primary key (site_id, valid_from),
    foreign key (site_id) references integration.site(id) on delete cascade
);

comment on table integration.site_identifier is 'Natural key, hostname, is decoupled from site_id to accommodate change in time';
comment on column integration.site_identifier.hostname is 'Hostname such as www.amazon.fr, www.amazon.com, www.thelibrary.fr';


create table integration.language (
    code char(3) primary key,  --MARC code
    code3 char(3) unique,
    code3_term char(3) unique,
    code2 char(2) unique,
    english_name text,
    english_iso_name text,
    french_name text,
    french_iso_name text,
    create_dts timestamp
);

comment on table integration.language is 'Language immutable reference';
comment on column integration.language.code is 'Primary key using MARC code (same as ISO 639-2 alpha-3 bibliographic code)';
comment on column integration.language.code3 is 'The ISO 639-2 alpha-3 bibliographic code (originally sourced from MARC code)';
comment on column integration.language.code3_term is 'The ISO 639-2 alpha-3 terminology code';
comment on column integration.language.code2 is 'The ISO 639-1 alpha-2 code (subset of alpha-3)';





------------- Data curated by librarians from lt .... ---------------

--Batch loaded from ISBNthing source

create table integration.work (
    refid bigint primary key,
    last_harvest_dts timestamp,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work is 'Book as a single piece of work irrespective of translations, editions and title sourced from lt (taken as refernece master data)';
comment on column integration.work.refid is 'Work identifer created, curated and managed by lt';

create table integration.work_info (
    work_refid bigint primary key,
    title text,
    original_lang char(3),
    update_dts timestamp,
    create_dts timestamp,
    load_audit_id int,
    foreign key (work_refid) references integration.work(refid),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_info is 'Attribute data related to a Work (unhistorized Satellite-type, could add history if need be)';


create table integration.work_sameas (
    work_refid bigint,
    master_refid bigint,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid, master_refid),
    foreign key (work_refid) references integration.work(refid),
    foreign key (master_refid) references integration.work(refid),
    foreign key (load_audit_id) references staging.load_audit(id)
);


comment on table integration.work_sameas is 'Different work_refid may exist in lt for same "master" Work';
comment on column integration.work_sameas.master_refid is 'The "master" work that work_refid refers to';


create table integration.isbn (
    ean bigint primary key,
    isbn13 char(13) not null,
    isbn10 char(10),
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.isbn is 'ISBNs associated to Work sourced from lt (used to relate reviews between site)';
comment on column integration.isbn.ean is 'Ean used as primary-key is simply the numerical representation of isbn13';



create table integration.isbn_info (
    ean bigint primary key,
    book_title text,
    lang_code char(2),
    source_site_id int,
    load_audit_id int,
    foreign key (ean) references integration.isbn(ean),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.isbn_info is 'Attribute data related to ISBN (un-historized Satellite-type, could add history if need be)';



create table integration.isbn_sameas (
    ean bigint not null,
    ean_same bigint not null,
    primary key (ean, ean_same),
    load_audit_id int,
    foreign key (ean) references integration.isbn(ean),
    foreign key (ean_same) references integration.isbn(ean),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.isbn_sameas is 'Association between isbn of SAME work taken from lt';
comment on column integration.isbn_sameas.ean is 'A reference ean taken arbitrarily from the set of same ean''s';
comment on column integration.isbn_sameas.ean_same is 'Ean considered same as the reference ean (including the reference itself)';


create table integration.work_isbn (
    work_refid bigint,
    ean bigint,
    source_site_id int,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid, ean),
    foreign key(work_refid) references integration.work(refid),
    foreign key(ean) references integration.isbn(ean),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_isbn is 'Association of work and ISBN (sourced from lt)';


create table integration.author (
    id uuid primary key,
    disamb_name text unique,
    first_name text,
    last_name text,
    legal_name text,
    gender char(1),
    nationality text,
    birth_year smallint,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.author is 'Author sourced from lt';
comment on column integration.author.id is 'Identifier derived from MD5 hashing of disamb_name';
comment on column integration.author.disamb_name is 'Unique and disambiguation name given by url in lt: /author/lnamefname-x';


create table integration.work_author (
    work_refid bigint,
    author_id uuid,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid, author_id),
    foreign key (work_refid) references integration.work(refid),
    foreign key (author_id) references integration.author(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_author is 'Association between Work and its author(s), sourced from lt';





------------- Data from Reviews, tag, list harvested .... ---------------


-- to be inserted ONLY by other site following harvesting activity
create table integration.work_site_mapping(
    work_refid bigint not null,
    work_uid text not null,
    site_id int not null,
    last_harvest_dts timestamp not null,
    book_title text,
    book_lang text,
    book_author text,
    create_dts timestamp,
    load_audit_id int,
    primary key(work_refid, work_uid, site_id),
    foreign key(work_refid) references integration.work(refid),
    foreign key(site_id) references integration.site(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_site_mapping is 'Map between work ref_id in lt and id used in other site';
comment on column integration.work_site_mapping.work_refid is 'Reference work id used in lt';
comment on column integration.work_site_mapping.work_uid is 'Id used in  other site';
comment on column integration.work_site_mapping.last_harvest_dts is 'Last time work was harvested';
comment on column integration.work_site_mapping.book_title is 'Book title, author, lang are for QA purposes (mapping between sites done through isbn(s) lookup)';



create table integration.tag (
    id uuid primary key,
    tag text unique,
    lang_code char(2),
    tag_upper text,
    source_site_id int,
    create_dts timestamp,
    load_audit_id int
);

comment on column integration.tag.id is 'Tag unique id derived from md5 hashing of tag';
comment on column integration.tag.tag is 'Tag text taken verbatim';
comment on column integration.tag.tag_upper is 'Tag capitalized, useful for aggregating similar tag';


create table integration.work_tag (
    work_refid bigint,
    tag_id uuid,
    source_site_id int,
    frequency int,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid, tag_id, source_site_id),
    foreign key (work_refid) references integration.work(refid),
    foreign key (tag_id) references integration.tag(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);


comment on table integration.work_tag is 'Tag assigned to Work (for now, use lt but could add source later on)';
comment on column integration.work_tag.frequency is 'Frequency indicating the importance of the tag for the associated work on the site';


create table integration.user (
    id uuid primary key,
    user_uid text,
    site_id int,
    username text,
    last_seen_date date,
    create_dts timestamp,
    load_audit_id int,
    unique (user_uid, site_id),
    foreign key (site_id) references integration.site(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.user is 'User having contributed in some way to a site (submitted rating, review, list own''s book, etc..)';
comment on column integration.user.id is 'Primary-key generated by MD5 hashing of concat(site_logical_name, username)';
comment on column integration.user.user_uid is 'Some site may use system-generated id (otherwise, this is same as username)';

--here, member should be identified with the most stable id (ex. gr shows fname-lname in page but these can be changed, so
--must find alternative.
--
-- however this poses the issue of how to recognise and link old to new pseudo ..
-- will there be any info/data attached to old one, ...  may be hard to map ?!?
-- could be easier simply to expire the previous one and re-build the new one entirely.


create table integration.user_info (
    user_id uuid,
    full_name text,
    birthdate date,
    any_other_demo text,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (user_id, valid_from),
    foreign key (user_id) references integration.user(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);




--Some sites does not restrict users reviewing many times same book, either remove constraint or adapt insert stmt
-- and other have identical duplicates of review (ex. lt w=12990555)
-- so for now, we don't impose  unique (work_refid, user_id), to be taken care of downstream

create table integration.review(
    id bigserial primary key,
    work_refid bigint not null,
    user_id uuid not null,
    site_id int not null,
    rating text,
    parsed_rating int,
    review text,
    review_date date,
    review_lang char(3),
    create_dts timestamp,
    load_audit_id int,
    foreign key (work_refid) references integration.work(refid),
    foreign key (user_id) references integration.user(id),
    foreign key (site_id) references integration.site(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.review is 'Review and/or rating done for a Work in a specific language by a user ';
comment on column integration.review.user_id is 'User identifier derived from MD5 hashing of username, site (ref. integration.derive_userid)';



-- A review must be attached to an edition (i.e. = a book = an isbn) so that we get the language info ...
-- goodreads has reviews by a book (given a Goodreads book-id. : api../book/show) or by isbn (api../book.show_by_isbn)
-- but the web page combine reviews for the same work (for all editions) so difficult
-- book.isbn_to_id gets the gr's book-id give the isbn.
--
--create table integration.book (
--    id uuid primary key,
--    title_sform text not null,
--    author_sform text not null,
--    lang_code char(2) not null,
--    title text,
--    source_site_id int,
--    create_dts timestamp,
--    load_audit_id int,
--    unique (title_sform, lang_code),
--    foreign key (load_audit_id) references staging.load_audit(id)
--);
--
--comment on table integration.book is 'All distinct title-author-lang found from harvesting book data from site (reviews, list, tag..)';
--comment on column integration.book.id is 'Primary-id generated by MD5 hashing of natural key, see plsql function' ;
--comment on column integration.book.title_sform is 'Std title form in capital with redundant blanks removed, see function';
--comment on column integration.book.author_sform is 'Std author form as (Lname, Fname), see function';
--comment on column integration.book.lang_code is 'Books in diff language considered distinct';
--comment on column integration.book.source_site_id is 'Web site where the book is first harvested';





-- to find "same-as" user from different site (could use review text to spot these!!)
-- some user writes their reviews on diff site
create table integration.user_sameas (
    user_id uuid not null,
    same_user_id uuid not null,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (user_id, same_user_id, valid_from),
    foreign key (user_id) references integration.user(id) on delete cascade,
    foreign key (same_user_id) references integration.user(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);




create table integration.rating_def (
    code text not null,
    description text,
    class text not null,
    value int not null,
    normal_value int not null,
    create_dts timestamp,
    load_audit_id int,
    primary key (code, class),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.rating_def is 'Simple rating code with its hierarchy class';



---------------------- View -------------------------

-- View that return how many reviews already persisted per site (logical name)


--create or replace view integration.reviews_persisted_lookup  as
--    select   s.logical_name
--            ,r.work_refid
--            ,r.lang_code
--            ,count(*) as nb_review
--    from integration.review r
--    join integration.site s on (s.id = r.site_id)
--    group by 1,2,3
--    ;
--
--comment on view integration.reviews_persisted_lookup is 'Report of #Review by work, lang and site currently persisted' ;





---------------------- Function -------------------------

CREATE OR REPLACE function integration.get_sform(input_str text) returns text as $$
    DECLARE
        trim_space text;
    BEGIN
        trim_space := REGEXP_REPLACE( trim(input_str), '\s+', ' ', 'g');
        return upper( trim_space );
    END;
$$ LANGUAGE plpgsql;

-- Used to derive a unique id based on the string-key used by sites (as used in work_site_mapping)
CREATE OR REPLACE function integration.derive_other_work_id
                    (work_ori_id text, site_logical_name text) returns uuid as $$
    BEGIN
        return cast( md5( concat(work_ori_id, site_logical_name) ) as uuid);
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE function integration.standardize_authorname
                    (fname text, lname text) returns text as $$
    BEGIN
        return  concat( upper(lname), ', ', upper(fname) );
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE function integration.derive_userid
                    (userid text, site_logical_name text) returns uuid as $$
    BEGIN
        return cast( md5( concat(userid, site_logical_name) ) as uuid);
    END;
$$ LANGUAGE plpgsql;





-------------------------------------- Delivery layer -------------------------------------

-- Goals:   - Layer where data is exposed for consumption by external users
--          - the main delievery is the sparse Matrix... so this is easily/efficiently stored in relation representation
--          - see other altenative solutions?? for greater flexibility




-------------------------------------- Delivery layer -------------------------------------
