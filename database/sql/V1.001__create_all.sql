-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-------------------------------------- Schema creation -------------------------------------
--It seems flyway picks up and create the schema automatically from table prefix!
--create schema staging;
--create schema integration;
--create schema delivery;


------------------------------------------ Staging layer -----------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Goals:   - Layer where raw data is bulk loaded straight from source, so remaining
--            integration steps done by DB-engine (ELT).
--
--------------------------------------------------------------------------------------------------------
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
    dup_refid bigint,
    work_uid text,
    title text,
    authors text,
    tags_t text,
    tags_n text,
    tags_lang varchar(3),
    parsed_review_date date,
    parsed_rating text,
    parsed_likes int,
    parsed_dislikes int,
    loading_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table staging.review is 'Review and/or Rating harvested from site';
comment on column staging.review.work_refid is 'Unique identifier of book (piece of work as ref to lt)';
comment on column staging.review.dup_refid is 'Duplicate id associated to a "master" work_refid (duplicates exist in lt)';
comment on column staging.review.work_uid is 'Work id used iby other site; to map with lt''s work_refid during harvest';
comment on column staging.review.parsed_review_date is 'Parsed date from raw string';
comment on column staging.review.likes is 'Nb of users liking the review (concept such as likes, green flag)';

create or replace view staging.handy_review as
    select id, work_refid, dup_refid, work_uid, site_logical_name, username, user_uid, rating, parsed_rating, review_date, likes, parsed_likes
    from staging.review order by 1;


create table staging.thingisbn (
    work_refid bigint,
    isbn_ori text,
    isbn13 char(13),
    isbn10 char(10),
    loading_dts timestamp
);
comment on table staging.thingisbn is 'Data from thingISBN.xml to refresh reference work/isbn data (No PK, as duplicates of <work_id,isbn> exist in source)'

create table staging.work_info (
    work_refid bigint unique,
    dup_refid bigint,
    title text,
    original_lang text,
    ori_lang_code char(3),
    authors text,
    authors_code text,  --used in lt for disambiguation
    mds_code text,
    mds_code_corr text,
    mds_text text,
    lc_subjects text,
    popularity text,
    other_lang_title text,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table staging.work_info is 'Staging for reference data harvested from work';


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


create table staging.isbn_lang (
    ean bigint unique,
    lang_code char(3),
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);


-- these should be harvested/refreshed during incremental reviews...(so could add these into staging.review)
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



------------------------------------------ Integration layer -------------------------------------------
--------------------------------------------------------------------------------------------------------
-- Two sub-layers:
--          - 1) Raw sub-layer: untransformed data from source without applying business rules
--          - 2) Business sub-layer: apply some transformation to help preparing for presentation layer
--                    2.1) de-duplication (same_as for work, user, review, etc...)
--                    2.2) any sort of standardization/harmonization...
--
--------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------
-------------------------------------- Raw Sub-layer -------------------------------------
------------------------------------------------------------------------------------------
create table integration.site (
    id int primary key,
    logical_name text unique,
    status text,
    create_dts timestamp not null
);
comment on table integration.site is 'Website of book reviews';
comment on column integration.site.logical_name is 'Name used as business key independent of evolving domain/url';
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
    code char(3) primary key,
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

create table integration.work (
    refid bigint primary key,
    last_harvest_dts timestamp,
    last_seen_date date,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work is 'Book as a single piece of work irrespective of translations, editions and title sourced from lt (taken as refernece master data)';
comment on column integration.work.refid is 'Work identifer created, curated and managed by lt';
comment on column integration.work.last_seen_date is 'Last time this work was seen during a thingisbn bulkload';

create table integration.work_info (
    work_refid bigint primary key,
    title text,
    original_lang char(3),
    popularity int,
    mds_code text,
    mds_code_ori text,
    lc_subjects text,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (work_refid) references integration.work(refid),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_info is 'Attribute data related to a Work (unhistorized Satellite-type, could add history if need be)';
comment on column integration.work_info.mds_code is 'mds code without dot and truncated to align with mds_text, same as integration.mds(code)';
comment on column integration.work_info.mds_code_ori is 'The mds original code as found from lt';


create table integration.isbn (
    ean bigint primary key,
    isbn13 char(13) not null,
    isbn10 char(10),
    last_seen_date date,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.isbn is 'ISBNs associated to Work sourced from lt (used to relate reviews between site)';
comment on column integration.isbn.ean is 'Ean used as primary-key is simply the numerical representation of isbn13';

create table integration.isbn_info (
    ean bigint primary key,
    book_title text,
    lang_code char(3),
    source_site_id int,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (ean) references integration.isbn(ean),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.isbn_info is 'Attribute data related to ISBN (un-historized Satellite-type, could add history if need be)';

create table integration.work_isbn (
    ean bigint primary key,
    work_refid bigint,
    last_seen_date date,
    deletion_date date,
    create_dts timestamp,
    load_audit_id int,
    foreign key(ean) references integration.isbn(ean),
    foreign key(work_refid) references integration.work(refid),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_isbn is 'Association of work and ISBN (sourced from lt)';
comment on column integration.work_isbn.ean is 'The ISBN13 code in numerical, defined as PK so one ean only associated to 1 work';

create table integration.author (
    id uuid primary key,
    code text unique,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.author is 'Author sourced from lt';
comment on column integration.author.id is 'Identifier derived from MD5 hashing of code';
comment on column integration.author.code is 'Unique and disambiguation code given by lt: /author/lnamefname-x';

create table integration.author_info (
    author_id uuid primary key,
    name text,
    legal_name text,
    gender char(1),
    nationality text,
    birth_year smallint,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (author_id) references integration.author(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.author_info is 'Author info sourced from lt';

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

create table integration.work_title (
    work_refid bigint,
    lang_code char(3),
    lang_text text,
    title text,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid, lang_text),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_title is 'Title of work in different language edition';
comment on column integration.work_title.lang_text is 'Language is used as PK and preserve so we spot missing language ref';


---- MDS has issues for long mds oce where a lot are 'Not set'
---- ex. Twisting in the Wind: The Murderess and the English Press has mds_code = 070.4493641523082094209034
---- but text is truncated (other item are 'Not set' : Information > Journalism And Publishing > Journalism And Publishing > Journalism > Special subjects: departments and editors > By Subject
create table integration.mds (
    code text primary key,
    code_w_dot text,
    mds_text text,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.mds is 'Melvil decimal system as of lt, but has some issues with Not-Set level';
comment on column integration.mds.code 'Code corrected (original code truncated based on levels found in text)';
comment on column integration.mds.code 'Code corrected (original code truncated based on levels found in text)';


--Still to do...
--create table integration.lc_subject (
--    subject text primary key,
--);
--comment on table integration.lc_subject is 'Library of congress subjects def';

create table integration.tag (
    id uuid primary key,
    tag text unique,
    lang_code char(3),
    tag_upper text,
    ori_site_id int,
    create_dts timestamp,
    load_audit_id int,
    foreign key(ori_site_id) references integration.site(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on column integration.tag.id is 'Tag unique id derived from md5 hashing of tag';
comment on column integration.tag.tag is 'Tag text taken verbatim';
comment on column integration.tag.ori_site_id is 'The site where tag was first harvested';
comment on column integration.tag.tag_upper is 'Tag capitalized, useful for aggregating similar tag';


create table integration.work_tag (
    work_refid bigint,
    tag_id uuid,
    source_site_id int,
    frequency int,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (work_refid, tag_id, source_site_id),
    foreign key (work_refid) references integration.work(refid),
    foreign key (tag_id) references integration.tag(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_tag is 'Tag-Work association identified by work, tag and site.  Site composes key in order to preserve relative frequency for site';
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
comment on column integration.user.id is 'Primary-key generated by MD5 hashing of concat(user_uid, site_logical_name)';
comment on column integration.user.user_uid is 'Site system-generated id to identify the user if present,(more stable than username), otherwise same as username)';
comment on column integration.user.username is 'Username or pseudo associated to the user';

create table integration.user_info (
    user_id uuid primary key,
    full_name text,
    birth_text text,
    birth_date date,
    gender char(1),
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (user_id) references integration.user(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.user_info is 'User attributes with no historical-tracking, simply keep latest value';

--The slowly changing Sat!
create table integration.user_geo (
    user_id uuid,
    location text,
    country_code char(3),
    region  text,
    city text,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (user_id, valid_from),
    foreign key (user_id) references integration.user(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.user_geo is 'User geo attributes with historical-tracking, preserving changes in time';


--The rapidly changing Sat!
create table integration.user_perso (
    user_id uuid,
    status text,
    occupation text,
    interest text,
    fav_books text,
    about_me text,
    ranking int,  --could normalize ranking for site where available
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (user_id, valid_from),
    foreign key (user_id) references integration.user(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.user_perso is 'User perso attributes with historical-tracking';


--Some sites does not restrict users reviewing many times same book
-- and other have identical duplicates of review (ex. lt w=12990555)
-- so for now, we don't impose  unique (work_refid, user_id), to be taken care of downstream
create table integration.review(
    id bigserial primary key,
    work_refid bigint not null,
    user_id uuid not null,
    site_id int not null,
    rating text,
    parsed_rating int,
    likes text,
    parsed_likes int,
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
comment on table integration.review is 'Review and/or rating done for a Work in a specific language by a user, with no PK since most sites do not restrict users from reviewing same book multiple times';
comment on column integration.review.user_id is 'User identifier derived from MD5 hashing of username, site (ref. integration.derive_userid)';


create or replace view integration.handy_review as
    select id, work_refid, user_id, site_id, review_lang, parsed_rating, review_date, load_audit_id
    from integration.review;


-----------------------------------------------------------------------------------------------
-------------------------------------- Business Sub-layer -------------------------------------
-----------------------------------------------------------------------------------------------
create table integration.work_sameas (
    work_refid bigint,
    master_refid bigint,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_refid),
    foreign key (work_refid) references integration.work(refid),
    foreign key (master_refid) references integration.work(refid),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_sameas is 'Different work_refid may exist in lt for same "master" Work';
comment on column integration.work_sameas.master_refid is 'The "master" work that work_refid refers to';


-- .. parent-child hierarchical relation
create table integration.mds_parent (
    code text,
    parent_code text,
    create_dts timestamp,
    load_audit_id int,
    primary key (code, parent_code),
    foreign key (code) references integration.mds(code),
    foreign key (parent_code) references integration.mds(code),
    foreign key (load_audit_id) references staging.load_audit(id)
);


-- Design for all site (except lt) to track down harvesting activity
create table integration.work_site_mapping(
    work_refid bigint not null,
    work_uid text not null,
    site_id int not null,
    last_harvest_dts timestamp not null,
    title text,
    book_lang text,
    authors text,
    create_dts timestamp,
    load_audit_id int,
    primary key(work_refid, work_uid, site_id),
    foreign key(work_refid) references integration.work(refid),
    foreign key(site_id) references integration.site(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);
comment on table integration.work_site_mapping is 'Map between work ref_id in lt and work_uid (id used by other site)';
comment on column integration.work_site_mapping.work_refid is 'Reference work id used in lt';
comment on column integration.work_site_mapping.work_uid is 'Id used in other site.  Value of -1 means no work found. Values -2 mans more than one id associated to same refid and not wish to tie them to the refid until reviews are found (e.g. asin for AZ) ';
comment on column integration.work_site_mapping.last_harvest_dts is 'Last time work was harvested';
comment on column integration.work_site_mapping.title is 'Book title, author, lang are for QA purposes (mapping between sites done through isbn(s) lookup)';


create table integration.review_similarto (
    rev_id bigint,
    other_rev_id bigint,
    similar_index float,
    same_work_flag boolean,
    same_author_flag boolean,
    check (other_rev_id < rev_id),
    primary key(rev_id, other_rev_id),
    foreign key(rev_id) references integration.review(id),
    foreign key(other_rev_id) references integration.review(id),
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

--could be convenient for downstream to store all pair-wise of similar review ??
comment on table integration.review_similarto is 'To track down reviews with similarity';
comment on column integration.review_similarto.rev_id is 'Review-id constraint that it is larger than other_rev_id (avoid dup pairwise comparison)'
comment on column integration.review_similarto.other_rev_id is 'The other similar review-id';


-- to
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

-- if user_id s1-u13, s3-u654 are considered the same, then this stores 2 rows (user_id, same_userid):
-- (s1-u13,s1-u13)
-- (s3-u654,s1-u13)
comment on table integration.user_sameas as 'Store "same-as" user across sites spotted when multiple reviews have very similar text (exact rules TBD)';
comment on column integration.user_sameas.user_id as 'All user_id in diff sites recognized as same user';
comment on column integration.user_sameas.same_user_id as 'Flag to identify same user_id (taken arbitrarily)';




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





-------------------------------------- Presentation layer -----------------------------------------
---------------------------------------------------------------------------------------------------
-- Goals:   - Layer where data is exposed for tools and user consumption
--          - Star schema/Datamart for analytical purposes
--          - Other delivery:  the sparse Matrix...built for recommending app
--              (efficiently stored in relation model)
---------------------------------------------------------------------------------------------------

create table presentation.fact_review (
    -- all dims

    -- all facts
    rating smallint,
    nb_likes int,
    nb_dislikes int,


);

--grain def: work level (will include diff language title atts..)
create table presentation.dim_book (
    id bigint primary key
);

--not personal info here just the high-level demographics and others..
create table presentation.dim_reviewer (
    id bigint primary key

);

--to be used in two distinct Role-playing:  Review-lang and Book-ori-lang
create table presentation.dim_language (
    id smallint primary key
);

--standardized rating info
create table presentation.dim_rating (
    id smallint primary key
);

--Review's date
create table presentation.dim_date (
    id int primary key
);

-- model review text as dim, while keeping text only (stripped from html tag/content):
-- would be ideal place for storing special extracted features or text dim-reduced representation ...)
create table presentation.dim_review_text (
    id uuid,
    review_t text,

);

-- the idea is that if I want to be able to report similar review (for account duplication, plagirism, ..)
-- then I need to preserve each review here (as opposed to clean them upstream and loose this capability in presentation layer!)
create table presentation.review_similar_to (
-- sort of dim outrigger!

);

create table presentation.dim_mds (
    id int primary key
);

--see how to best manage the many-to-many...
create table presentation.dim_tag (
    id int primary key
);


-- check out other representation for review text typical of text mining app
-- like n-grams or shingling representation (see Google’s Ngram Viewer) requiring NLP techniques such as stemming and stop word removal
-- or  chunking, i.e., light sentence parsing, to extract sequences of words likely to be meaningful phrases
--checkout pg_similarity
create table presentation.dim_text_reformat (
    id bigint primary key
);

create table presentation.dim_source (
    smallint primary key
);



