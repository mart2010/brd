

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
    start_dts timestamp,
    finish_dts timestamp,
    rows_impacted int,
    period_begin date,
    period_end date
);

comment on table staging.load_audit is 'Metadata used to manage each job that insert/update batch of records';
comment on column staging.load_audit.step_no is 'Step process order within a batch_job (used for step in pending.. status)';
comment on column staging.load_audit.status is 'Status of step or error msg when failure';
comment on column staging.load_audit.period_end is 'Excluded, 12/11/15 -> 11/11/15 23:59:59';


create table staging.review (
    id serial primary key,
    hostname text not null,
    site_logical_name text,
    username text not null,
    user_uid text,
    rating text not null,
    review text,
    review_date text not null,
    book_title text not null,
    book_uid text,
    book_lang char(2) not null,
    book_isbn_list text,   --use the array-type instead
    author_fname text,
    author_lname text,
    parsed_review_date date,
    loading_dts timestamp,
    load_audit_id int not null,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table staging.review is 'Transient Review and/or Rating taken from website as-is (i.e. with no transformation)';
comment on column staging.review.book_uid is 'Unique identifier of the book as used by website, when applicable';
comment on column staging.review.load_audit_id is 'Refers to audit of original Dump of scraped data, used to get period loaded';
comment on column staging.review.parsed_review_date is 'Spider knows how to parse date in string format';
comment on column staging.review.book_isbn_list is 'Comma separated list of isbn related to the book being reviewed';


create table staging.rejected_review as
            select * from staging.review
            with no data
;

comment on table staging.rejected_review is 'Review record not loaded into integration layer (this was useful when only one spider could load new books... and the other MUST link to exsiting..)';


-- no primary key constraint since there are duplicates <work-id,isbn> in xml feed!
create table staging.thingisbn (
    work_uid bigint,
    isbn text,
    loading_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table staging.thingisbn is 'Data from thingISBN.xml to load occasionally to refresh reference work/isbn data';




--ex. of book with multiple authors:
-- https://www.librarything.com/work/26628
-- https://www.librarything.com/work/989447
-- https://www.librarything.com/work/5072307
-- https://www.librarything.com/work/2156700

-- taken from .../work/####/details (to get correct title) and ok since only 1 ISBN taken here: from <meta property="books.isbn" content="xxxxxxxx">

create table staging.work_ref (
    id serial primary key,
    work_id bigint unique,
    title text,
    original_lang char(2),
    authors_name text[],
    authors_disamb_id text[],
    isbn varchar(13),
    lc_subjects text[],
    ddc_mds text[],
    load_audit_id int not null,
    foreign key (load_audit_id) references staging.load_audit(id)
);



create table staging_work_tag (
    id serial primary key,
    work_id bigint unique,
    tags text[],
    frequency int[],
    load_audit_id int not null,
    foreign key (load_audit_id) references staging.load_audit(id)
);



-------------------------------------- Integration layer -------------------------------------

-- Principles:  - 1) raw-layer: harvest and integrate website data as-is without applying any business rules
--              - 2) business-layer: apply some transformation to integrate/harmonize/standardize data (key integration, dedup...)
--

-------------------------------------- Integration layer -------------------------------------



------------- Data from curated by librarians from lt .... ---------------

--Batch loaded from ISBNthing source

create table integration.work (
    uid bigint primary key,
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work is 'Book as a single piece of work irrespective of translations, editions and title sourced from lt (taken as refernece master data)';
comment on column integration.work.uid is 'Work identifer created, curated and managed by lt';


create table integration.work_info (
    work_uid bigint primary key,
    title text,
    original_lang char(2),
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    foreign key (work_uid) references integration.work(uid),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_info is 'Attribute data related to a Work (unhistorized Satellite-type, could add history if need be)';


create table integration.isbn (
    isbn10 char(10) primary key,
    isbn13 char(13),   --for now, will be ignored
    create_dts timestamp,
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.isbn is 'ISBNs associated to Work sourced from lt (used to relate reviews between site)';


create table integration.isbn_info (
    isbn10 char(10) primary key,
    book_title text,
    lang_code char(2),
    source_site_id int,
    load_audit_id int,
    foreign key (isbn10) references integration.isbn(isbn10),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.isbn_info is 'Attribute data related to ISBN (unhistorized Satellite-type, could add history if need be)';



create table integration.isbn_sameas (
    isbn10 char(10),
    isbn10_same char(10),
    primary key (isbn10, isbn10_same),
    load_audit_id int,
    foreign key (load_audit_id) references staging.load_audit(id)
);


comment on table integration.isbn_sameas is 'Association between isbn of SAME work taken from lt';
comment on column integration.isbn_sameas.isbn10 is 'A reference isbn (taken arbitrarily among same isbn)';
comment on column integration.isbn_sameas.isbn10_same is 'Isbn considered same as the reference isbn13 (including the reference itself)';



create table integration.work_isbn (
    work_uid bigint,
    isbn10 char(10),
    source_site_id int,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_uid, isbn10),
    foreign key(work_uid) references integration.work(uid),
    foreign key(isbn10) references integration.isbn(isbn10),
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
    work_uid bigint,
    author_id uuid,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_uid, author_id),
    foreign key (work_uid) references integration.work(uid),
    foreign key (author_id) references integration.author(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.work_author is 'Association between Work and its author(s), sourced from lt';


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
    work_id bigint,
    tag_id uuid,
    source_site_id int,
    frequency int,
    create_dts timestamp,
    load_audit_id int,
    primary key (work_id, tag_id, source_site_id),
    foreign key (work_id) references integration.work(uid),
    foreign key (tag_id) references integration.tag(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);


comment on table integration.work_tag is 'Tag assigned to Work (for now, use lt but could add source later on)';
comment on column integration.work_tag.frequency is 'Frequency indicating the importance of the tag for the associated work on the site';




------------- Data from Reviews harvested .... ---------------


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
    code char(2) primary key,
    code3 char(3) unique,
    name text,
    create_dts timestamp
);

comment on table integration.language is 'Language immutable reference using ISO code as PK';


create table integration.user (
    id uuid primary key,
    username text,
    site_id int,
    userid text,
    last_seen_date date,
    create_dts timestamp,
    load_audit_id int,
    unique (username, site_id),
    foreign key (site_id) references integration.site(id) on delete cascade,
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.user is 'User having contributed in some way to a site (submitted rating, review, list own''s book, etc..)';
comment on column integration.user.id is 'Primary-key generated by MD5 hashing of concat(site_logical_name, username)';
comment on column integration.user.userid is 'Site-generated id possibly used to identify user (instead of username)';

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



create table integration.book (
    id uuid primary key,
    title_sform text not null,
    author_sform text not null,
    lang_code char(2) not null,
    title text,
    source_site_id int,
    create_dts timestamp,
    load_audit_id int,
    unique (title_sform, lang_code),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.book is 'All distinct title-author-lang found from harvesting book data from site (reviews, list, tag..)';
comment on column integration.book.id is 'Primary-id generated by MD5 hashing of natural key, see plsql function' ;
comment on column integration.book.title_sform is 'Std title form in capital with redundant blanks removed, see function';
comment on column integration.book.author_sform is 'Std author form as (Lname, Fname), see function';
comment on column integration.book.lang_code is 'Books in diff language considered distinct';
comment on column integration.book.source_site_id is 'Web site where the book is first harvested';


--Some sites does not restrict users reviewing more than once same book, either remove unique(book_id, reviewer_id) or adapt insert stmt
--no update done (ex. only loaded after they can no longer be updated on site)
/*
create table integration.book_review(
    id bigserial primary key,
    user_id uuid not null,
    book_id uuid not null,
    rating text not null,  --this will go in external clob...
    review text,
    review_date date,
    source_site_id int,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    unique (book_id, user_id),
    foreign key (book_id) references integration.book(id),
    foreign key (user_id) references integration.user(id),
    foreign key (load_audit_id) references staging.load_audit(id)
);

comment on table integration.book_review is 'Book reviewed and/or rated by user';


create table integration.book_user (
    id uuid primary key,
    book_id uuid not null,
    user_id uuid not null,
    source_site_id int,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    unique (book_id, user_id),
    foreign key (book_id) references integration.book(id),
    foreign key (user_id) references integration.user(id)
);

comment on table integration.book_user is 'User relationship with Book, such like lt''s collection or gr''s bookshelf';
comment on column integration.book_user.id is 'Identifier derived from MD5 hashing of book_id and user_id';

create table integration.book_user_desc (
    book_user_id uuid not null,
    qualifier text not null,
    valid_from timestamp not null,
    valid_to timestamp,
    create_dts timestamp,
    update_dts timestamp,
    load_audit_id int,
    primary key (book_user_id, valid_from),
    foreign key (book_user_id) references integration.book_user_link(id)
);

comment on table integration.book_user_desc is 'Temporal qualifier of the relationship of book_user';
comment on column integration.book_user_desc.qualifier is 'Ex. for gr: read, reading, to-read, or lt: your-library, wishlist, to-read, read-but-unowned, reading, favorites (default ones, the custom one will be ignored)';



create table integration.book_isbn(
    book_id uuid,
    ean13_id numeric(13),
    source_site_id int,
    create_dts timestamp,
    load_audit_id int,
    primary key (book_id, ean13_id),
    foreign key(book_id) references integration.book(id),
    foreign key(ean13_id) references integration.isbn(ean13_id)
);

comment on integration.book_isbn is 'Associations between book and ISBN(s) found from harvesting reviews';
comment on column integration.book_isbn.source_site_id is 'Original site that first loaded this association';



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

-- View that return how many reviews already persisted per website (logical name)

create or replace view integration.reviews_persisted_lookup  as
    select   logical_name
            ,book_uid
            ,1 as one_review
    from integration.book_site_review b
    join integration.site s on (s.id = b.site_id)
    join integration.review r on (r.book_id = b.book_id);

comment on view integration.reviews_persisted_lookup is 'Report the number of reviews currently persisted by website (logical_name)' ;


*/



---------------------- Function -------------------------

CREATE OR REPLACE function integration.get_sform(input_str text) returns text as $$
    DECLARE
        trim_space text;
    BEGIN
        trim_space := REGEXP_REPLACE( trim(input_str), '\s+', ' ', 'g');
        return upper( trim_space );
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE function integration.derive_bookid
                    (title_sform text, lang char(2), author_sform text) returns uuid as $$
    BEGIN
        return cast( md5( concat(title_sform, lang, author_sform) ) as uuid);
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE function integration.standardize_authorname
                    (fname text, lname text) returns text as $$
    BEGIN
        return  concat( upper(lname), ', ', upper(fname) );
    END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE function integration.derive_userid
                    (pseudo text, site_logical_name text) returns uuid as $$
    BEGIN
        return cast( md5( concat(pseudo, site_logical_name) ) as uuid);
    END;
$$ LANGUAGE plpgsql;







-------------------------------------- Delivery layer -------------------------------------

-- Goals:   - Layer where data is exposed for consumption by external users
--          - the main delievery is the sparse Matrix... so this is easily/efficiently stored in relation representation
--          - see other altenative solutions?? for greater flexibility




-------------------------------------- Delivery layer -------------------------------------
