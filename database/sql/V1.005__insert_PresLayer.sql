-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-- DML given to create the table & flat file for Redshift import
insert into presentation.dim_site
select s.id, s.logical_name, si.hostname
from integration.site s
join integration.site_identifier si on s.id = si.site_id
where s.id in (1,2,4)
;


insert into presentation.dim_language
select code, english_name, french_name
from integration.language
;


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
ORDER BY 1
;


---------------------------------------------------------------
insert into presentation.dim_mds(code, parent_code, original_code, text)
select code, left(code, char_length(code)-1), code_w_dot, mds_text
from integration.mds
;

--insert the missing parent node
insert into presentation.dim_mds(code, text)
select parent_code,  max(array_to_string((string_to_array(text,'-->'))[1:array_length(string_to_array(text,'-->'),1)-1], '-->'))
from presentation.dim_mds d
where not exists (select 1 from presentation.dim_mds c where d.parent_code = c.code)
group by parent_code
;

alter table presentation.dim_mds add foreign key (parent_code) references presentation.dim_mds (code);



----------------------------------------------------------------------
-- Rules For presentation on dim_book:
 -- ignore all duplicate work (only use master work)
 -- ignore all work with null Title

----------------------------------------------------------------------
insert into presentation.dim_book(book_id, title_ori, lang_ori, mds_code,
                english_title, french_title, german_title)
select w.work_refid
        , w.title
        , lower(w.ori_lang_code)
        , mds_code
        , w.title as english_title
        , max(case when wt.lang_code = 'fre' then wt.title else NULL end) as french_title
        , max(case when wt.lang_code = 'ger' then wt.title else NULL end) as german_title
from integration.work_info w
left join integration.work_title wt on wt.work_refid = w.work_refid
where not exists (select 1 from integration.work_sameas s where w.work_refid = s.work_refid)
and w.title IS NOT NULL
group by 1,2,3,4,5
;



create sequence presentation.tag_seq;

---------------------------------------------------------------
-- Rules:
--reject "technical" tag beginning with :
-- '!',  '"' (could keep those but remove double quote),
-- '#' , '$', "'" (single quote.. same as double quote)
-- '=', ':', '?', '@', '%', '&', '*'
-- '"', '(',  ''' (probably need to keep the following text)
-- '[0-9.]+$' 'only a bunch of leading number with/without optional dot
-- '[0-9]{5,}' having 5 or more consecitive digits or non-word characters
-- tag longer than 40 characters
-- finally easier    tag ~ '^[\w]{3,}' and tag !~ '^[0-9.]+$'


with tags as (
    insert into presentation.dim_tag(tag_id, tag, lang_code)
    select nextval('presentation.tag_seq')
        , tag_upper
        , max(lang_code)
    from integration.tag t
    where tag ~ '^[\w]{3,}' and tag !~ '^[0-9.]+$'
    and char_length(tag) <= 40
    group by tag_upper
    returning *
)
insert into presentation.rel_tag(book_id, tag_id)
select distinct wt.work_refid as book_id
        , tags.tag_id
from integration.work_tag wt
join integration.tag t on t.id = wt.tag_id
join tags on (tags.tag = t.tag_upper)
--keep only Books loaded in Presentation
join presentation.dim_book b on b.book_id = wt.work_refid
;


create sequence presentation.author_seq;

---------------------------------------------------------------
with authors as (
    insert into presentation.dim_author(author_id, code, name)
    select nextval('presentation.author_seq')
            , a.code
            , ai.name
    from integration.author a
    join integration.author_info ai on a.id = ai.author_id
    returning *
)
insert into presentation.rel_author(book_id, author_id)
select distinct w.work_refid, authors.author_id
from integration.work_author w
join integration.author a on w.author_id = a.id
join authors on a.code = authors.code
--keep only Books loaded in Presentation
join presentation.dim_book b on b.book_id = w.work_refid
;



---------------------------------------------------------------
insert into presentation.dim_reviewer(reviewer_uuid, username, gender, birth_year, status, site_name)
select u.id as id_uuid
        , username
        , case when random() < 0.4 then 'F' when random() < 0.6 then 'M' else 'U' end as gender
        , cast(random() * 50 + 1950 as smallint) as birth_year
        , case when random() < 0.4 then 'Single' when random() < 0.6 then 'Married' when random() < 0.2 then 'Divorced' else 'Unkwnown' end as status
        , s.logical_name
from integration.user u
join integration.site s on u.site_id = s.id
where u.username is not null
;

-- add fake city data at random (with larger populated city more likely chosen) and set lat/long with some noise (on Normal dist)
update presentation.dim_reviewer r set city = c.city, lati = c.lat + normal_rand(1,0,0.3), longi = c.long + normal_rand(1,0,0.3)
from (select reviewer_id, (SELECT random()*2289584999 WHERE s = s) as p_cum
      from presentation.dim_reviewer s) sub
join staging.city c on (sub.p_cum between c.pop_cum - c.pop and c.pop_cum)
where sub.reviewer_id = r.reviewer_id
;

---------------------------------------------------------------
insert into presentation.review(id, similarto_id, book_id, reviewer_id, site_id, date_id, rating, nb_likes, lang_code, review)
select r.id
        , s.other_review_id
        , r.work_refid
        , pr.reviewer_id
        , r.site_id
        , r.review_date
        , r.parsed_rating
        , r.parsed_likes
        , r.review_lang
        , r.review
from integration.review r
join presentation.dim_book db on db.book_id = r.work_refid
join presentation.dim_reviewer pr on pr.reviewer_uuid = r.user_id
left join integration.review_similarto s on s.review_id = r.id
;

