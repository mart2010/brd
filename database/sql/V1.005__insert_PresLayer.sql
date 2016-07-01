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


CREATE SEQUENCE seq;

---------------------------------------------------------------
--TODO: validate exclusion of a lot of "technical" tag beginning by :
-- '!',  '"' (could keep those but remove double quote),
-- '#' , '$', "'" (single quote.. same as double quote)
-- '32.41' (a bunch of leading number but with a dot ), (to avoid removing date like 1607-1776, 15th century)
-- '=', ':', '?', '@' (probbaly need to keep the following text), etc...

with tags as (
    insert into presentation.dim_tag(id, tag, lang_code)
    select nextval('seq')
        , tag_upper
        , max(lang_code)
    from integration.tag t
    -- filter out all unwanted tags (validate: the escape of $ (\$), the dot ., and the ?
    where tag !~ '^(!|#|\$|[0-9]+\.[0-9]+|=|:|\?|@)'
    group by tag_upper
    returning *
)
insert into presentation.rel_tag(book_id, tag_id)
select distinct wt.work_refid as book_id
        , tags.id
from integration.work_tag wt
join integration.tag t on t.id = wt.tag_id
join tags on (tags.tag = t.tag_upper)
;


---------------------------------------------------------------
with authors as (
    insert into presentation.dim_author(id, code, name)
    select nextval('seq')
            , a.code
            , ai.name
    from integration.author a
    join integration.author_info ai on a.id = ai.author_id
    returning *
)
insert into presentation.rel_author(book_id, author_id)
select w.work_refid, authors.id
from integration.work_author w
join integration.author a on w.author_id = a.id
join authors on a.code = authors.code
;


---------------------------------------------------------------
--TODO: validate this
insert into presentation.dim_book(id, title_ori, mds_code,
                english_title, french_title, german_title)
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
;


---------------------------------------------------------------
--TODO: validate
--need to be populated prior to review
insert into presentation.dim_reviewer(id_uuid, username, gender, birth_year, status, site_name)
select u.id as id_uuid
        , username
        , case when random() < 0.4 then 'F' when random() < 0.6 then 'M' else 'U' end as gender
        , cast(random() * 50 + 1950 as smallint) as birth_year
        , case when random() < 0.4 then 'Single' when random() < 0.6 then 'Married' when random() < 0.2 then 'Divorced' else 'Unkwnown' end as status
        , s.logical_name
from integration.user u
join integration.site s on u.site_id = s.id
;

---------------------------------------------------------------
insert into presentation.review(id, id_similarto, book_id, reviewer_id, site_id, date_id, rating, nb_likes, lang_code)
select r.id
        , s.other_review_id as id_similarto
        , coalesce(ws.master_refid, r.work_refid)
        , pr.id
        , r.site_id
        , r.review_date
        , r.parsed_rating
        , r.parsed_likes
        , r.review_lang
from integration.review r
left join integration.review_similarto s on s.review_id = r.id
left join integration.work_sameas ws on ws.work_refid = r.work_refid
join presentation.dim_reviewer pr on pr.id_uuid = r.user_id
;

