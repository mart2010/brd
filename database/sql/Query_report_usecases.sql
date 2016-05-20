
--------------------------------------------------------------------
-- Report: Trend (time series) of rating for Books
--------------------------------------------------------------------
select wi.title
        , r.review_date
        , avg(r.parsed_rating) as avg_rating
        , count(r.id) as nb_rating
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
where
wi.title in ('1984', 'The Hobbit', 'Brave New World')
group by 1,2
order by 1,2
;

--------------------------------------------------------------------
-- Report: Trend of rating (time series) for a Books given their ISBNs
--------------------------------------------------------------------
-- <same as above> appending these additional tables:
join integration.work_isbn wis on (wis.work_refid = r.work_refid)
join integration.isbn i on (i.ean = wis.ean)
where
i.isbn13 in ('9780141036144', '9781402536946', '9780754054375')


--Report:  Cultural/language appreciation differences
select wi.title
        , l.english_name as review_language
        , avg(r.parsed_rating) as avg_rating
        , count(r.id) as nb_rating
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
join integration.language l on (l.code = r.review_lang)
where
wi.title in ('The Kite Runner', 'The Hunger Games', 'Jane Eyre')
group by 1,2
order by 1,2
;


--------------------------------------------------------------------
--Report: Reader info/demograhics appreciation differences
--------------------------------------------------------------------
select wi.title
        , ui.gender
        , date_part('year', age(ui.birth_date)) as age
        , ug.country_code
        , ug.region
        , avg(r.parsed_rating) as avg_rating
        , count(r.id) as nb_rating
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
left join integration.user_info ui on (ui.user_id = r.user_id)
left join integration.user_geo ug on
   (ug.user_id = r.user_id
    and r.review_date >= ug.valid_from
    and r.review_date < ug.valid_to)
where
wi.title in ('The Kite Runner', 'The Hunger Games', 'Jane Eyre')
group by 1,2,3,4,5
order by 1
;

--------------------------------------------------------------------
-- Report: Difference in rating between helpful review (at least one like) or none helpful
--------------------------------------------------------------------
select title
        , helpful
        , avg(parsed_rating) as avg_rating
        , count(1) as nb_rating
from
    (select wi.title
            , case when likes > 0 then 'Helpful' else 'Not helpful' end  as helpful
            , r.parsed_rating
    from integration.review r
    join integration.work_info wi on (wi.work_refid = r.work_refid)
    where
    wi.title in ('Sense and Sensibility', 'Northern Lights', 'On the Road', 'Little Women', 'A Tale of Two Cities', 'The Alchemist')
    ) as foo
group by 1,2
order by 1,2
;


--------------------------------------------------------------------
--Report: Reviews statistics difference per site for given Books
--------------------------------------------------------------------
select  wi.title
        ,s.logical_name
        , count(r.id) as nb_reviews
        , avg(r.parsed_rating) as avg_rating
        , min(r.parsed_rating) as min_rating
        , max(r.parsed_rating) as max_rating
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
join integration.site s on (s.id = r.site_id)
where wi.title in ( 'Harry Potter and the Goblet of Fire',
 'Harry Potter and the Order of the Phoenix',
 'Harry Potter and the Half-Blood Prince',
 'Harry Potter and the Prisoner of Azkaban',
 'Harry Potter and the Sorcerer''s Stone (Book 1)',
 'Harry Potter and the Chamber of Secrets',
 'Harry Potter and the Deathly Hallows' )
group by 1,2
order by 1,2
;


--------------------------------------------------------------------
--Report: How do sites vary in terms of rating (avg on popular works)
--------------------------------------------------------------------
-- based on most popular common Books (5000 most popular books on LT)
-- N.B.: pivot() function requires:  create extension tablefunc;
-- $$ is equivalent to quoting (dollar-quoting)

select * from crosstab('
    select  wi.title
            ,s.logical_name
            , avg(r.parsed_rating) as avg_rating
    from integration.review r
    join integration.work_info wi on (wi.work_refid = r.work_refid)
    join integration.site s on (s.id = r.site_id)
    where wi.popularity <  5000
    group by 1,2
    order by 1,2'
    , $$ select logical_name from integration.site order by 1 $$)
as ct ("title" text,
        "amazon.com" decimal,
        "babelio" decimal,
        "critiqueslibres" decimal,
        "goodreads" decimal,
        "librarything" decimal)
;


--------------------------------------------------------------------
-- Report: Produce reviewer - books (flatten MATRIX) for recommending engine algo
-- user kept as-is (preserve users accross sites with identical reviews)
--------------------------------------------------------------------
-- User-key: the uuid coming from hashing user_uid + site_name (Matrix rows)
-- Book-key: 'title::athor1_author2..' (compact representation for very sparse Matrix Cols Books)

select u.id as user_key
        , concat( wi.title,'::', array_to_string(array_agg(ai.name),'_')) as book_key
        , avg(r.parsed_rating) as rating  --user could've rated many times same work
from integration.review r
join integration.user u on (r.user_id = u.id)
join integration.work_info wi on (wi.work_refid = r.work_refid)
join integration.work_author wa on (wa.work_refid = r.work_refid)
join integration.author_info ai on (ai.author_id = wa.author_id)
group by u.id, wi.work_refid
;


--------------------------------------------------------------------
-- Report: Same as above but merging same users across sites
-- same user have identical reviews across site
--------------------------------------------------------------------

select coalesce(usame.same_user_id, u.id) as user_key
        , concat( wi.title,'::', array_to_string(array_agg(ai.name),'_')) as book_key
        , avg(r.parsed_rating) as rating
from integration.review r
join integration.user u on (r.user_id = u.id)
left join integration.user_sameas usame on (usame.user_id = u.id)
join integration.work_info wi on (wi.work_refid = r.work_refid)
join integration.work_author wa on (wa.work_refid = r.work_refid)
join integration.author_info ai on (ai.author_id = wa.author_id)
group by coalesce(usame.same_user_id, u.id), wi.work_refid
;



-- Query for review-text similarity:
-- requires an index on FK work_refid
-- install as root: create extension pg_trgm;

-- Very Slowww (just the cross-product take 2-3min) (kill after 45min) CPU bound!! need alternative!!!
-- Must employ solution with 'integration.review_similarto' where I load this incrementally!!!!

select r1.work_refid, r1.id as r1_id, r2.id as r2_id, similarity(r1.review, r2.review)
from integration.review as r1
join integration.review as r2 on (r1.work_refid = r2.work_refid
                                  and r1.review_lang = r2.review_lang
                                  and r1.id != r2.id)
where
r1.work_refid between 2000 and 2500;
















