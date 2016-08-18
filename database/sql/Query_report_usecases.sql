---------------------------------------------------------------------------------
-- Examples of Report implemented from both integration and presentation layer
---------------------------------------------------------------------------------

--------------------------------------------------------------------
-- Report: Trend (time series) of rating for Books
--------------------------------------------------------------------
select wi.title
        , date_trunc('year',r.review_date) as year
        , avg(r.parsed_rating) as avg_rating
        , count(r.id) as nb_rating
        , sum(count(r.id)) OVER (partition by title) as nb_total
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
where
wi.title in ('1984', 'The Hobbit', 'Brave New World', 'The Awakening', 'Into the Wild', 'Beowulf', 'The Canterbury Tales')
group by 1,2
order by 1,2
;

select english_title
        , date_trunc('year',date_id) as year
        , avg(rating) as avg_rating
        , count(*)  as nb_rating
        , sum(count(*)) OVER (partition by english_title) as nb_total
from presentation.review
join presentation.dim_book d using (book_id)
where english_title in ('1984', 'The Hobbit', 'Brave New World', 'The Awakening', 'Into the Wild', 'Beowulf', 'The Canterbury Tales')
group by 1,2
order by 1,2
;


--------------------------------------------------------------------
--Report:  Cultural/language appreciation differences
--------------------------------------------------------------------
select wi.title
        , l.english_name as review_language
        , avg(r.parsed_rating) as avg_rating
        , count(r.id) as nb_rating
        , sum( avg(r.parsed_rating) * count(r.id) ) over (partition by wi.title)
            / sum (count(r.id)) over (partition by wi.title)  as avg_overall
from integration.review r
join integration.work_info wi on (wi.work_refid = r.work_refid)
join integration.language l on (l.code = r.review_lang)
where
wi.title in ('The Kite Runner', 'The Hunger Games', 'Jane Eyre')
group by 1,2
order by 1,4 desc
;

select english_title
        , lang_code as review_language
        , avg(rating) as avg_rating
        , count(*)  as nb_rating
        -- weighted average
        , sum(avg(rating) * count(*)) over (partition by english_title)
            / sum(count(*)) over (partition by english_title) as avg_overall
from presentation.review r
join presentation.dim_book d using (book_id)
join presentation.dim_language dl on dl.code = r.lang_code
where english_title in ('The Kite Runner', 'The Hunger Games', 'Jane Eyre')
group by 1,2
order by 1,4 desc
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


select english_title
        , gender
        , status
        , avg(rating) as avg_rating
        , count(*) as nb_rating
        , sum ( avg(rating) * count(*) ) over (partition by english_title)
            / sum (count(*)) over (partition by english_title) as avg_overall
from presentation.review r
join presentation.dim_book db using (book_id)
join presentation.dim_reviewer dr using (reviewer_id)
group by 1,2,3
order by 1,2,3
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
            , case when parsed_likes > 0 then 'Helpful' else 'Not helpful' end  as helpful
            , r.parsed_rating
    from integration.review r
    join integration.work_info wi on (wi.work_refid = r.work_refid)
    where
    wi.title in ('Sense and Sensibility', 'Northern Lights', 'On the Road', 'Little Women', 'A Tale of Two Cities', 'The Alchemist')
    ) as foo
group by 1,2
order by 1,2
;

select title_ori
        , helpful
        , avg(rating) as avg_rating
        , count(1) as nb_rating
from
    (select title_ori
            , case when nb_likes > 0 then 'Helpful' else 'Not helpful' end  as helpful
            , rating
    from presentation.review r
    join presentation.dim_book b using (book_id)
    where
    title_ori in ('Sense and Sensibility', 'Northern Lights', 'On the Road', 'Little Women', 'A Tale of Two Cities', 'The Alchemist')
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


select  title_ori
        , s.name
        , count(1) as nb_reviews
        , avg(rating) as avg_rating
        , min(rating) as min_rating
        , max(rating) as max_rating
from presentation.review r
join presentation.dim_book b using (book_id)
join presentation.dim_site s using (site_id)
where title_ori in ( 'Harry Potter and the Goblet of Fire',
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
--Report: Who are the best authors in terms of average rating
-- for those having at least 100 reviews with not null rating
--------------------------------------------------------------------


select author_id
        , a.name
        , count(distinct r.book_id) as nb_books
        , count(1) as nb_reviews
        , avg(rating::float) as avg_rating
from review r
join rel_author ra using (book_id)
join dim_author a using (author_id)
where rating is not null
group by 1, 2
having count(1) > 100
order by 5 desc
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

select r.user_id as user_key
        , concat( wi.title,'::', array_to_string(array_agg(ai.name),'_')) as book_key
        , avg(r.parsed_rating) as rating  --user could've rated many times same work
from integration.review r
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
