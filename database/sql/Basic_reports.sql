

select site_id, count(distinct user_id) as "Nb of reviewer"
      , count(1)::float / count(distinct user_id) as "Avg nb of reviews per reviewer"
      , count(1)::float / count(distinct work_refid) as "Avg nb of reviews per book"
from integration.review
group by 1;

--#ofWork with reviews per site
select count(1) from integration.work_site_mapping where site_id = 2 and work_uid != '-1';
select count(1) from integration.work_site_mapping where site_id = 4 and work_uid != '-1';


--#ofWork without reviews
select count(distinct refid)
from integration.work w
where last_harvest_dts is not null
and not exists (select 1 from integration.review r where r.work_refid = w.refid);


select concat(w.title, ' (id=', w.work_refid, ')'), count(1)
from integration.review r
join integration.work_info w on w.work_refid = r.work_refid
group by 1
order by 2 desc
limit 20;


with basic_stats as (
select work_refid
       , avg(case when site_id = 1 then parsed_rating else null end) as avg_rating_lt
       , stddev(case when site_id = 1 then parsed_rating else null end) as std_rating_lt
       , sum(case when site_id = 1 then 1 else 0 end) as ctn_lt
       , avg(case when site_id = 2 then parsed_rating else null end) as avg_rating_gr
       , stddev(case when site_id = 2 then parsed_rating else null end) as std_rating_gr
       , sum(case when site_id = 2 then 1 else 0 end) as ctn_gr
       , avg(case when site_id = 4 then parsed_rating else null end) as avg_rating_ba
       , stddev(case when site_id = 4 then parsed_rating else null end) as std_rating_ba
       , sum(case when site_id = 4 then 1 else 0 end) as ctn_ba
from integration.review
group by work_refid)
select
   avg(avg_rating_lt) as mean_lt, avg(std_rating_lt) as std_lt
   , avg(avg_rating_gr) as mean_gr, avg(std_rating_gr) as std_gr
   , avg(avg_rating_ba) as mean_ba, avg(std_rating_ba) as std_ba
   , avg(avg_rating_lt-avg_rating_gr) as mean_lt_gr, avg(std_rating_lt-std_rating_gr) as std_lt_gr
   , avg(avg_rating_lt-avg_rating_ba) as mean_lt_ba, avg(std_rating_lt-std_rating_ba) as std_lt_ba
   , avg(avg_rating_gr-avg_rating_ba) as mean_gr_ba, avg(std_rating_gr-std_rating_ba) as std_gr_ba
   , corr(avg_rating_lt, avg_rating_gr) as corr_lt_gr
   , corr(avg_rating_lt, avg_rating_ba) as corr_lt_ba
   , corr(avg_rating_gr, avg_rating_ba) as corr_gr_ba
from public.basic_stats;



with per_wid as (
 select
       r.work_refid
       , sum(case when dupes.id IS NOT NULL then 1 else 0 end) as Nb_dupes
       , count(1) as Nb_reviews
 from integration.review r
 left join (select review_id as id from integration.review_similarto
            union
            select other_review_id as id from integration.review_similarto) as dupes on (r.id = dupes.id)
 group by 1
 --for perf only.. : limit 200)
)
select
     sum(Nb_dupes) as "Total dupes"
     , sum(Nb_reviews) as "Total reviews"
     , avg(Nb_reviews) as "Avg Reviews"
     , avg(Nb_dupes) as "Avg dupes"
     , avg(Nb_dupes) / avg(Nb_reviews) as "Ratio avg dupes over avg nb reviews"
from per_wid;


--here it's just the distinct dupes (the same 2 reviews only counted once)
select sum(case when r.site_id = 1 and o.site_id = 1 then 1 else 0 end) as "Total within Lt"
     , sum(case when r.site_id = 2 and o.site_id = 2 then 1 else 0 end) as "Total within Gr"
     , sum(case when r.site_id = 4 and o.site_id = 4 then 1 else 0 end) as "Total within Ba"
     , sum(case when r.site_id = 1 and o.site_id = 2 then 1
                when r.site_id = 2 and o.site_id = 1 then 1 else 0 end) as "Total between Lt and Gr"
     , sum(case when r.site_id = 1 and o.site_id = 4 then 1
                when r.site_id = 4 and o.site_id = 1 then 1 else 0 end) as "Total between Lt and Ba"
     , sum(case when r.site_id = 2 and o.site_id = 4 then 1
                when r.site_id = 4 and o.site_id = 2 then 1 else 0 end) as "Total between Gr and Ba"
     , sum(case when u.username = ou.username and r.review_date = o.review_date
                     and r.site_id = o.site_id then 1 else 0 end) as "Exact copies"
     , sum(case when u.username != ou.username and r.review_date = o.review_date
                     and r.site_id = o.site_id then 1 else 0 end) as "Copies except username"
     , sum(case when u.username = ou.username and r.review_date != o.review_date
                     and r.site_id = o.site_id then 1 else 0 end) as "Copies except date"
     , sum(case when u.username = ou.username and r.review_date = o.review_date
                     and r.site_id != o.site_id then 1 else 0 end) as "Copies except site"
     , sum(case when u.username = ou.username and r.review_date != o.review_date
                     and r.site_id != o.site_id then 1 else 0 end) as "Copies only username"
     , sum(case when u.username != ou.username and r.review_date = o.review_date
                     and r.site_id != o.site_id then 1 else 0 end) as "Copies only date"
     , sum(case when u.username != ou.username and r.review_date = o.review_date
                     and r.site_id != o.site_id then 1 else 0 end) as "Copies only date"
     , sum(case when u.username != ou.username and r.review_date != o.review_date
                     and r.site_id != o.site_id then 1 else 0 end) as "Diff everything"
from integration.review_similarto s
join integration.review r on (s.review_id = r.id)
join integration.user u on (r.user_id = u.id)
join integration.review o on (s.other_review_id = o.id)
join integration.user ou on (o.user_id = ou.id)
;


select review_lang, count(1)
from integration.review
where review_lang not in ('--','')
group by 1
order by 2 desc;

