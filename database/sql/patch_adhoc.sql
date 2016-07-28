
-- patch to reset review_lang for reviews written mostly in CAPITAL letter (over 50%)
-- (langid does not detect language correctly for these ..)
update integration.review set review_lang = NULL
where id in
(select id
from integration.review
where
char_length(review) >= 30
-- at least one capital letter exist
and review ~ '[A-Z]'
-- capital letters represent at least 50% of text
and  char_length(regexp_replace(review, '[A-Z]','','g'))::float / char_length(review) < 0.5
);


-- patch to remove reviews of master-id loaded twice from LT (similarto must have ON DELETE CASCADE):
-- (rare circumstances when master-id/work-id pair not known during harvest and master-id already harvested)
-- Run on 20/06/16 (result:  DELETE 520)
delete from integration.review r
using
(select foo1.work_refid, max(load_audit_id) as load_audit_id, count(1)
 from
        (select r.load_audit_id, r.work_refid
        from review r join work_sameas w on r.work_refid = w.master_refid
        where r.site_id = 1
        group by 1,2) as foo1
 group by  1
 having count(1) > 1) as foo2
where foo2.work_refid = r.work_refid
and foo2.load_audit_id = r.load_audit_id
and r.site_id = 1;


-- patch to remove reviews on goodreads
-- For now, only 9 works have been harvested twice... not worth deletion!!
select s.master_refid, s.work_refid, m1.work_uid, m1.last_harvest_dts,  m2.work_uid, m2.last_harvest_dts
from integration.work_sameas s
join integration.work_site_mapping m1 on (s.work_refid = m1.work_refid and m1.site_id = 2)
join integration.work_site_mapping m2 on (s.master_refid = m2.work_refid and m2.site_id = 2)
order by 1;


--------------------For Babelio --------------------
--44 for Babelio (site_id = 4)
-- But WARNING, some of these not to be deleted as they are not merged in babelio and have have diff reviews ...

-- however, there are other real duplicates where work_uid is the same with diff work_refid (LT has not merged these yet)
-- Run on 21/06/16 (result: DELETE 28205)
delete from integration.review rev
using
(select m.work_refid
from work_site_mapping m
join
    (select work_uid, min(load_audit_id) min_load_audit_id, count(1)
    from integration.work_site_mapping
    where site_id = 4
    and work_uid != '-1'
    group by 1
    having count(1) > 1) dup
    on (dup.work_uid = m.work_uid and m.site_id = 4)
where m.load_audit_id > dup.min_load_audit_id) as dupes
where rev.work_refid = dupes.work_refid
and rev.site_id = 4
;



-- There were '.' in mds_code included initially in table work_info
update integration.work_info set mds_code = replace(mds_code,'.','');

-- To truncate too long mds_code stored in work_info (initially loaded before mds were corrected)
--keep decreasing mds_code length such as above until no more update are done...

-- About 15 iterations like these were run before no more rows were updated (19/07/2016)
update integration.work_info set mds_code = left(mds_code,char_length(mds_code)-1)
where mds_code in (select distinct mds_code from integration.work_info w where mds_code is not null and not exists (select 1 from integration.mds m where m.code= w.mds_code));


-- Path to remove duplicates of review_similarto created on early "faulty" process (load_audit_id = 1167)
delete from rev_similarto_process where load_audit_id = 1167;
delete from review_similarto where load_audit_id = 1169;

--relaunch job
--python -m luigi --module brd.taskpres BatchProcessReviewSimilarTo --n-work 50 --local-scheduler

--re-define PK correctly
alter table integration.review_similarto drop constraint review_similarto_pkey;
alter table integration.review_similarto add primary key (review_id);

