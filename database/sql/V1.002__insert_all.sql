-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------


insert into integration.site(id, logical_name, status, create_dts)
values
(1, 'librarything', 'active', now())
,(2, 'goodreads', 'active', now())
,(3, 'critiqueslibres', 'active', now())
,(4, 'babelio', 'active', now())
,(5, 'amazon.com', 'active', now())
;

insert into integration.site_identifier(site_id, hostname, valid_from, valid_to, create_dts)
values
(1, 'www.librarything.com', now(), '''infinity'''::timestamp, now())
,(2, 'www.goodreads.com', now(), '''infinity'''::timestamp, now())
,(3, 'www.critiqueslibres.com', now(), '''infinity'''::timestamp, now())
,(4, 'www.babelio.com', now(), '''infinity'''::timestamp, now())
,(5, 'www.amazon.com', now(), '''infinity'''::timestamp, now())
;


-- 'copy' requires root privilege and the use of plsql '\copy' is only possible in psql shell env!
-- now done in python!
--copy integration.language(code3,code3_term,code2,code,english_iso_name,english_name,french_iso_name,french_name)
--from '/Users/mart/Google Drive/brd/ref_data/Iso_639_and_Marc_code - ISO-639-2_utf-8.tsv'
--with delimiter '\t';


--insert into integration.language(code, code2, name, native_name, create_dts)
--values
--('ENG', 'EN', 'English', 'English', now())
--,('FRE', 'FR', 'French', 'Français', now())
--,('GER', 'DE', 'German', '', now())
--,('SPA', '', 'Spanish', '', now())
--,('DUT', '', 'Dutch', '', now())
--,('ITA', '', 'Italian', '', now())
--,('POR1', '', 'Portuguese (Brazil)', '', now())
--,('POR2', '', 'Portuguese (Portugal)', '', now())
--,('SWE', '', 'Swedish', '', now())
--,('DAN', '', 'Danish', '', now())
--,('FIN', '', 'Finnish', '', now())
--,('NOR', '', 'Norwegian', '', now())
--,('POL', '', 'Polish', '', now())
--,('CAT', '', 'Catalan', '', now())
--,('HUN', '', 'Hungarian', '', now())
--,('TUR', '', 'Turkish', '', now())
--,('RUS', '', 'Russian', '', now())
--,('GRE', '', 'Greek', '', now())
--,('HIN', '', 'Hindi', '', now())
--,('CZE', '', 'Czech', '', now())
--,('PIR', '', 'Piratical', '', now())
--,('JPN', '', 'Japanese', '', now())
--,('LIT', '', 'Lithuanian', '', now())
--,('ALB', '', 'Albanian', '', now())
--,('LAT', '', 'Latin', '', now())
--,('BUL', '', 'Bulgarian', '', now())
--,('RUM', '', 'Romanian', '', now())
--,('CHI1', '', 'Chinese, traditional', '', now())
--,('SCR', '', 'Croatian', '', now())
--,('CHI2', '', 'Chinese, simplified', '', now())
--,('EST', '', 'Estonian', '', now())
--,('SLO', '', 'Slovak', '', now())
--,('LAV', '', 'Latvian', '', now())
--,('ARA', '', 'Arabic', '', now())
--,('BAQ', '', 'Basque', '', now())
--,('POR', '', 'Portuguese', '', now())
--,('ICE', '', 'Icelandic', '', now())
--,('WEL', '', 'Welsh', '', now())
--,('HEB', '', 'Hebrew', '', now())
--,('SCC', '', 'Serbian', '', now())
--,('IND', '', 'Indonesian', '', now())
--,('GLE', '', 'Irish', '', now())
--,('KOR', '', 'Korean', '', now())
--,('PER', '', 'Persian', '', now())
--,('AFR', '', 'Afrikaans', '', now())
--,('TGL', '', 'Tagalog', '', now())
--,('ARM', '', 'Armenian', '', now())
--,('SLV', '', 'Slovenian', '', now())
--,('EPO', '', 'Esperanto', '', now())
--,('GEO', '', 'Georgian', '', now())
--,('GLG', '', 'Galician', '', now())
--,('URD', '', 'Urdu', '', now())
--,('MAC', '', 'Macedonian', '', now())
--,('YID', '', 'Yiddish', '', now())
--,('ENM', '', 'English (Middle)', '', now())
--,('CHI', '', 'Chinese', '', now())
--,('NOB', '', 'Norwegian (Bokmål)', '', now())
--;




-- patch to reset review_lang for reviews written mostly in CAPITAL letter (over 50%)

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


-- patch to remove reviews
-- For now, only 9 works have been harvested twice... not worth deletion!!
select s.master_refid, s.work_refid, m1.work_uid, m1.last_harvest_dts,  m2.work_uid, m2.last_harvest_dts
from integration.work_sameas s
join integration.work_site_mapping m1 on (s.work_refid = m1.work_refid and m1.site_id = 2)
join integration.work_site_mapping m2 on (s.master_refid = m2.work_refid and m2.site_id = 2)
order by 1;


--------------------For Babelio --------------------
--44 for Babelio (site_id = 4)
-- But WARNING, some of these not to be deleted as they are not merged in babelio and have have diff reviews ...
--better to identify real dupes:



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

