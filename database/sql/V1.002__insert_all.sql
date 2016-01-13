


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------


insert into integration.site(id, logical_name, status, create_dts)
values
(1, 'librarything', 'active', now())
,(2, 'goodreads', 'active', now())
,(3, 'critiqueslibres', 'active', now())
,(4, 'babelio', 'active', now())
;

insert into integration.site_identifier(site_id, hostname, valid_from, valid_to, create_dts)
values
(1, 'www.librarything.com', now(), '''infinity'''::timestamp, now())
,(2, 'www.goodreads.com', now(), '''infinity'''::timestamp, now())
,(3, 'www.critiqueslibres.com', now(), '''infinity'''::timestamp, now())
,(4, 'www.babelio.com', now(), '''infinity'''::timestamp, now())
;


insert into integration.language(code, code3, name, create_dts)
values
('FR', 'FRE', 'Français', now())
,('EN', 'ENG', 'English', now())
,('ES', 'SPA', 'Spanish', now())
,('DE', 'GER', 'German', now())
;






