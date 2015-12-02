


-------------------------------------- Data insertion -------------------------------------

-- the goal here is to pre-load all reference data and static content....

-------------------------------------------------------------------------------------------


insert into integration.site(id, logical_name, status, create_dts)
values (1, 'critiqueslibres', 'active',now());

insert into integration.site_identifier(site_id, hostname, valid_from, valid_to, create_dts)
values (1, 'www.critiqueslibres.com', now(), '''infinity'''::timestamp, now());

insert into integration.language(lang_code, lang_name, create_dts)
values  ('FR', 'Français', now()),
        ('EN', 'English', now());




