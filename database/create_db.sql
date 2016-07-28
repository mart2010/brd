-- author = 'mart2010'
-- copyright = "Copyright 2016, The BRD Project"


-- As superuser, create role brd
	create role brd with login password 'brd';
	alter role brd CREATEROLE;
	create database brd owner= brd;
	
-- As superuser, switch to new db and revoke privileges to other users */
	\c brd
	revoke connect on database brd from public;
	revoke all on schema public from public;
	grant all on schema public to brd;
	
	
-- used to backup database
pg_dump -f brd_20160727.sql  --schema=staging --schema=integration -U brd  -p 54355 brd


