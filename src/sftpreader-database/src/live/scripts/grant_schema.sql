/* 
	grant schema 
*/

do
$$
begin

	execute 'grant all on database ' || current_database() || ' to ' || current_user;

	execute 'grant all on schema configuration to ' || current_user;
	execute 'grant all privileges on all tables in schema configuration to ' || current_user;
	execute 'grant all privileges on all sequences in schema configuration to ' || current_user;
	execute 'grant all privileges on all functions in schema configuration to ' || current_user;

	execute 'grant all on schema log to ' || current_user;
	execute 'grant all privileges on all tables in schema log to ' || current_user;
	execute 'grant all privileges on all sequences in schema log to ' || current_user;
	execute 'grant all privileges on all functions in schema log to ' || current_user;

	execute 'grant all on schema helper to ' || current_user;
	execute 'grant all privileges on all tables in schema helper to ' || current_user;
	execute 'grant all privileges on all sequences in schema helper to ' || current_user;
	execute 'grant all privileges on all functions in schema helper to ' || current_user;

end
$$ language plpgsql;
