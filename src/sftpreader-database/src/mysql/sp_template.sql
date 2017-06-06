delimiter $$

drop procedure if exists new_procedure;

$$

create procedure new_procedure
(
)
begin
	select * 
    from interface_file_type;
end

$$

delimiter ;
