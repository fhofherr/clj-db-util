create table ${schema}.placeholder_support (
    placeholder_value varchar2(30)
);

insert into ${schema}.placeholder_support(placeholder_value)
values ('${placeholder}');
