begin;

insert into numbers (num, name)
select 1, 'One'
union all
select 2, 'Two'
union all
select 3, 'Three'
union all
select 4, 'Four'
union all
select 5, 'Five'
union all
select 6, 'Six'
union all
select 7, 'Seven'
union all
select 8, 'Eight'
;

insert into prime_numbers (num, name, n)
select 2, 'Two', 1
union all
select 3, 'Three', 2
union all
select 5, 'Five', 3
;

commit;
