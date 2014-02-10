drop procedure get_pay_stat
go

CREATE PROCEDURE get_pay_stat (db_name varchar(10))
NOT DETERMINISTIC
CONTAINS SQL
begin
declare 
month_formated,month_start_formated,month_end_formated,country_name varchar(255); 
declare done integer default 0;
declare month_list cursor for 
select distinct
date_format(selected_date,'%Y-%m') month_formated,
date_format(last_day(selected_date),'%Y-%m-01') month_start_formated,
date_format(last_day(selected_date),'%Y-%m-%d 23:59:59') month_end_formated
from 
(select adddate('1970-01-01',t4.i*10000 + t3.i*1000 + t2.i*100 + t1.i*10 + t0.i) selected_date from
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t0,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t1,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t2,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t3,
 (select 0 i union select 1 union select 2 union select 3 union select 4 union select 5 union select 6 union select 7 union select 8 union select 9) t4) v
where selected_date between '2012-01-01' and date_add(sysdate(), interval 1 year);
declare continue handler for sqlstate '02000' set done=1; 

select eng_name into country_name from common.country 
where id in (select f_cntr_id from common.local_resource where lr_db_name=db_name);
open month_list;
repeat 
  fetch month_list into month_formated,month_start_formated,month_end_formated;
  if not done then
    set @month_formated = month_formated;
    set @month_start_formated = month_start_formated;
    set @month_end_formated = month_end_formated;
    set @country_name = country_name;
    set @month_formated2 = concat(month_formated,'-01');

    set @s = CONCAT('select count(*) into @ps_all from ',db_name,'.contacts where ctrl=''1'' and dater < ?;');
    prepare stmt from @s;
    execute stmt using @month_end_formated;
    deallocate prepare stmt;
  
    set @s = CONCAT('select count(*) into @ps_reg from ',db_name,'.contacts where dater >= ? and dater <= ?;');
    prepare stmt from @s;
    execute stmt using @month_start_formated,@month_end_formated;
    deallocate prepare stmt;


    set @s = CONCAT('select count(distinct n.f_cont_id) into @ps_reg_self 
            from ',db_name,'.enterprise_note_txt as t
                inner join ',db_name,'.enterprise_note as n on (t.en_id = n.en_id)
            where 
                n.en_datetime >= ? 
                and n.en_datetime < ? 
                and n.f_enot_id = 25
                and t.en_text like ''%000rrr%'';');
    prepare stmt from @s;
    execute stmt using @month_start_formated,@month_end_formated;
    deallocate prepare stmt;

    set @s = CONCAT('select count(distinct f_cont_id) into @ps_payed
            from ',db_name,'.membership_period 
            where 
                mp_start_date <=  ? 
                and mp_end_date > ? 
                and f_mt_id in (3,5,13,17);');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated;
    deallocate prepare stmt;


insert into pay_tp (country_name,ps_month,f_mt_id,mt_name,kol_ps_go)
 select 
country_name,@month_formated2,mt_id,mt_name,0
from
(select mt_id,mt_name from common.membership_type
                union
                select 999,'старт') mt where mt_id in (3,5,13,17,999) 
and mt_id not in 
(select f_mt_id from pay_tp where country_name=country_name and ps_month=@month_formated2);


    set @s = CONCAT('update pay_tp set kol_ps_payed = (select count(distinct f_cont_id) 
                        from (select 
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end f_mt_id,
mp.mp_end_date,mp.f_cont_id,mp.mp_start_date 
from ',db_name,'.membership_period mp 
join ',db_name,'.contacts c on mp.f_cont_id=c.id
join common.membership_type mt on mt.mt_id = mp.f_mt_id
join common.country ct on c.country=ct.id
) membership_period
                        where 
                            mp_start_date <=  ?
                            and mp_end_date > ? 
                            and f_mt_id=3
                    group by f_mt_id) where country_name= ? and ps_month=? and f_mt_id=3');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated,@country_name,@month_formated2;
    deallocate prepare stmt;


set @s = CONCAT('update pay_tp set kol_ps_payed = (select count(distinct f_cont_id) 
                        from (select 
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end f_mt_id,
mp.mp_end_date,mp.f_cont_id,mp.mp_start_date 
from ',db_name,'.membership_period mp 
join ',db_name,'.contacts c on mp.f_cont_id=c.id
join common.membership_type mt on mt.mt_id = mp.f_mt_id
join common.country ct on c.country=ct.id
) membership_period
                        where 
                            mp_start_date <=  ?
                            and mp_end_date > ? 
                            and f_mt_id=999
                    group by f_mt_id) where country_name= ? and ps_month=? and f_mt_id=999');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated,@country_name,@month_formated2;
    deallocate prepare stmt;


    set @s = CONCAT('update pay_tp set kol_ps_payed = (select count(distinct f_cont_id) 
                        from ',db_name,'.membership_period
                        where 
                            mp_start_date <=  ?
                            and mp_end_date > ? 
                            and f_mt_id=5
                    group by f_mt_id) where country_name= ? and ps_month=? and f_mt_id=5');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated,@country_name,@month_formated2;
    deallocate prepare stmt;

    set @s = CONCAT('update pay_tp set kol_ps_payed = (select count(distinct f_cont_id) 
                        from ',db_name,'.membership_period
                        where 
                            mp_start_date <=  ?
                            and mp_end_date > ? 
                            and f_mt_id=13
                    group by f_mt_id) where country_name= ? and ps_month=? and f_mt_id=13');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated,@country_name,@month_formated2;
    deallocate prepare stmt;

    set @s = CONCAT('update pay_tp set kol_ps_payed = (select count(distinct f_cont_id) 
                        from ',db_name,'.membership_period
                        where 
                            mp_start_date <=  ?
                            and mp_end_date > ? 
                            and f_mt_id=17
                    group by f_mt_id) where country_name= ? and ps_month=? and f_mt_id=17');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_end_formated,@country_name,@month_formated2;
    deallocate prepare stmt;

    set @s = CONCAT('select count(distinct f_cont_id) into @ps_payed_month 
            from ',db_name,'.membership_period 
            where 
                mp_start_date <=  ? 
                and mp_start_date >= ? 
                and f_mt_id in (3,5,13,17);');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated;
    deallocate prepare stmt;
    

    set @s = CONCAT('select count(*) into @ps_new from (select f_cont_id, min(mp_start_date) as min_start_date
            from ',db_name,'.membership_period
            where 
                f_mt_id in (3,5,13,17)
            group by f_cont_id
            having 
                min_start_date <= ?
                AND min_start_date >= ?) t;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated;
    deallocate prepare stmt;

    select @ps_payed_month-@ps_new into @ps_renew;


    set @s = CONCAT('select count(*) into @ps_go from (select mp.f_cont_id, max(mp.mp_end_date) as max_end_date
            from ',db_name,'.membership_period mp
            join ',db_name,'.contacts c on mp.f_cont_id=c.id
            where 
                mp.f_mt_id in (3,5,13,17)
            group by mp.f_cont_id
            having 
                max_end_date <= ?
                and max_end_date >= ?) t;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated;
    deallocate prepare stmt;


    set @s = CONCAT('select sum((select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice 
                        where 
                            oi_invoice_date <= ?
                            and oi_invoice_date >= ?) + 
               (select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice_archive 
                        where 
                            oi_invoice_date <= ?
                            and oi_invoice_date >= ?)) into @ps_invoice_set;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_end_formated,@month_start_formated;
    deallocate prepare stmt;
    

    set @s = CONCAT('select sum((select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice as oi
                            left join ',db_name,'.order_invoice_payment as pay on (pay.f_oi_id = oi.oi_id)
                        where 
                            oi_payment_date <= ?
                            and oi_payment_date >= ?
                            and pay.f_oi_id is null) +
                (select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice_archive as oi
                            left join ',db_name,'.order_invoice_payment as pay on (pay.f_oi_id = oi.oi_id)
                        WHERE 
                            oi_payment_date <= ?
                            and oi_payment_date >= ?
                            and pay.f_oi_id is null)) into @ps_invoice_payed;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_end_formated,@month_start_formated;
    deallocate prepare stmt;
    
    set @s = CONCAT('select sum((select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice as oi
                            inner join ',db_name,'.order_invoice_payment as pay on (pay.f_oi_id = oi.oi_id)
                        where 
                            oip_date <= ?
                            and oip_date >= ?) + 
                (select count(distinct f_ent_id) 
                        from ',db_name,'.order_invoice_archive as oi
                            inner join ',db_name,'.order_invoice_payment as pay on (pay.f_oi_id = oi.oi_id)
                        where 
                            oip_date <= ?
                            AND oip_date >= ?)) into @ps_invoice_payed_part;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_end_formated,@month_start_formated;
    deallocate prepare stmt;

    set @s = CONCAT('select count(*) into @ps_re_reg_month from
(select mp.f_cont_id,mp.mp_end_date,mp.mp_payment_date
            from ',db_name,'.membership_period mp
            join ',db_name,'.contacts c on mp.f_cont_id=c.id
            where 
                mp.f_mt_id in (3,5,13,17)
                        and mp.mp_payment_date<= ?
                        and mp.mp_payment_date>= ? 
and mp.f_cont_id in (select f_cont_id 
 from (select mp.f_cont_id, mp.mp_end_date as max_end_date
            from ',db_name,'.membership_period mp
            join ',db_name,'.contacts c on mp.f_cont_id=c.id
            where 
                mp.f_mt_id in (3,5,13,17)
and
mp.mp_end_date <= ?
                and mp.mp_end_date >= ?
            group by mp.f_cont_id) p)) t;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_end_formated,@month_start_formated;
    deallocate prepare stmt;


    set @s = CONCAT('select count(*) into @ps_re_reg_prev_month from
(select distinct( f_ent_id) 
                        from ',db_name,'.order_invoice as oi
                            left join ',db_name,'.order_invoice_payment 
                                as pay on (pay.f_oi_id = oi.oi_id)
                        where 
                            oi_payment_date <= ?
                            and oi_payment_date >= ?
                            and pay.f_oi_id is null
and f_ent_id
in (select f_cont_id 
 from (select mp.f_cont_id, mp.mp_end_date as max_end_date
            from ',db_name,'.membership_period mp
            join ',db_name,'.contacts c on mp.f_cont_id=c.id
            where 
                mp.f_mt_id in (3,5,13,17)
                and mp.mp_end_date <= ?
            group by mp.f_cont_id) p)) t;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_start_formated;
    deallocate prepare stmt;


set @s = CONCAT('select count(*) into @ps_re_reg_next_month from
(select distinct( f_ent_id) 
                        from ',db_name,'.order_invoice as oi
                            left join ',db_name,'.order_invoice_payment as pay on (pay.f_oi_id = oi.oi_id)
                        where 
                            oi_payment_date <= ?
                            and oi_payment_date >= ?
                            and pay.f_oi_id is null
and f_ent_id
in (select f_cont_id 
 from (select mp.f_cont_id, max(mp.mp_start_date) as max_start_date
            from ',db_name,'.membership_period mp
            join ',db_name,'.contacts c on mp.f_cont_id=c.id
            where 
                mp.f_mt_id in (3,5,13,17)
and mp.mp_end_date> ?
and mp.mp_end_date>mp.mp_payment_date
            group by mp.f_cont_id
            having 
count(mp_end_date)>1
) p)) t;');
    prepare stmt from @s;
    execute stmt using @month_end_formated,@month_start_formated,@month_end_formated;
    deallocate prepare stmt;


insert into pay_stat(country_name,ps_month, ps_all, ps_payed_month, ps_reg, ps_reg_self, ps_new, ps_renew, ps_go, ps_invoice_set, ps_invoice_payed, ps_invoice_payed_part, ps_payed, ps_re_reg_month, ps_re_reg_prev_month, ps_re_reg_next_month) 
	VALUES(country_name,concat(@month_formated,'-01'), @ps_all, @ps_payed_month, @ps_reg, @ps_reg_self, @ps_new, @ps_renew, @ps_go, @ps_invoice_set, @ps_invoice_payed, @ps_invoice_payed_part, @ps_payed, @ps_re_reg_month, @ps_re_reg_prev_month, @ps_re_reg_next_month);
  end if;
until done end repeat;
close month_list; 
end
GO

drop procedure table_sync
go
 
CREATE PROCEDURE table_sync ()
NOT DETERMINISTIC
CONTAINS SQL
begin
declare db_name varchar(55);
declare done integer default 0;
declare db_name_list cursor for 
select lr_db_name db_name from common.local_resource 
where lr_db_name in ('gemma','blr','ru','kz','uz','md','ge','az');
declare continue handler for sqlstate '02000' set done=1; 

drop table if exists life_cycle;
create table life_cycle as
select 
r.id id_region, 
r.reg_name region_name,
c.city id_city,
ct.name city_name,
c.name contact_name, 
k.kopfg_abb_ru ownership,
cast(c.tld as char(8)) tld,
c.address address,
c.chief_telfax  tel,
(select min(mp_payment_date) from gemma.membership_period where f_cont_id=c.id) start_registration,
(select max(mp_payment_date) from gemma.membership_period where f_cont_id=c.id) restart_registration,
(select max(mp_start_date) from gemma.membership_period where f_cont_id=c.id) start_access,
(select max(mp_end_date) from gemma.membership_period where f_cont_id=c.id) last_access,
(select max(cs_date) from gemma.click_stat where f_cont_id=c.id) dateMY,
m.mod_name mod_name,
mt.mt_code mt_code,
(select max(mp_sum) from gemma.membership_period where f_cont_id=c.id 
and mp_payment_date = (select max(mp_payment_date) from gemma.membership_period 
where f_cont_id=c.id)) mp_sum,
c.id id_contacts,
cr.eng_name country_name,
c.update_datetime update_datetime,
(select count(*) from gemma.goods where f_ent_id=c.id) kol_goods,
(select count(*) from gemma.services where f_ent_id=c.id) kol_services
from gemma.contacts c
left join gemma.regions r on  r.id=c.region
left join gemma.cities ct on c.city=ct.id
left join gemma.KOPFG k on k.kopfg_code=c.f_kopfg_code
left join gemma.moderator m on m.mod_id=c.f_mod_id
left join common.country cr on cr.id=c.country
left join common.membership_type mt on mt.mt_id=c.f_mt_id
where 1=0;

drop table if exists pay_tp;
create table pay_tp as
select 
country_name,
date_format(max_end_date,'%Y-%m-01') ps_month,
f_mt_id,
mt.mt_name,
count(*) kol_ps_go  
from 
(select ct.eng_name country_name, mp.f_cont_id,max(mp.mp_end_date) max_end_date,
case 
when ct.id=173 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 )=3 and mp.mp_sum=7999 then 999 /*Russia*/
else SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), ',', 1 ) end f_mt_id
from gemma.membership_period mp
join gemma.contacts c on mp.f_cont_id=c.id
join common.country ct on c.country=ct.id
join common.membership_type mt on mp.f_mt_id=mt.mt_id
where mp.f_mt_id in (3,5,13,17) and 1=0
group by mp.f_cont_id,ct.eng_name) t
join (select mt_id,mt_name from common.membership_type
union
select 999,'старт' from common.membership_type) mt on t.f_mt_id=mt.mt_id
group by country_name,date_format(max_end_date,'%Y-%m-01'),f_mt_id;

alter table pay_tp
	add column kol_ps_payed bigint null;

drop table if exists pay_stat;
create table pay_stat (country_name varchar(255),
ps_month date, ps_all int,ps_payed int, ps_payed_month int,
ps_reg int,ps_reg_self int,ps_new int,
ps_renew int,ps_go int,ps_invoice_set int,
ps_invoice_payed int,ps_invoice_payed_part int, 
ps_re_reg_month int, ps_re_reg_prev_month int, 
ps_re_reg_next_month int);

drop table if exists pay_doxod;
create table pay_doxod as
select 
ct.eng_name country_name,
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end f_mt_id,
date_format(mp.mp_payment_date,'%Y-%m-01') ps_month,
sum(mp.mp_sum) sum_TP,
case
when ct.id=173 and mt.mt_name='бизнес' and mp.mp_sum=999 then 'старт' /*Ukraine*/
when ct.id=71 and mt.mt_name='бизнес' and mp.mp_sum=23999 then 'старт' /*Kazakhstan*/
when ct.id=172 and mt.mt_name='бизнес' and mp.mp_sum=389999 then 'старт' /*Kazakhstan*/
when ct.id=110 and mt.mt_name='бизнес' and mp.mp_sum=1799 then 'старт' /*Moldova*/
when ct.id=49 and mt.mt_name='бизнес' and mp.mp_sum=249 then 'старт' /*Georgia*/
when ct.id=4 and mt.mt_name='бизнес' and mp.mp_sum=149 then 'старт' /*Azerbaijan*/
when ct.id=136 and mt.mt_name='бизнес' and mp.mp_sum=7999 then 'старт' /*Russia*/
else mt.mt_name end nazv_TP,
count(*) kol,
case 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=3 then 2 
when 
(
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=999 then 1
when mp.f_mt_id=5 then 3
when mp.f_mt_id=13 then 4
when mp.f_mt_id=17 then 5 else null end da_pofik,
reg.reg_name region,
sum(mp.mp_sum) div count(*) dox_kol
from gemma.membership_period mp 
join gemma.contacts c on mp.f_cont_id=c.id
join common.membership_type mt on mt.mt_id = mp.f_mt_id
join common.country ct on c.country=ct.id
join gemma.regions reg on c.region=reg.id
where  mp.f_mt_id in (3,5,13,17,999) and 1=0
Group by  
ct.eng_name,
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end,
date_format(mp.mp_payment_date,'%Y-%m-01'),
case
when ct.id=173 and mt.mt_name='бизнес' and mp.mp_sum=999 then 'старт' /*Ukraine*/
when ct.id=71 and mt.mt_name='бизнес' and mp.mp_sum=23999 then 'старт' /*Kazakhstan*/
when ct.id=172 and mt.mt_name='бизнес' and mp.mp_sum=389999 then 'старт' /*Kazakhstan*/
when ct.id=110 and mt.mt_name='бизнес' and mp.mp_sum=1799 then 'старт' /*Moldova*/
when ct.id=49 and mt.mt_name='бизнес' and mp.mp_sum=249 then 'старт' /*Georgia*/
when ct.id=4 and mt.mt_name='бизнес' and mp.mp_sum=149 then 'старт' /*Azerbaijan*/
when ct.id=136 and mt.mt_name='бизнес' and mp.mp_sum=7999 then 'старт' /*Russia*/
else mt.mt_name end,
case 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=3 then 2 
when 
(
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=999 then 1
when mp.f_mt_id=5 then 3
when mp.f_mt_id=13 then 4
when mp.f_mt_id=17 then 5 else null end,
reg.reg_name;

open db_name_list;
repeat 
  fetch db_name_list into db_name;
  if not done then
  set @s = CONCAT('insert into life_cycle
    select 
    r.id id_region, 
    r.reg_name region_name,
    c.city id_city,
    ct.name city_name,
    c.name contact_name, 
    k.kopfg_abb_ru ownership,
    cast(c.tld as char(8)) tld,
    c.address address,
    c.chief_telfax  tel,
    (select min(mp_payment_date) from ',db_name,'.membership_period where f_cont_id=c.id) start_registration,
    (select max(mp_payment_date) from ',db_name,'.membership_period where f_cont_id=c.id) restart_registration,
    (select max(mp_start_date) from ',db_name,'.membership_period where f_cont_id=c.id) start_access,
    (select max(mp_end_date) from ',db_name,'.membership_period where f_cont_id=c.id) last_access,
    (select max(cs_date) from ',db_name,'.click_stat where f_cont_id=c.id) dateMY,
    m.mod_name mod_name,
    mt.mt_code mt_code,
    (select max(mp_sum) from ',db_name,'.membership_period where f_cont_id=c.id 
    and mp_payment_date = (select max(mp_payment_date) from ',db_name,'.membership_period 
    where f_cont_id=c.id)) mp_sum,
    c.id id_contacts,
    cr.eng_name country_name,
    c.update_datetime update_datetime,
    (select count(*) from ',db_name,'.goods where f_ent_id=c.id) kol_goods,
    (select count(*) from ',db_name,'.services where f_ent_id=c.id) kol_services
    from ',db_name,'.contacts c
    left join ',db_name,'.regions r on  r.id=c.region
    left join ',db_name,'.cities ct on c.city=ct.id
    left join ',db_name,'.KOPFG k on k.kopfg_code=c.f_kopfg_code
    left join ',db_name,'.moderator m on m.mod_id=c.f_mod_id
    left join common.country cr on cr.id=c.country
    left join common.membership_type mt on mt.mt_id=c.f_mt_id');
    prepare stmt from @s;
    execute stmt;
    deallocate prepare stmt;

set @s = CONCAT('insert into pay_tp (country_name, ps_month, f_mt_id, mt_name, kol_ps_go)
        select 
country_name,
date_format(max_end_date,''%Y-%m-01'') ps_month,
f_mt_id,
mt.mt_name,
count(*) kol_ps_go  
from 
(select ct.eng_name country_name, mp.f_cont_id,max(mp.mp_end_date) max_end_date,
case 
when ct.id=173 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 )=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
else SUBSTRING_INDEX(GROUP_CONCAT(CAST(mp.f_mt_id AS CHAR) ORDER BY mp.mp_end_date desc), '','', 1 ) end f_mt_id
from ',db_name,'.membership_period mp
join ',db_name,'.contacts c on mp.f_cont_id=c.id
join common.country ct on c.country=ct.id
join common.membership_type mt on mp.f_mt_id=mt.mt_id
where mp.f_mt_id in (3,5,13,17)
group by mp.f_cont_id,ct.eng_name) t
join (select mt_id,mt_name from common.membership_type
union
select 999,''старт'' from common.membership_type) mt on t.f_mt_id=mt.mt_id
group by country_name,date_format(max_end_date,''%Y-%m-01''),f_mt_id');
    prepare stmt from @s;
    execute stmt;
    deallocate prepare stmt;

    set @s = CONCAT('insert into pay_doxod
    select 
ct.eng_name country_name,
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end f_mt_id,
date_format(mp.mp_payment_date,''%Y-%m-01'') ps_month,
sum(mp.mp_sum) sum_TP,
case
when ct.id=173 and mt.mt_name=''бизнес'' and mp.mp_sum=999 then ''старт'' /*Ukraine*/
when ct.id=71 and mt.mt_name=''бизнес'' and mp.mp_sum=23999 then ''старт'' /*Kazakhstan*/
when ct.id=172 and mt.mt_name=''бизнес'' and mp.mp_sum=389999 then ''старт'' /*Kazakhstan*/
when ct.id=110 and mt.mt_name=''бизнес'' and mp.mp_sum=1799 then ''старт'' /*Moldova*/
when ct.id=49 and mt.mt_name=''бизнес'' and mp.mp_sum=249 then ''старт'' /*Georgia*/
when ct.id=4 and mt.mt_name=''бизнес'' and mp.mp_sum=149 then ''старт'' /*Azerbaijan*/
when ct.id=136 and mt.mt_name=''бизнес'' and mp.mp_sum=7999 then ''старт'' /*Russia*/
else mt.mt_name end nazv_TP,
count(*) kol,
case 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=3 then 2 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=999 then 1
when mp.f_mt_id=5 then 3
when mp.f_mt_id=13 then 4
when mp.f_mt_id=17 then 5 else null end da_pofik,
reg.reg_name region,
sum(mp.mp_sum) div count(*) dox_kol
from ',db_name,'.membership_period mp 
join ',db_name,'.contacts c on mp.f_cont_id=c.id
join common.membership_type mt on mt.mt_id = mp.f_mt_id
join common.country ct on c.country=ct.id
join ',db_name,'.regions reg on c.region=reg.id
where  mp.f_mt_id in (3,5,13,17,999)
Group by  
ct.eng_name,
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end,
date_format(mp.mp_payment_date,''%Y-%m-01''),
case
when ct.id=173 and mt.mt_name=''бизнес'' and mp.mp_sum=999 then ''старт'' /*Ukraine*/
when ct.id=71 and mt.mt_name=''бизнес'' and mp.mp_sum=23999 then ''старт'' /*Kazakhstan*/
when ct.id=172 and mt.mt_name=''бизнес'' and mp.mp_sum=389999 then ''старт'' /*Kazakhstan*/
when ct.id=110 and mt.mt_name=''бизнес'' and mp.mp_sum=1799 then ''старт'' /*Moldova*/
when ct.id=49 and mt.mt_name=''бизнес'' and mp.mp_sum=249 then ''старт'' /*Georgia*/
when ct.id=4 and mt.mt_name=''бизнес'' and mp.mp_sum=149 then ''старт'' /*Azerbaijan*/
when ct.id=136 and mt.mt_name=''бизнес'' and mp.mp_sum=7999 then ''старт'' /*Russia*/
else mt.mt_name end,
case 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=3 then 2 
when (
case 
when ct.id=173 and mp.f_mt_id=3 and mp.mp_sum=999 then 999 /*Ukraine*/
when ct.id=71 and mp.f_mt_id=3 and mp.mp_sum=23999 then 999 /*Kazakhstan*/
when ct.id=172 and mp.f_mt_id=3 and mp.mp_sum=389999 then 999 /*Kazakhstan*/
when ct.id=110 and mp.f_mt_id=3 and mp.mp_sum=1799 then 999 /*Moldova*/
when ct.id=49 and mp.f_mt_id=3 and mp.mp_sum=249 then 999 /*Georgia*/
when ct.id=4 and mp.f_mt_id=3 and mp.mp_sum=149 then 999 /*Azerbaijan*/
when ct.id=136 and mp.f_mt_id=3 and mp.mp_sum=7999 then 999 /*Russia*/
else mp.f_mt_id end
)=999 then 1
when mp.f_mt_id=5 then 3
when mp.f_mt_id=13 then 4
when mp.f_mt_id=17 then 5 else null end,
reg.reg_name');
    prepare stmt from @s;
    execute stmt;
    deallocate prepare stmt;

    call get_pay_stat(db_name);
    
    end if;
until done end repeat;
close db_name_list; 

ALTER TABLE pay_tp MODIFY COLUMN ps_month date NULL;
ALTER TABLE pay_doxod MODIFY COLUMN ps_month date NULL;
ALTER TABLE pay_stat MODIFY COLUMN ps_month date NULL;

ALTER TABLE pay_tp add (da_pofik int NULL, procent numeric(8,6) null);

drop table if exists pay_tp_tmp;

create table pay_tp_tmp as
select t1.country_name,t1.ps_month,t1.f_mt_id,
case 
when t1.f_mt_id=3 then 2 
when t1.f_mt_id=999 then 1
when t1.f_mt_id=5 then 3
when t1.f_mt_id=13 then 4
when t1.f_mt_id=17 then 5 else null end da_pofik,
FORMAT(t1.kol_ps_go/((t1.kol_ps_payed+t2.kol_ps_payed)/2),6) procent
from pay_tp t1
left join pay_tp t2 on t1.country_name=t2.country_name 
and date_add(t1.ps_month, interval -1 month)=t2.ps_month 
and t1.f_mt_id=t2.f_mt_id;

update pay_tp p set da_pofik = (select da_pofik from pay_tp_tmp t 
where t.country_name=p.country_name and t.ps_month=p.ps_month and t.f_mt_id=p.f_mt_id),
procent = (select procent from pay_tp_tmp t 
where t.country_name=p.country_name and t.ps_month=p.ps_month and t.f_mt_id=p.f_mt_id);

drop table pay_tp_tmp;

CREATE INDEX idx_life_cycle_id_region USING BTREE 
	ON life_cycle(id_region);

CREATE INDEX idx_life_cycle_id_city USING BTREE 
	ON life_cycle(id_city);

CREATE INDEX idx_life_cycle_tld USING BTREE 
	ON life_cycle(tld);

CREATE INDEX idx_life_cycle_country_name USING BTREE 
	ON life_cycle(country_name);

CREATE INDEX idx_pay_stat_country_name USING BTREE 
	ON pay_stat(country_name);

CREATE INDEX idx_pay_tp_country_name USING BTREE 
	ON pay_tp(country_name);

CREATE INDEX idx_pay_tp_ps_month USING BTREE 
	ON pay_tp(ps_month);

CREATE INDEX idx_pay_doxod_country_name USING BTREE 
	ON pay_doxod(country_name);

CREATE INDEX idx_pay_doxod_ps_month USING BTREE 
	ON pay_doxod(ps_month);


OPTIMIZE TABLE life_cycle,pay_stat,pay_tp,pay_doxod;
end
GO
