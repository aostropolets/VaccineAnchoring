/**************
Creating target and comparator cohorts for estimating the effect of anchoring on baseline patient characteristics
**************/

/************
1. covid vs date
************/

select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
        where drug_concept_id in (702866,724905,724906,724907,766231,766232,766233,766234,766235,766236,766237,766238,766239,766240,766241,1219271,1227568,1230962,1230963,35891484,35891603,35891695,35891709,35895095,35895096,35895097,35895190,35895191,35895192,36388974,36391504,37003431,37003432,37003433,37003434,37003435,37003436,37003516,37003517,37003518,42796198,42796343,42797615,42797616)
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/

select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
         from @cdm_database_schema.person p1
                  left join #target t1
                            on p1.person_id = t1.person_id
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
         where t1.person_id is null
     ) c1
     on t1.year_of_birth = c1.year_of_birth
         and t1.gender_concept_id = c1.gender_concept_id
         and t1.index_date >= dateadd(day,365,c1.observation_period_start_date)
         and t1.index_date <= c1.observation_period_end_date
;

select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, c_observation_period_start_date, c_observation_period_end_date,
       row_number() over (partition by t_person_id order by c_person_id asc) as trn,
       row_number() over (partition by c_person_id order by t_person_id asc) as crn
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
                 inner join #target t1
                            on m1.t_person_id = t1.person_id
    ) t1
where rn1 <= 100;


create table #target_comparator_match
(
    t_person_id bigint,
    c_person_id bigint
);

/*****
keep running the following insert statement until no records are inserted
*****/

insert into #target_comparator_match (t_person_id, c_person_id)
select min(c1.t_person_id) as t_person_id, c1.c_person_id
from
    (
        select c2.t_person_id, min(trn) as min_trn, t0.min_crn
        from
            (
                select c1.t_person_id,
                       min(crn) as min_crn
                from #candidate_target_comparator_match_sample c1
                         left join (select t_person_id, count(c_person_id) as num_matches from #target_comparator_match group by t_person_id) m1
                                   on c1.t_person_id = m1.t_person_id
                                       and m1.num_matches = 1  /* set this for the number of comparators per target, will be variable-ratio matching */
                         left join #target_comparator_match m2
                                   on c1.c_person_id = m2.c_person_id
                where m1.t_person_id is null
                  and m2.c_person_id is null
                group by c1.t_person_id
            ) t0
                inner join (select c0.* from
                #candidate_target_comparator_match_sample c0
                    left join #target_comparator_match m2
                              on c0.c_person_id = m2.c_person_id
                            where m2.c_person_id is null) c2
                           on t0.t_person_id = c2.t_person_id
                               and t0.min_crn = c2.crn
        group by c2.t_person_id, t0.min_crn
    ) t1
        inner join (select c0.* from
        #candidate_target_comparator_match_sample c0
            left join #target_comparator_match m2
                      on c0.c_person_id = m2.c_person_id
                    where m2.c_person_id is null) c1
                   on t1.t_person_id = c1.t_person_id
                       and t1.min_trn = c1.trn
                       and t1.min_crn = c1.crn
group by c1.c_person_id;


/******
put target and comparator into standard COHORT table structure
******/
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (1,2);
insert into  @target_database_schema.@target_cohort_table
    select *
from (
         select 1                                as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id

         union

         select  2                                as cohort_definition_id,
                c1.c_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id
     ) a
;


/************
2. influenza vs date
*************/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;

select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
                 join @cdm_database_schema.concept_ancestor ca
                      on drug_concept_id = descendant_concept_id
        where ancestor_concept_id  in (40213141,40213142,40213143,40213144,40213146,40213147,40213148,40213150,40213153,40213154,40213149,40213156,40213159,
                                       40213155,40213152,40213157,40213151,40213327)
          and (drug_exposure_start_date >= DATEFROMPARTS(2017, 6, 1) and drug_exposure_start_date <= DATEFROMPARTS(2018, 5, 31))
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
         from @cdm_database_schema.person p1
                  left join #target t1
                            on p1.person_id = t1.person_id
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
         where t1.person_id is null
     ) c1
     on t1.year_of_birth = c1.year_of_birth
         and t1.gender_concept_id = c1.gender_concept_id
         and t1.index_date >= dateadd(day,365,c1.observation_period_start_date)
         and t1.index_date <= c1.observation_period_end_date
;

IF OBJECT_ID('tempdb..#candidate_target_comparator_match_sample;') IS NOT NULL DROP TABLE #candidate_target_comparator_match_sample
;
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, c_observation_period_start_date, c_observation_period_end_date,
       row_number() over (partition by t_person_id order by c_person_id asc) as trn,
       row_number() over (partition by c_person_id order by t_person_id asc) as crn
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
                 inner join #target t1
                            on m1.t_person_id = t1.person_id
    ) t1
where rn1 <= 100;


IF OBJECT_ID('tempdb..#target_comparator_match;') IS NOT NULL DROP TABLE #target_comparator_match
;
create table #target_comparator_match
(
    t_person_id bigint,
    c_person_id bigint
);

/*****
keep running the following insert statement until no records are inserted
*****/

insert into #target_comparator_match (t_person_id, c_person_id)
select min(c1.t_person_id) as t_person_id, c1.c_person_id
from
    (
        select c2.t_person_id, min(trn) as min_trn, t0.min_crn
        from
            (
                select c1.t_person_id,
                       min(crn) as min_crn
                from #candidate_target_comparator_match_sample c1
                         left join (select t_person_id, count(c_person_id) as num_matches from #target_comparator_match group by t_person_id) m1
                                   on c1.t_person_id = m1.t_person_id
                                       and m1.num_matches = 1  /* set this for the number of comparators per target, will be variable-ratio matching */
                         left join #target_comparator_match m2
                                   on c1.c_person_id = m2.c_person_id
                where m1.t_person_id is null
                  and m2.c_person_id is null
                group by c1.t_person_id
            ) t0
                inner join (select c0.* from
                #candidate_target_comparator_match_sample c0
                    left join #target_comparator_match m2
                              on c0.c_person_id = m2.c_person_id
                            where m2.c_person_id is null) c2
                           on t0.t_person_id = c2.t_person_id
                               and t0.min_crn = c2.crn
        group by c2.t_person_id, t0.min_crn
    ) t1
        inner join (select c0.* from
        #candidate_target_comparator_match_sample c0
            left join #target_comparator_match m2
                      on c0.c_person_id = m2.c_person_id
                    where m2.c_person_id is null) c1
                   on t1.t_person_id = c1.t_person_id
                       and t1.min_trn = c1.trn
                       and t1.min_crn = c1.crn
group by c1.c_person_id;

/******
put target and comparator into standard COHORT table structure
******/
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (3,4);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 3                                as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id

         union

         select  4                                as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.t_index_date                  as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id
     ) a
;


/*****
3. covid vs visit
********/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
        where drug_concept_id in (702866,724905,724906,724907,766231,766232,766233,766234,766235,766236,766237,766238,766239,766240,766241,1219271,1227568,1230962,1230963,35891484,35891603,35891695,35891709,35895095,35895096,35895097,35895190,35895191,35895192,36388974,36391504,37003431,37003432,37003433,37003434,37003435,37003436,37003516,37003517,37003518,42796198,42796343,42797615,42797616)
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date,
       visit_start_date
into #candidate_target_comparator_match
from #target t1

         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date, visit_start_date
         from @cdm_database_schema.person p1
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join @cdm_database_schema.visit_occurrence v
                             on p1.person_id = v.person_id
                  left join #target t1
                            on p1.person_id = t1.person_id

         where t1.person_id is null
     ) c1
     on t1.year_of_birth = c1.year_of_birth
         and t1.gender_concept_id = c1.gender_concept_id
         and t1.index_date >= dateadd(day,365,c1.observation_period_start_date)
         and t1.index_date = visit_start_date
;

IF OBJECT_ID('tempdb..#candidate_target_comparator_match_sample;') IS NOT NULL DROP TABLE #candidate_target_comparator_match_sample
;
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, c_observation_period_start_date, c_observation_period_end_date,
       row_number() over (partition by t_person_id order by c_person_id asc) as trn,
       row_number() over (partition by c_person_id order by t_person_id asc) as crn
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
                 inner join #target t1
                            on m1.t_person_id = t1.person_id
    ) t1
where rn1 <= 100;

IF OBJECT_ID('tempdb..#target_comparator_match;') IS NOT NULL DROP TABLE #target_comparator_match
;
create table #target_comparator_match
(
    t_person_id bigint,
    c_person_id bigint
);

/*****
keep running the following insert statement until no records are inserted
*****/

insert into #target_comparator_match (t_person_id, c_person_id)
select min(c1.t_person_id) as t_person_id, c1.c_person_id
from
    (
        select c2.t_person_id, min(trn) as min_trn, t0.min_crn
        from
            (
                select c1.t_person_id,
                       min(crn) as min_crn
                from #candidate_target_comparator_match_sample c1
                         left join (select t_person_id, count(c_person_id) as num_matches from #target_comparator_match group by t_person_id) m1
                                   on c1.t_person_id = m1.t_person_id
                                       and m1.num_matches = 1  /* set this for the number of comparators per target, will be variable-ratio matching */
                         left join #target_comparator_match m2
                                   on c1.c_person_id = m2.c_person_id
                where m1.t_person_id is null
                  and m2.c_person_id is null
                group by c1.t_person_id
            ) t0
                inner join (select c0.* from
                #candidate_target_comparator_match_sample c0
                    left join #target_comparator_match m2
                              on c0.c_person_id = m2.c_person_id
                            where m2.c_person_id is null) c2
                           on t0.t_person_id = c2.t_person_id
                               and t0.min_crn = c2.crn
        group by c2.t_person_id, t0.min_crn
    ) t1
        inner join (select c0.* from
        #candidate_target_comparator_match_sample c0
            left join #target_comparator_match m2
                      on c0.c_person_id = m2.c_person_id
                    where m2.c_person_id is null) c1
                   on t1.t_person_id = c1.t_person_id
                       and t1.min_trn = c1.trn
                       and t1.min_crn = c1.crn
group by c1.c_person_id;


/******
put target and comparator into standard COHORT table structure
******/

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (5,6);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 5                                as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id

         union

         select  6                                as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.t_index_date                  as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id
     ) a
;

/************
4. influenza vs visit
************/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
                 join @cdm_database_schema.concept_ancestor ca
                      on drug_concept_id = descendant_concept_id
        where ancestor_concept_id  in (40213141,40213142,40213143,40213144,40213146,40213147,40213148,40213150,40213153,40213154,40213149,40213156,40213159,
                                       40213155,40213152,40213157,40213151,40213327)
          and (drug_exposure_start_date >= DATEFROMPARTS(2017, 6, 1) and drug_exposure_start_date <= DATEFROMPARTS(2018, 5, 31))
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date, visit_start_date
         from @cdm_database_schema.person p1
                  left join #target t1
                            on p1.person_id = t1.person_id
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join @cdm_database_schema.visit_occurrence v
                             on p1.person_id = v.person_id
         where t1.person_id is null
     ) c1
     on t1.year_of_birth = c1.year_of_birth
         and t1.gender_concept_id = c1.gender_concept_id
         and t1.index_date >= dateadd(day,365,c1.observation_period_start_date)
         and t1.index_date = visit_start_date
;


IF OBJECT_ID('tempdb..#candidate_target_comparator_match_sample;') IS NOT NULL DROP TABLE #candidate_target_comparator_match_sample
;
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, c_observation_period_start_date, c_observation_period_end_date,
       row_number() over (partition by t_person_id order by c_person_id asc) as trn,
       row_number() over (partition by c_person_id order by t_person_id asc) as crn
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
                 inner join #target t1
                            on m1.t_person_id = t1.person_id
    ) t1
where rn1 <= 100;

IF OBJECT_ID('tempdb..#target_comparator_match;') IS NOT NULL DROP TABLE #target_comparator_match
;
create table #target_comparator_match
(
    t_person_id bigint,
    c_person_id bigint
);

/*****
keep running the following insert statement until no records are inserted
*****/

insert into #target_comparator_match (t_person_id, c_person_id)
select min(c1.t_person_id) as t_person_id, c1.c_person_id
from
    (
        select c2.t_person_id, min(trn) as min_trn, t0.min_crn
        from
            (
                select c1.t_person_id,
                       min(crn) as min_crn
                from #candidate_target_comparator_match_sample c1
                         left join (select t_person_id, count(c_person_id) as num_matches from #target_comparator_match group by t_person_id) m1
                                   on c1.t_person_id = m1.t_person_id
                                       and m1.num_matches = 1  /* set this for the number of comparators per target, will be variable-ratio matching */
                         left join #target_comparator_match m2
                                   on c1.c_person_id = m2.c_person_id
                where m1.t_person_id is null
                  and m2.c_person_id is null
                group by c1.t_person_id
            ) t0
                inner join (select c0.* from
                #candidate_target_comparator_match_sample c0
                    left join #target_comparator_match m2
                              on c0.c_person_id = m2.c_person_id
                            where m2.c_person_id is null) c2
                           on t0.t_person_id = c2.t_person_id
                               and t0.min_crn = c2.crn
        group by c2.t_person_id, t0.min_crn
    ) t1
        inner join (select c0.* from
        #candidate_target_comparator_match_sample c0
            left join #target_comparator_match m2
                      on c0.c_person_id = m2.c_person_id
                    where m2.c_person_id is null) c1
                   on t1.t_person_id = c1.t_person_id
                       and t1.min_trn = c1.trn
                       and t1.min_crn = c1.crn
group by c1.c_person_id;


/******
put target and comparator into standard COHORT table structure
******/
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (7,8);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 7                                as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id

         union

         select  8                                as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.t_index_date                  as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1
                  inner join #target_comparator_match m1
                             on c1.t_person_id = m1.t_person_id
                                 and c1.c_person_id = m1.c_person_id
     ) a
;


/********
5. covid vs visit, same patients
********/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
        where drug_concept_id in (702866,724905,724906,724907,766231,766232,766233,766234,766235,766236,766237,766238,766239,766240,766241,1219271,1227568,1230962,1230963,35891484,35891603,35891695,35891709,35895095,35895096,35895097,35895190,35895191,35895192,36388974,36391504,37003431,37003432,37003433,37003434,37003435,37003436,37003516,37003517,37003518,42796198,42796343,42797615,42797616)
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date,
       visit_start_date
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date, visit_start_date
         from @cdm_database_schema.person p1
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join @cdm_database_schema.visit_occurrence v
                             on p1.person_id = v.person_id
                  inner join #target t1
                            on p1.person_id = t1.person_id
     ) c1
     on t1.person_id = c1.person_id
         and visit_start_date >= dateadd(day,365,c1.observation_period_start_date)
         and datediff(day,visit_start_date, t1.index_date) > 180
		 and datediff(day,visit_start_date, t1.index_date) < 450
;

IF OBJECT_ID('tempdb..#target_comparator_match_sample;') IS NOT NULL DROP TABLE #target_comparator_match_sample
;
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, visit_start_date, c_observation_period_end_date
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
    ) t1
where rn1 = 1;


/******
put target and comparator into standard COHORT table structure
******/
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (9,10);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 9                                as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

         union

         select  10                               as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.visit_start_date              as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

     ) a
;


/*****
6. influenza vs visit, same patients
********/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure de
                 join @cdm_database_schema.concept_ancestor ca
                      on drug_concept_id = descendant_concept_id
        where ancestor_concept_id  in (40213141,40213142,40213143,40213144,40213146,40213147,40213148,40213150,40213153,40213154,40213149,40213156,40213159,
                                       40213155,40213152,40213157,40213151,40213327)
          and (drug_exposure_start_date >= DATEFROMPARTS(2017, 6, 1) and drug_exposure_start_date <= DATEFROMPARTS(2018, 5, 31))
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target

*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;

select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date,
       visit_start_date
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date, visit_start_date
         from @cdm_database_schema.person p1
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join @cdm_database_schema.visit_occurrence v
                             on p1.person_id = v.person_id
                  inner join #target t1
                             on p1.person_id = t1.person_id
     ) c1
     on t1.person_id = c1.person_id
         and visit_start_date >= dateadd(day,365,c1.observation_period_start_date)
		 and datediff(day,visit_start_date, t1.index_date) > 180
		 and datediff(day,visit_start_date, t1.index_date) < 450
;


IF OBJECT_ID('tempdb..#candidate_target_comparator_match_sample;') IS NOT NULL DROP TABLE #candidate_target_comparator_match_sample
;
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, visit_start_date, c_observation_period_end_date
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
               row_number() over (partition by m1.t_person_id order by rand()) as rn1
        from #candidate_target_comparator_match m1
    ) t1
where rn1 = 1;

/******
put target and comparator into standard COHORT table structure
******/

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (11,12);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 11                               as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

         union

         select  12                               as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.visit_start_date              as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

     ) a
;

/*****
7. covid vs date, same patients
********/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure
        where drug_concept_id in (702866,724905,724906,724907,766231,766232,766233,766234,766235,766236,766237,766238,766239,766240,766241,1219271,1227568,1230962,1230963,35891484,35891603,35891695,35891709,35895095,35895096,35895097,35895190,35895191,35895192,36388974,36391504,37003431,37003432,37003433,37003434,37003435,37003436,37003516,37003517,37003518,42796198,42796343,42797615,42797616)
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target
*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date,
       rd_start, rd_end
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id,
         dateadd(day,-180, t1.index_date) as rd_end,
         case when t1.index_date < dateadd(day,815,op1.observation_period_start_date) --365d + 450d
              then dateadd(day,365,op1.observation_period_start_date)
              else dateadd(day,-450, t1.index_date) end as rd_start,
         op1.observation_period_start_date,
         op1.observation_period_end_date
         from @cdm_database_schema.person p1
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join #target t1
                            on p1.person_id = t1.person_id
     ) c1
     on t1.person_id = c1.person_id
  where rd_start<rd_end
;

-- sampling random date from a range
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, rd_dt, c_observation_period_end_date
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
                dateadd(day,
               rand(checksum(newid()))*(1+datediff(day, rd_start, rd_end)),
               rd_start) as rd_dt
        from #candidate_target_comparator_match m1
    ) t1
;

/******
put target and comparator into standard COHORT table structure
******/
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (13,14);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 13                               as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

         union

         select  14                               as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.rd_dt                         as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

     ) a
;


/*****
8. influenza vs date, same patients
********/
IF OBJECT_ID('tempdb..#target;') IS NOT NULL DROP TABLE #target
;
select t1.person_id, t1.index_date, p1.year_of_birth, p1.gender_concept_id, op1.observation_period_start_date, op1.observation_period_end_date
into #target from
    (
        select person_id, min(drug_exposure_start_date) as index_date
        from @cdm_database_schema.drug_exposure de
                 join @cdm_database_schema.concept_ancestor ca
                      on drug_concept_id = descendant_concept_id
        where ancestor_concept_id  in (40213141,40213142,40213143,40213144,40213146,40213147,40213148,40213150,40213153,40213154,40213149,40213156,40213159,
                                       40213155,40213152,40213157,40213151,40213327)
          and (drug_exposure_start_date >= DATEFROMPARTS(2017, 6, 1) and drug_exposure_start_date <= DATEFROMPARTS(2018, 5, 31))
        group by person_id
    ) t1
        inner join @cdm_database_schema.observation_period op1
                   on t1.person_id = op1.person_id
                       and t1.index_date >= dateadd(day,365,op1.observation_period_start_date)
                       and t1.index_date <= op1.observation_period_end_date
        inner join @cdm_database_schema.person p1
                   on t1.person_id = p1.person_id
;

/*******
find all eligible age*sex*index date candidate comparators, per target

*********/
IF OBJECT_ID('tempdb..#candidate_target_comparator_match;') IS NOT NULL DROP TABLE #candidate_target_comparator_match
;
select t1.person_id as t_person_id, t1.year_of_birth as t_year_of_birth, t1.gender_concept_id as t_gender_concept_id, t1.index_date as t_index_date, t1.observation_period_start_date as t_observation_period_start_date, t1.observation_period_end_date as t_observation_period_end_date,
       c1.person_id  as c_person_id, c1.year_of_birth as c_year_of_birth, c1.gender_concept_id as c_gender_concept_id, c1.observation_period_start_date as c_observation_period_start_date, c1.observation_period_end_date as c_observation_period_end_date,
       rd_start, rd_end
into #candidate_target_comparator_match
from #target t1
         inner join
     (
         select p1.person_id, p1.year_of_birth, p1.gender_concept_id,
         dateadd(day,-180, t1.index_date) as rd_end,
         case when t1.index_date < dateadd(day,815,op1.observation_period_start_date) --365d + 450d
              then dateadd(day,365,op1.observation_period_start_date)
              else dateadd(day,-450, t1.index_date) end as rd_start,
         op1.observation_period_start_date,
         op1.observation_period_end_date
         from @cdm_database_schema.person p1
                  inner join @cdm_database_schema.observation_period op1
                             on p1.person_id = op1.person_id
                  inner join #target t1
                            on p1.person_id = t1.person_id
     ) c1
     on t1.person_id = c1.person_id
  where rd_start<rd_end
;

-- sampling random date from a range
select t_person_id, t_year_of_birth, t_gender_concept_id, t_index_date, t_observation_period_start_date, t_observation_period_end_date,
       c_person_id, c_year_of_birth, c_gender_concept_id, rd_dt, c_observation_period_end_date
into #candidate_target_comparator_match_sample
from
    (
        select m1.*,
                dateadd(day,
               rand(checksum(newid()))*(1+datediff(day, rd_start, rd_end)),
               rd_start) as rd_dt
        from #candidate_target_comparator_match m1
    ) t1
;

/******
put target and comparator into standard COHORT table structure
******/

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id  in (15,16);
insert into  @target_database_schema.@target_cohort_table
select *
from (
         select 15                               as cohort_definition_id,
                c1.t_person_id                   as subject_id,
                c1.t_index_date                  as cohort_start_date,
                c1.t_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

         union

         select  16                               as cohort_definition_id,
                 c1.c_person_id                   as subject_id,
                 c1.rd_dt                         as cohort_start_date,
                 c1.c_observation_period_end_date as cohort_end_date
         from #candidate_target_comparator_match_sample c1

     ) a
;
