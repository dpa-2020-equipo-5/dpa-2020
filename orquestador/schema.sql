--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2
-- Dumped by pg_dump version 12.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: clean; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA clean;


--
-- Name: raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA raw;


--
-- Name: testing; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA testing;


--
-- Name: transformed; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA transformed;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: inspections; Type: TABLE; Schema: clean; Owner: -
--

CREATE TABLE clean.inspections (
    centername character varying,
    legalname character varying,
    building character varying,
    street character varying,
    borough character varying,
    zipcode character varying,
    phone character varying,
    permitexp character varying,
    status character varying,
    agerange character varying,
    maximumcapacity character varying,
    dc_id character varying,
    programtype character varying,
    facilitytype character varying,
    childcaretype character varying,
    bin character varying,
    violationratepercent character varying,
    totaleducationalworkers character varying,
    averagetotaleducationalworkers character varying,
    publichealthhazardviolationrate character varying,
    criticalviolationrate character varying,
    inspectiondate character varying,
    regulationsummary character varying,
    violationcategory character varying,
    healthcodesubsection character varying,
    violationstatus character varying,
    inspectionsummaryresult character varying,
    permitnumber character varying,
    url character varying,
    datepermitted character varying,
    actual character varying,
    violationavgratepercent character varying,
    averagepublichealthhazardiolationrate character varying,
    avgcriticalviolationrate character varying
);


--
-- Name: metadata; Type: TABLE; Schema: clean; Owner: -
--

CREATE TABLE clean.metadata (
    executed_at timestamp without time zone,
    task_params character varying,
    record_count integer,
    execution_user character varying,
    source_ip character varying,
    database_name character varying,
    database_schema character varying,
    database_table character varying,
    database_user character varying,
    vars character varying,
    script_tag character varying
);


--
-- Name: table_updates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.table_updates (
    update_id text NOT NULL,
    target_table text,
    inserted timestamp without time zone DEFAULT now()
);


--
-- Name: inspections; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.inspections (
    inspection json
);


--
-- Name: metadata; Type: TABLE; Schema: raw; Owner: -
--

CREATE TABLE raw.metadata (
    executed_at timestamp without time zone,
    task_params character varying,
    record_count integer,
    execution_user character varying,
    source_ip character varying,
    database_name character varying,
    database_schema character varying,
    database_table character varying,
    database_user character varying,
    vars character varying,
    script_tag character varying
);


--
-- Name: extractions; Type: TABLE; Schema: testing; Owner: -
--

CREATE TABLE testing.extractions (
    test character varying,
    ran_at timestamp without time zone,
    params character varying,
    status character varying,
    note character varying
);


--
-- Name: feature_engineering; Type: TABLE; Schema: testing; Owner: -
--

CREATE TABLE testing.feature_engineering (
    test character varying,
    ran_at timestamp without time zone,
    params character varying,
    status character varying,
    note character varying
);


--
-- Name: centers; Type: TABLE; Schema: transformed; Owner: -
--

CREATE TABLE transformed.centers (
    centername character varying,
    legalname character varying,
    building character varying,
    street character varying,
    zipcode character varying,
    phone character varying,
    permitnumber character varying,
    permitexp character varying,
    status character varying,
    agerange character varying,
    maximumcapacity character varying,
    dc_id character varying,
    childcaretype character varying,
    bin character varying,
    url character varying,
    datepermitted character varying,
    actual character varying,
    violationratepercent character varying,
    violationavgratepercent character varying,
    totaleducationalworkers character varying,
    averagetotaleducationalworkers character varying,
    publichealthhazardviolationrate character varying,
    averagepublichealthhazardiolationrate character varying,
    criticalviolationrate character varying,
    avgcriticalviolationrate character varying,
    programtype_infant_toddler character varying,
    programtype_preschool character varying,
    facilitytype_gdc character varying,
    facilitytype_sbcc character varying,
    borough_bronx character varying,
    borough_brooklyn character varying,
    borough_manhattan character varying,
    borough_queens character varying,
    borough_staten_island character varying,
    programtype_all_age_camp character varying,
    facilitytype_camp character varying,
    programtype_school_age_camp character varying,
    programtype_preschool_camp character varying
);


--
-- Name: inspections; Type: TABLE; Schema: transformed; Owner: -
--

CREATE TABLE transformed.inspections (
    center_id character varying,
    inspectiondate character varying,
    regulationsummary character varying,
    healthcodesubsection character varying,
    violationstatus character varying,
    borough character varying,
    reason character varying,
    result_1_passed_inspection character varying,
    result_1_passed_inspection_with_no_violations character varying,
    result_1_reinspection_not_required character varying,
    result_1_reinspection_required character varying,
    result_2_fines_pending character varying,
    result_2_violations_corrected_at_time_of_inspection character varying,
    inspection_year character varying,
    inspection_month character varying,
    inspection_day_name character varying,
    violationcategory_critical character varying,
    violationcategory_general character varying,
    violationcategory_nan character varying,
    violationcategory_public_health_hazard character varying,
    dias_ultima_inspeccion character varying,
    violaciones_hist_salud_publica character varying,
    violaciones_2019_salud_publica character varying,
    violaciones_hist_criticas character varying,
    violaciones_2019_criticas character varying,
    ratio_violaciones_hist character varying,
    ratio_violaciones_2019 character varying,
    prom_violaciones_hist_borough character varying,
    prom_violaciones_2019_borough character varying,
    ratio_violaciones_hist_sp character varying,
    ratio_violaciones_2019_sp character varying,
    ratio_violaciones_hist_criticas character varying,
    ratio_violaciones_2019_criticas character varying,
    result_1_previously_cited_violations_corrected character varying
);


--
-- Name: metadata; Type: TABLE; Schema: transformed; Owner: -
--

CREATE TABLE transformed.metadata (
    executed_at timestamp without time zone,
    task_params character varying,
    record_count integer,
    execution_user character varying,
    source_ip character varying,
    database_name character varying,
    database_schema character varying,
    database_table character varying,
    database_user character varying,
    vars character varying,
    script_tag character varying
);


--
-- Name: update_centers_metadata; Type: TABLE; Schema: transformed; Owner: -
--

CREATE TABLE transformed.update_centers_metadata (
    executed_at timestamp without time zone,
    task_params character varying,
    record_count integer,
    execution_user character varying,
    source_ip character varying,
    database_name character varying,
    database_schema character varying,
    database_table character varying,
    database_user character varying,
    vars character varying,
    script_tag character varying
);


--
-- Name: table_updates table_updates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.table_updates
    ADD CONSTRAINT table_updates_pkey PRIMARY KEY (update_id);


--
-- PostgreSQL database dump complete
--

