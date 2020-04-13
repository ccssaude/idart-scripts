ALTER TABLE stock ADD COLUMN IF NOT EXISTS numeroGuia character  varying(255) default 'Sem Info.';
ALTER TABLE drug DROP CONSTRAINT IF EXISTS fk_drug_atccode;
ALTER TABLE drug DROP COLUMN IF EXISTS atccode_id;
ALTER TABLE drug ADD COLUMN IF NOT EXISTS atccode_id character varying(30);
ALTER TABLE drug ADD COLUMN IF NOT EXISTS active boolean default true;
ALTER TABLE drug ADD COLUMN IF NOT EXISTS tipoDoenca character  varying(30) default 'TARV';
ALTER TABLE regimendrugs DROP CONSTRAINT IF EXISTS regimen_fkey;
-- ALTER TABLE regimendrugs DROP CONSTRAINT IF EXISTS fk281dee122dfb779f;
ALTER TABLE regimendrugs ADD CONSTRAINT regimen_fkey FOREIGN KEY (regimen) REFERENCES regimeterapeutico (regimeid) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;
ALTER TABLE regimeTerapeutico DROP COLUMN IF EXISTS pediatrico;
ALTER TABLE regimeTerapeutico DROP COLUMN IF EXISTS linhaid;
ALTER TABLE regimeTerapeutico DROP COLUMN IF EXISTS adult;
ALTER TABLE regimeTerapeutico ALTER COLUMN codigoregime TYPE character varying(255);
ALTER TABLE regimeTerapeutico ADD COLUMN  IF NOT EXISTS codigoregime character varying(255);
ALTER TABLE regimeTerapeutico ADD COLUMN IF NOT EXISTS regimeesquemaidart character varying(255);
ALTER TABLE regimeTerapeutico ADD COLUMN IF NOT EXISTS regimenomeespecificado character varying(255);
ALTER TABLE regimeTerapeutico ALTER COLUMN regimeesquemaidart TYPE character varying(255);
ALTER TABLE regimeTerapeutico ALTER COLUMN regimenomeespecificado TYPE character varying(255);
ALTER TABLE prescription DROP CONSTRAINT IF EXISTS prescription_regime;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS durationsentence character varying(255) default null;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS dispensatrimestral integer NOT NULL DEFAULT 0;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS tipodt character varying(255);
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS dispensasemestral integer NOT NULL DEFAULT 0;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS tipods character varying(255);
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS dc character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS prep character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS ce character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS cpn character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS af character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS ca character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS ccr character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS fr character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS gaac character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS saaj character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS linhaid integer;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS prescricaoespecial character(1) DEFAULT 'F'::bpchar;
ALTER TABLE prescription ADD COLUMN IF NOT EXISTS motivocriacaoespecial character varying(255) DEFAULT '';
ALTER TABLE patient ADD COLUMN IF NOT EXISTS uuidopenmrs character varying(255) default null;
INSERT INTO identifiertype (id, name, index, voided) VALUES (NEXTVAL('hibernate_sequence')::integer, 'CRAM'::character varying(255), '1'::integer, false::boolean);	
INSERT INTO identifiertype (id, name, index, voided) VALUES (NEXTVAL('hibernate_sequence')::integer, 'PREP'::character varying(255), '2'::integer, false::boolean);
INSERT INTO identifiertype (id, name, index, voided) VALUES (NEXTVAL('hibernate_sequence')::integer, 'PPE'::character varying(255), '3'::integer, false::boolean);
INSERT INTO identifiertype (id, name, index, voided) VALUES (NEXTVAL('hibernate_sequence')::integer, 'Outro'::character varying(255), '4'::integer, false::boolean);
DELETE FROM simpledomain WHERE description  = 'prescription_reason';
DELETE FROM simpledomain WHERE description  = 'Disease';
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'prescription_reason','prescription_reason','Perda de Medicamento');
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'prescription_reason','prescription_reason','Ausencia do Clinico');
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'prescription_reason','prescription_reason','Laboratorio');
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'prescription_reason','prescription_reason','Rotura de Stock');
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'prescription_reason','prescription_reason','Outro');
INSERT INTO simpledomain VALUES (NEXTVAL('hibernate_sequence')::integer,'Disease','TARV','TARV');
UPDATE drug SET atccode_id = '' WHERE atccode_id IS NULL;
UPDATE prescription set cpn='T' where ptv = 'T';
UPDATE prescription set dispensatrimestral = 0 where dispensatrimestral is null;
UPDATE regimeTerapeutico SET codigoregime = '' WHERE codigoregime IS NULL;
UPDATE regimeTerapeutico SET regimenomeespecificado= '' WHERE regimenomeespecificado IS NULL;
DROP TABLE IF EXISTS public.sync_temp_dispense CASCADE;
CREATE TABLE public.sync_temp_dispense
(
    id integer NOT NULL,
    clinicalstage integer,
    current character(1) COLLATE pg_catalog."default",
    date timestamp with time zone,
    doctor integer,
    duration integer,
    modified character(1) COLLATE pg_catalog."default",
    patient integer NOT NULL,
    sync_temp_dispenseid character varying(255) COLLATE pg_catalog."default",
    weight double precision,
    reasonforupdate character varying(255) COLLATE pg_catalog."default",
    notes character varying(255) COLLATE pg_catalog."default",
    enddate timestamp with time zone,
    drugtypes character varying(20) COLLATE pg_catalog."default",
    regimeid character varying(255) COLLATE pg_catalog."default",
    datainicionoutroservico timestamp(6) with time zone,
    motivomudanca character varying(32) COLLATE pg_catalog."default",
    linhaid character varying(255) COLLATE pg_catalog."default",
    ppe character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    ptv character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    tb character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    tpi character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    tpc character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    dispensatrimestral integer NOT NULL DEFAULT 0,
    tipodt character varying(255) COLLATE pg_catalog."default",
    gaac character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    af character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    ca character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    ccr character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    saaj character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    fr character(1) COLLATE pg_catalog."default" DEFAULT 'F'::bpchar,
    amountpertime character varying(255) COLLATE pg_catalog."default",
    dispensedate timestamp with time zone,
    drugname character varying(255) COLLATE pg_catalog."default",
    expirydate timestamp with time zone,
    patientid character varying(255) COLLATE pg_catalog."default",
    patientfirstname character varying(255) COLLATE pg_catalog."default",
    patientlastname character varying(255) COLLATE pg_catalog."default",
    dateexpectedstring character varying(255) COLLATE pg_catalog."default",
    pickupdate timestamp with time zone,
    timesperday integer,
    weekssupply integer,
    qtyinhand character varying(255) COLLATE pg_catalog."default",
    summaryqtyinhand character varying(255) COLLATE pg_catalog."default",
    qtyinlastbatch character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT sync_temp_dispense_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
ALTER TABLE public.sync_temp_dispense OWNER to postgres;
DROP TABLE IF EXISTS public.sync_temp_patients CASCADE;
CREATE TABLE public.sync_temp_patients
(
    id integer NOT NULL,
    accountstatus boolean,
    cellphone character varying(255) COLLATE pg_catalog."default",
    dateofbirth timestamp with time zone,
    clinic integer NOT NULL,
    clinicname character varying(255) COLLATE pg_catalog."default",
    mainclinic integer NOT NULL,
    mainclinicname character varying(255) COLLATE pg_catalog."default" NOT NULL,
    firstnames character varying(255) COLLATE pg_catalog."default",
    homephone character varying(255) COLLATE pg_catalog."default",
    lastname character varying(255) COLLATE pg_catalog."default",
    modified character(1) COLLATE pg_catalog."default",
    patientid character varying(255) COLLATE pg_catalog."default" NOT NULL,
    province character varying(255) COLLATE pg_catalog."default",
    sex character(1) COLLATE pg_catalog."default",
    workphone character varying(255) COLLATE pg_catalog."default",
    address1 character varying(255) COLLATE pg_catalog."default",
    address2 character varying(255) COLLATE pg_catalog."default",
    address3 character varying(255) COLLATE pg_catalog."default",
    nextofkinname character varying(255) COLLATE pg_catalog."default",
    nextofkinphone character varying(255) COLLATE pg_catalog."default",
    race character varying(255) COLLATE pg_catalog."default",
    uuid character varying(255) COLLATE pg_catalog."default",
    datainiciotarv character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT chave_primaria PRIMARY KEY (id, mainclinicname)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.sync_temp_patients
    OWNER to postgres;

CREATE TABLE IF NOT EXISTS public.openmrserrorlog
(
    id integer NOT NULL,
    datecreated timestamp without time zone,
    errordescription character varying(255) COLLATE pg_catalog."default",
    patient integer,
    pickupdate timestamp without time zone,
    prescription integer,
    returnpickupdate timestamp without time zone,
    CONSTRAINT openmrserrorlog_pkey PRIMARY KEY (id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.openmrserrorlog
    OWNER to postgres;