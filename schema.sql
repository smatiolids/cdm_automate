CREATE TABLE cdm_run_parameters (
    parameter_id TEXT PRIMARY KEY,
    parameter_value TEXT
);

INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('start_token', '-9223372036854775808');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('end_token', '9223372036854775807');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('token_increment', '1844674407370955');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('last_end_token', '-9223372036854775808');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0000', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0100', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0200', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0300', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0400', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0500', '100000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0600', '80000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0700', '60000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0800', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_0900', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1000', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1100', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1200', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1300', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1400', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1500', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1600', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1700', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1800', '40000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_1900', '80000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_2000', '80000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_2100', '80000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_2200', '80000');
INSERT INTO cdm_run_parameters (parameter_id, parameter_value) VALUES ('read_rate_limit_2300', '80000');

CREATE TABLE cdm_run_interval (
    run_id TEXT,
    start_time timestamp,
    end_time timestamp,
    start_token bigint,
    end_token bigint,
    token_increment bigint,
    last_end_token bigint,
    read_rate_limit bigint,
    write_rate_limit bigint,
    status TEXT,
    elapsed_time bigint,
    PRIMARY KEY (run_id, start_time)
) with clustering order by (start_time DESC);