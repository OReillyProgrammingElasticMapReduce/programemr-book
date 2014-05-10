--
-- setup piggyback functions
--
register file:/home/hadoop/lib/pig/piggybank.jar
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT;

--
-- Load input file for processing
--
RAW_LOGS = LOAD '$INPUT' USING TextLoader as (line:chararray);

--
-- Parse and convert log records into individual column values
--
LOGS_BASE = FOREACH RAW_LOGS GENERATE
    FLATTEN(
      EXTRACT(line, '^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\]
        "(.+?)" (\\d{3}) (\\S+)')
    )
    as (
      clientAddr:    chararray,
      remoteLogname: chararray,
      user:          chararray,
      time:          chararray,
      request:       chararray,
      status:        chararray,
      bytes_string:  chararray
  );

CONV_LOG = FOREACH LOGS_BASE generate clientAddr, remoteLogname, user, time,
    request, (int)status, (int)bytes_string;

--
-- Remove log lines that do not contain errors and group data based on HTTP
--   request lines
--
FILTERED = FILTER CONV_LOG BY status >= 300;
GROUP_REQUEST = GROUP FILTERED BY request;

--
-- Count the log lines that are for the same HTTP request and output the
--   results to S3
--
final_data = FOREACH GROUP_REQUEST GENERATE FLATTEN(group) AS request, COUNT($1);
STORE final_data into '$OUTPUT';
