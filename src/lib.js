import { EvidenceType } from "@evidence-dev/db-commons";

/**
 * TODO: duplicate with index.js
 */
export const databaseTypeToEvidenceType = {
  BOOL: EvidenceType.BOOLEAN,
  /* add BOOLEAN? => BOOLEAN*/
  INT64: EvidenceType.NUMBER,
  FLOAT32: EvidenceType.NUMBER,
  FLOAT64: EvidenceType.NUMBER,
  NUMERIC: EvidenceType.NUMBER,
  /* add INT, SMALLINT, INTEGER, BIGINT, TINYINT, BYTEINT, DECIMAL, BIGDECIMAL, BIGNUMERIC, FLOAT? => NUMBER*/
  STRING: EvidenceType.STRING,
  BYTES: EvidenceType.STRING,
  /* add TIME, GEOGRAPHY, INTERVAL, VARCHAR? => STRING */
  TIMESTAMP: EvidenceType.DATE,
  DATE: EvidenceType.DATE,
  /* add DATETIME? => DATE */
  STRUCT: undefined,
  ARRAY: undefined,
  JSON: undefined,
};
