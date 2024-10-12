import gcp_spanner from "@google-cloud/spanner";
const { Spanner, SpannerOptions } = gcp_spanner;
import { OAuth2Client } from "google-auth-library";
import { EvidenceType, TypeFidelity, asyncIterableToBatchedAsyncGenerator } from "@evidence-dev/db-commons";


/**
 * @see https://docs.evidence.dev/plugins/create-source-plugin/#options-specification
 */
export const options = {
  project_id: {
    title: 'Project ID',
    type: 'string',
    secret: true,
    required: true,
    references: '$.keyfile.project_id',
    forceReference: false
  },
  location: {
    title: 'Location (Region)',
    type: 'string',
    secret: false,
    required: false,
    default: 'US'
  },
  authenticator: { /* TODO: add emulator as option? */
    title: 'Authentication Method',
    type: 'select',
    secret: false,
    nest: false,
    required: true,
    default: 'service-account',
    options: [
      {
        value: 'service-account',
        label: 'Service Account'
      },
      {
        value: 'gcloud-cli',
        label: 'GCloud CLI'
      },
      {
        value: 'oauth',
        label: 'OAuth Access Token'
      }
    ],
    children: {
      'service-account': {
        keyfile: {
          title: 'Credentials File',
          type: 'file',
          fileFormat: 'json',
          virtual: true
        },
        client_email: {
          title: 'Client Email',
          type: 'string',
          secret: true,
          required: true,
          references: '$.keyfile.client_email',
          forceReference: true
        },
        private_key: {
          title: 'Private Key',
          type: 'string',
          secret: true,
          required: true,
          references: '$.keyfile.private_key',
          forceReference: true
        }
      },
      'gcloud-cli': {
        /* no-op; only needs projectId */
      },
      oauth: {
        token: {
          type: 'string',
          title: 'Token',
          secret: true,
          required: true
        }
      }
    }
  }
};


/**
 * @see https://docs.evidence.dev/plugins/create-source-plugin/
 * @type {import("@evidence-dev/db-commons").GetRunner<SpannerOptions>}
 */
export const getRunner = (options) => {
  console.debug(`project_id = ${options.project_id}`);
  console.debug(`project_id = ${options.authenticator}`);
  return async (queryText, queryPath) => {
    if (!queryPath.endsWith('.sql')) return null;
    return runQuery(queryText, options);
  };
};


/** @type {import("@evidence-dev/db-commons").RunQuery<SpannerOptions>} */
export const runQuery = async (queryText, database, batchSize = 100000) => {
  try {
    const spanner = getConnection(database);
    const instance = spanner.instance(database.instanceId);
    const databaseConnection = instance.database(database.databaseId);
    const [rows] = await databaseConnection.run({ sql: queryText });

    const result = await asyncIterableToBatchedAsyncGenerator(rows, batchSize, {
      standardizeRow
    });

    result.columnTypes = mapResultsToEvidenceColumnTypes(rows);
    result.expectedRowCount = rows.length;

    return result;
  } catch (err) {
    throw err.message;
  }
};


/**
 * @param {SpannerOptions} db
 * @returns {Spanner}
 */
const getConnection = (credentials) => new Spanner({ ...getCredentials(credentials), maxRetries: 10 });


/**
 * @param {Partial<SpannerOptions>} database
 * @returns {SpannerOptions}
 */
const getCredentials = (database = {}) => {
  const authentication_method = database.authenticator ?? 'service-account';

  if (authentication_method === 'gcloud-cli') {
    return {
      projectId: database.project_id,
      location: database.location
    };
  } else if (authentication_method === 'oauth') {
    const access_token = database.token;
    const oauth = new OAuth2Client();
    oauth.setCredentials({ access_token });

    return {
      authClient: oauth,
      projectId: database.project_id,
      location: database.location
    };
  } else { /* service-account */
    return {
      projectId: database.project_id,
      location: database.location,
      credentials: {
        client_email: database.client_email,
        private_key: database.private_key?.replace(/\\n/g, '\n').trim()
      }
    };
  }
};


/**
 * Standardizes a row from a Spanner query
 * @param {Record<string, unknown>} row
 * @returns {Record<string, unknown>}
 */
const standardizeRow = (row) => {
  const standardized = {};
  for (const [key, value] of Object.entries(row)) {
    if (typeof value === 'string' || typeof value === 'boolean' || typeof value === 'number') {
      standardized[key] = value;
    } else if (value instanceof Date) { /* SpannerDate extends Date */
      standardized[key] = value.toISOString();
    } else if (value instanceof Buffer) {
      standardized[key] = value.toString('base64');
    } else if (typeof value?.toFloat64 === 'function') {
      standardized[key] = value.toFloat64();
    }
  }
  return standardized;
};


/**
 * @param {SpannerRow[]} results TODO: check if type is correct
 * @returns {import('@evidence-dev/db-commons').ColumnDefinition[] | undefined}
 */
const mapResultsToEvidenceColumnTypes = (results) => {
  return results?.map((field) => { /* TODO: check if results?.map is correct of if something like results?.schema?.fields?.map is needed */
    /** @type {TypeFidelity} */
    let typeFidelity = TypeFidelity.PRECISE;
    let evidenceType = nativeTypeToEvidenceType(/** @type {string} */(field.type));
    if (!evidenceType) {
      typeFidelity = TypeFidelity.INFERRED;
      evidenceType = EvidenceType.STRING;
    }
    return {
      name: /** @type {string} */ (field.name),
      evidenceType: evidenceType,
      typeFidelity: typeFidelity
    };
  });
};


/**
 * TODO: check if it's compatible with Spanner in PostgreSQL mode: https://cloud.google.com/spanner/docs/reference/postgresql/data-types
 * TODO: currently duplicate with lib.js
 * @see https://cloud.google.com/spanner/docs/reference/standard-sql/data-types
 * @param {string} nativeFieldType
 * @param {undefined} defaultType
 * @returns {EvidenceType | undefined}
 */
const nativeTypeToEvidenceType = (nativeFieldType, defaultType = undefined) => {
  switch (nativeFieldType) {
    case 'BOOL':
      return EvidenceType.BOOLEAN; /* add BOOLEAN? */
    case 'INT64':
    case 'FLOAT32':
    case 'FLOAT64':
    case 'NUMERIC':
      return EvidenceType.NUMBER; /* add INT, SMALLINT, INTEGER, BIGINT, TINYINT, BYTEINT, DECIMAL, BIGDECIMAL, BIGNUMERIC, FLOAT? */
    case 'STRING':
    case 'BYTES':
      return EvidenceType.STRING; /* add TIME, GEOGRAPHY, INTERVAL? */
    case 'TIMESTAMP':
    case 'DATE':
      return EvidenceType.DATE; /* add DATETIME? */
    case 'STRUCT':
    case 'ARRAY':
    case 'JSON':
    default:
      return defaultType;
  }
};


/** @type {import("@evidence-dev/db-commons").ConnectionTester<SpannerOptions>} */
export const testConnection = async (opts) => {
  const spanner = getConnection(opts);
  const instance = spanner.instance(opts.instanceId);
  const database = instance.database(opts.databaseId);

  return await database
    .run({ sql: 'SELECT 1' })
    .then(() => true)
    .catch((e) => {
      if (e instanceof Error) return { reason: e.message };
			try {
				return { reason: JSON.stringify(e) };
			} catch {
				return { reason: 'Unknown Connection Error' };
			}
    })
};
