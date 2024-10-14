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
  instance_id: {
    title: 'Spanner Instance Id',
    type: 'string',
    secret: false,
    required: true
  },
  database_id: {
    title: 'Spanner Database Id',
    type: 'string',
    secret: false,
    required: true
  },
  location: {
    title: 'Location (Region)',
    type: 'string',
    secret: false,
    required: false,
    default: 'US'
  },
  authenticator: {
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
      },
      {
        value: 'emulator',
        label: 'Emulator'
      }
    ],
    children: { /* TODO: Add description for parameters */
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
      },
      emulator: {
        emulator_host: {
          type: 'string',
          title: 'Host',
          description: 'Hostname of running Cloud Spanner Emulator, without the port. (e.g. localhost)',
          default: 'localhost',
          required: true
        },
        emulator_port: {
          type: 'string',
          title: 'Port',
          description: 'HTTP Port of running Cloud Spanner Emulator. (e.g. 9020)',
          default: '9020',
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
  return async (queryText, queryPath) => {
    if (!queryPath.endsWith('.sql')) return null;
    return runQuery(queryText, options);
  };
};


/** @type {import("@evidence-dev/db-commons").RunQuery<SpannerOptions>} */
export const runQuery = async (queryText, database, batchSize = 100000) => {
  try {
    const spanner = getConnection(database);
    const instance = spanner.instance(database.instance_id);
    const databaseConnection = instance.database(database.database_id);
    const [rows] = await databaseConnection.run({ sql: queryText });
    console.log(rows)
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
  } else if (authentication_method === 'emulator') {
      return {
        projectId: database.project_id,
        apiEndpoint: database.emulator_host,
        port: database.emulator_port
      }
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
  console.log(Object.entries(row))
  for (const [key, value] of Object.entries(row)) {
    if (typeof value === 'string' || typeof value === 'boolean' || typeof value === 'number') {
      standardized[key] = value;
    } else if (value instanceof Date) {
      standardized[key] = value.toISOString();
    } else if (value instanceof Buffer) {
      standardized[key] = value.toString('base64');
    } else if (typeof value?.toFloat64 === 'function') {
      standardized[key] = value.toFloat64();
    }
  }
  console.log(standardized)
  return standardized;
};


/**
 * @param {Record<string, unknown>[]} results
 * @returns {import('@evidence-dev/db-commons').ColumnDefinition[] | undefined}
 */
const mapResultsToEvidenceColumnTypes = (results) => {
  if (!results || results.length === 0) return [];

  // Use the first row to extract column names and infer types
  const firstRow = results[0];
  return firstRow.map((column) => {
    const { name, value } = column;
    /** @type {TypeFidelity} */
    let typeFidelity = TypeFidelity.INFERRED;
    let evidenceType;

    // Try to infer type from the value
    if (typeof value === 'string') {
      evidenceType = EvidenceType.STRING;
      typeFidelity = TypeFidelity.PRECISE;
    } else if (typeof value === 'number') {
      evidenceType = EvidenceType.NUMBER;
      typeFidelity = TypeFidelity.PRECISE;
    } else if (typeof value === 'boolean') {
      evidenceType = EvidenceType.BOOLEAN;
      typeFidelity = TypeFidelity.PRECISE;
    } else if (value instanceof Date) {
      evidenceType = EvidenceType.DATE;
      typeFidelity = TypeFidelity.PRECISE;
    } else if (value instanceof Object) {
      typeFidelity = TypeFidelity.PRECISE;
      evidenceType = nativeTypeToEvidenceType(/** @type {string} */(value.constructor.name));
    }
    
    if (!evidenceType) {
      typeFidelity = TypeFidelity.INFERRED;
      evidenceType = EvidenceType.STRING;
    }

    return {
      name: /** @type {string} */ (name),
      evidenceType: evidenceType,
      typeFidelity: typeFidelity
    };
  });
};


/**
 * TODO: check if it's compatible with Spanner in PostgreSQL mode: https://cloud.google.com/spanner/docs/reference/postgresql/data-types
 * 
 * These types returned by spanner do not exactly match Spanner Data Types
 * @see https://cloud.google.com/spanner/docs/reference/standard-sql/data-types
 * 
 * e.g. INT64 database column returns 'Int { value: '42' }' as object.
 * 
 * @param {string} nativeFieldType
 * @param {undefined} defaultType
 * @returns {EvidenceType | undefined}
 */
const nativeTypeToEvidenceType = (nativeFieldType, defaultType = undefined) => {
  switch (nativeFieldType) {
    case 'Int':
    case 'Float32':
    case 'Float':
    case 'Numeric':
      return EvidenceType.NUMBER;
    case 'Buffer':
      return EvidenceType.STRING;
    default:
      return defaultType;
  }
};


/** @type {import("@evidence-dev/db-commons").ConnectionTester<SpannerOptions>} */
export const testConnection = async (opts) => {
  const spanner = getConnection(opts);
  const instance = spanner.instance(opts.instance_id);
  const database = instance.database(opts.database_id);
  
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
