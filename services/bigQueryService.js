// bigQueryService.js
require('dotenv').config();
const { BigQuery } = require('@google-cloud/bigquery');

const projectId = process.env.GOOGLE_CLOUD_PROJECT || process.env.PROJECT_ID || 'bi-embratecc';
const datasetId = process.env.DATASET_ID || process.env.BIGQUERY_DATASET || 'field_control_data';
const location = process.env.BIGQUERY_LOCATION || process.env.LOCATION || 'southamerica-east1';

const bigquery = new BigQuery({ projectId });

// Allowlist de tabelas permitidas para prevenir injeção ou nomes inválidos
const ALLOWED_TABLES = new Set([
  'services',
  'employees',
  'customers',
  'locations',
  'orders',
  'order_comments',
  'attachments',
  'tasks',
  'forms_answers',
  'forms_answers_questions',
  'tickets',
  'ratings'
]);

function validateTable(tableId) {
  if (!ALLOWED_TABLES.has(tableId)) {
    throw new Error(`[BigQuery Security] Tabela não autorizada: '${tableId}'. Tabelas permitidas: ${Array.from(ALLOWED_TABLES).join(', ')}`);
  }
}

// Retorna o tipo de CAST adequado para o BigQuery com base no tipo do schema da tabela
function getCastType(bqType) {
  switch (bqType.toUpperCase()) {
    case 'INTEGER':
    case 'INT64':
      return 'INT64';
    case 'FLOAT':
    case 'FLOAT64':
      return 'FLOAT64';
    case 'BOOLEAN':
    case 'BOOL':
      return 'BOOL';
    case 'TIMESTAMP':
      return 'TIMESTAMP';
    case 'DATE':
      return 'DATE';
    case 'DATETIME':
      return 'DATETIME';
    case 'NUMERIC':
      return 'NUMERIC';
    case 'TIME':
      return 'TIME';
    default:
      return 'STRING';
  }
}

/**
 * Função simplificada para UPSERT usando MERGE direto via UNNEST(@rows).
 * Evita qualquer tabela de staging (temporária ou persistente), eliminando custos de DDL,
 * latência de escrita em disco e erros com DML em buffers de streaming.
 */
async function upsertRows(tableId, rows) {
  if (!rows || rows.length === 0) return;
  validateTable(tableId);

  const destinationTable = bigquery.dataset(datasetId).table(tableId);

  try {
    console.log(`🚀 [UPSERT ${tableId}] Iniciando processo para ${rows.length} linhas via MERGE + UNNEST.`);

    const pk = (tableId.toLowerCase() === 'ratings') ? null : 'id';
    
    if (!pk) {
      // Se for ratings (não tem chave primária para merge), apenas inserimos direto
      await destinationTable.insert(rows);
      console.log(`✅ [INSERT ${tableId}] ${rows.length} linhas inseridas.`);
      return;
    }

    // 1. Obter os metadados (schema) da tabela de destino
    const [metadata] = await destinationTable.getMetadata();
    const columns = metadata.schema.fields.map(f => f.name);

    // 2. Sanitizar e tipar cada linha de acordo com o schema do BigQuery
    // Isso é essencial para que o array de STRUCTs seja inferido com os campos e tipos corretos
    const sanitizedRows = rows.map(row => {
      const sanitized = {};
      metadata.schema.fields.forEach(field => {
        const val = row[field.name];
        
        if (field.mode === 'REPEATED') {
          // Para campos REPEATED (Arrays), mantemos o array ou convertemos null/undefined para array vazio
          sanitized[field.name] = Array.isArray(val) ? val : [];
        } else if (val === undefined || val === null) {
          sanitized[field.name] = null;
        } else if (val instanceof Date) {
          sanitized[field.name] = val.toISOString();
        } else if (typeof val === 'object') {
          sanitized[field.name] = JSON.stringify(val);
        } else {
          // Garante casting de primitivos para evitar discrepâncias em JS
          if (field.type === 'INTEGER' || field.type === 'INT64') {
            const parsed = parseInt(val, 10);
            sanitized[field.name] = isNaN(parsed) ? null : parsed;
          } else if (field.type === 'FLOAT' || field.type === 'FLOAT64' || field.type === 'NUMERIC') {
            const parsed = parseFloat(val);
            sanitized[field.name] = isNaN(parsed) ? null : parsed;
          } else if (field.type === 'BOOLEAN' || field.type === 'BOOL') {
            sanitized[field.name] = (val === 'true' || val === true);
          } else {
            sanitized[field.name] = String(val);
          }
        }
      });
      return sanitized;
    });

    // 3. Gerar os campos de atualização do UPDATE aplicando CAST explícito
    const temporalTypes = ['TIME', 'DATE', 'TIMESTAMP', 'DATETIME'];
    const updateFields = metadata.schema.fields
      .filter(f => f.name.toLowerCase() !== pk)
      .map(f => {
        const isRepeated = f.mode === 'REPEATED';
        let valueExpr;
        if (isRepeated) {
          valueExpr = `source.${f.name}`;
        } else if (temporalTypes.includes(f.type.toUpperCase())) {
          // Para fields temporais, enviamos o valor como STRING no parâmetro
          // e usamos SAFE_CAST no SQL para evitar erros de atribuição
          valueExpr = `SAFE_CAST(source.${f.name} AS ${getCastType(f.type)})`;
        } else {
          valueExpr = `CAST(source.${f.name} AS ${getCastType(f.type)})`;
        }
        return `target.${f.name} = ${valueExpr}`;
      })
      .join(', ');

    // 4. Gerar a lista de valores do INSERT aplicando CAST explícito
    const insertValues = metadata.schema.fields
      .map(f => {
        const isRepeated = f.mode === 'REPEATED';
        if (isRepeated) return `source.${f.name}`;
        if (temporalTypes.includes(f.type.toUpperCase())) {
          return `SAFE_CAST(source.${f.name} AS ${getCastType(f.type)})`;
        }
        return `CAST(source.${f.name} AS ${getCastType(f.type)})`;
      })
      .join(', ');

    // 5. Executar o MERGE diretamente com o parâmetro @rows
    const mergeQuery = `
      MERGE \`${datasetId}.${tableId}\` AS target
      USING (
        SELECT * FROM UNNEST(@rows) AS source
      ) AS source
      ON target.${pk} = source.${pk}
      WHEN MATCHED THEN
        UPDATE SET ${updateFields}
      WHEN NOT MATCHED THEN
        INSERT (${columns.join(', ')})
        VALUES (${insertValues})
    `;

    // 5. Mapear os tipos exatos de cada campo do Struct para o BigQuery
    // Isso evita o erro: "Parameter types must be provided for null values via the 'types' field..."
    const rowTypes = {};
    metadata.schema.fields.forEach(field => {
      const isRepeated = field.mode === 'REPEATED';
      const castType = getCastType(field.type);
      // Para tipos temporais, enviaremos o parâmetro como STRING para que o
      // valor recebido seja compatível com o que o client envia (strings).
      const paramType = (temporalTypes.includes(field.type.toUpperCase())) ? 'STRING' : castType;
      rowTypes[field.name] = isRepeated ? [paramType] : paramType;
    });

    await bigquery.query({
      query: mergeQuery,
      params: { rows: sanitizedRows },
      types: { rows: [rowTypes] },
      location: 'southamerica-east1'
    });

    console.log(`✅ [UPSERT ${tableId}] Merge via UNNEST concluído com sucesso.`);

  } catch (error) {
    console.error(`❌ [UPSERT ${tableId}] Erro no processo:`, error.message);
    if (error.errors) console.error('Detalhes:', JSON.stringify(error.errors));
    throw error;
  }
}

/**
 * Insert simples para casos onde o merge não é necessário
 */
async function insertRows(tableId, rows) {
  if (!rows || rows.length === 0) return;
  validateTable(tableId);

  try {
    await bigquery.dataset(datasetId).table(tableId).insert(rows);
    console.log(`✅ [INSERT ${tableId}] ${rows.length} linhas inseridas.`);
  } catch (error) {
    console.error(`❌ [INSERT ${tableId}] Erro:`, error.message);
    throw error;
  }
}

/**
 * Busca todos os IDs de clientes já carregados no BigQuery
 */
async function getAllCustomerIds() {
  const query = `SELECT id FROM \`${datasetId}.customers\``;
  const [rows] = await bigquery.query({ query, location });
  return rows.map(row => row.id);
}

/**
 * Busca todos os IDs de localizações já carregados no BigQuery
 */
async function getAllLocationIds() {
  const query = `SELECT id FROM \`${datasetId}.locations\``;
  const [rows] = await bigquery.query({ query, location });
  return rows.map(row => row.id);
}

// Não esqueça de exportar no final do arquivo:


module.exports = {
  insertRows,
  upsertRows,
  getAllCustomerIds,
  getAllLocationIds
};