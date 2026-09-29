// scripts/update_historical_labels.js
const { BigQuery } = require('@google-cloud/bigquery');
const path = require('path');

// Tenta carregar as variáveis do arquivo .env se ele existir localmente
try {
  require('dotenv').config({ path: path.join(__dirname, '../.env') });
} catch (e) {
  // Ignora se não houver pacote dotenv ou .env
}

// Configurações do BigQuery
const bigquery = new BigQuery();
const datasetId = 'field_control_data';
const tableId = 'orders';

// Importa a API do Field Control
const fieldApi = require('../services/fieldControlApi');

// Utilitário para delay (respeitar rate limit de 1 req/seg)
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

async function runJsonMigration() {
  console.log('--- FASE 1: Migrando etiquetas em formato JSON já existentes no BigQuery ---');
  const sqlQuery = `
    UPDATE \`${datasetId}.${tableId}\`
    SET 
      labels = (
        SELECT STRING_AGG(JSON_VALUE(label, '$.name'), ', ')
        FROM UNNEST(JSON_QUERY_ARRAY(labels)) AS label
      )
    WHERE labels LIKE '[%';
  `;
  try {
    console.log('Executando query de atualização no BigQuery...');
    const [job] = await bigquery.createQueryJob({
      query: sqlQuery,
      location: 'southamerica-east1'
    });
    console.log(`Job do BigQuery iniciado: ${job.id}. Aguardando conclusão...`);
    const [rows] = await job.getQueryResults();
    console.log('✅ Fase 1 concluída! Todas as etiquetas em formato JSON foram convertidas.');
  } catch (err) {
    console.error('❌ Erro na Fase 1:', err.message);
  }
}

async function recoverNullLabels(days = 30) {
  console.log(`\n--- FASE 2: Recuperando etiquetas nulas via API do Field Control (Últimos ${days} dias) ---`);
  
  if (!process.env.FIELD_CONTROL_API_KEY) {
    console.warn('⚠️  Aviso: FIELD_CONTROL_API_KEY não foi fornecida no ambiente.');
    console.warn('A Fase 2 (recuperação via API) foi pulada. Para executá-la, rode o script passando a chave.');
    console.warn('Exemplo: $env:FIELD_CONTROL_API_KEY="sua_chave" ; node scripts/update_historical_labels.js\n');
    return;
  }

  try {
    // Busca orders com labels nulas criadas nos últimos X dias
    const sqlQuery = `
      SELECT id, identifier, createdAt
      FROM \`${datasetId}.${tableId}\`
      WHERE labels IS NULL
        AND createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
      ORDER BY createdAt DESC
    `;
    
    console.log('Buscando ordens afetadas no BigQuery...');
    const [ordersToUpdate] = await bigquery.query({
      query: sqlQuery,
      params: { days: parseInt(days, 10) },
      location: 'southamerica-east1'
    });

    console.log(`Encontradas ${ordersToUpdate.length} ordens com etiquetas nulas criadas nos últimos ${days} dias.`);

    if (ordersToUpdate.length === 0) {
      console.log('Nenhuma ordem para atualizar.');
      return;
    }

    let updatedCount = 0;
    let noLabelsCount = 0;
    let errorCount = 0;

    for (let i = 0; i < ordersToUpdate.length; i++) {
      const order = ordersToUpdate[i];
      const percent = (((i + 1) / ordersToUpdate.length) * 100).toFixed(1);
      
      console.log(`[${i + 1}/${ordersToUpdate.length} - ${percent}%] Consultando OS ${order.identifier} (ID: ${order.id})...`);

      // Respeita o rate limit
      await sleep(1000);

      try {
        const labels = await fieldApi.getOrderLabels(order.id);
        
        if (labels && labels.length > 0) {
          const labelsStr = labels.map(l => l.name).join(', ');
          const labelsArray = labels.map(l => l.name);
          const labelsCount = labels.length;

          console.log(`   -> OS ${order.identifier} possui as etiquetas: [${labelsStr}]. Atualizando no BigQuery...`);
          
          const updateQuery = `
            UPDATE \`${datasetId}.${tableId}\`
            SET 
              labels = @labels,
              labels_array = @labels_array,
              labels_count = @labels_count
            WHERE id = @id
          `;

          await bigquery.query({
            query: updateQuery,
            params: {
              labels: labelsStr,
              labels_array: labelsArray,
              labels_count: labelsCount,
              id: order.id
            },
            location: 'southamerica-east1'
          });
          
          updatedCount++;
        } else {
          // A ordem realmente não tinha etiquetas na API, então atualizamos para String vazia ou apenas registramos
          noLabelsCount++;
        }
      } catch (apiErr) {
        console.error(`   ❌ Erro ao processar OS ${order.identifier}:`, apiErr.message);
        errorCount++;
      }
    }

    console.log('\n--- Resumo da Fase 2 ---');
    console.log(`- Total de ordens verificadas: ${ordersToUpdate.length}`);
    console.log(`- Etiquetas recuperadas e gravadas: ${updatedCount}`);
    console.log(`- Ordens que não possuíam etiquetas: ${noLabelsCount}`);
    console.log(`- Erros encontrados: ${errorCount}`);

  } catch (err) {
    console.error('❌ Erro crítico na Fase 2:', err.message);
  }
}

async function main() {
  const args = process.argv.slice(2);
  const daysArg = args.find(arg => arg.startsWith('--days='));
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 30;

  console.log('=== Iniciando Script de Atualização Histórica de Etiquetas ===');
  
  // Fase 1: Atualização dos JSONs antigos
  await runJsonMigration();

  // Fase 2: Recuperação de nulos via API
  await recoverNullLabels(days);

  console.log('=== Script Concluído! ===');
}

main();
