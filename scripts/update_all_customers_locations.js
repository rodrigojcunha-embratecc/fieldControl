// scripts/update_all_customers_locations.js
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

// Importações dos serviços e mappers locais
const fieldApi = require('../services/fieldControlApi');
const bqService = require('../services/bigQueryService');
const mappers = require('../functions/mappers');

// Utilitário para delay
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Helper de retry com backoff exponencial para lidar com limites de cota da API (429)
async function withRetry(fn, label = 'API request') {
  let attempt = 1;
  const maxAttempts = 5;
  while (true) {
    try {
      return await fn();
    } catch (err) {
      if (attempt >= maxAttempts) {
        throw err;
      }
      const isRateLimit = err.response?.status === 429 || err.message.includes('429');
      const delay = (isRateLimit ? 5000 : 2000) * Math.pow(2, attempt - 1) + Math.random() * 500;
      console.warn(`   ⚠️  [RETRY ${attempt}/${maxAttempts}] ${label} falhou (${err.message}). Esperando ${Math.round(delay)}ms...`);
      await sleep(delay);
      attempt++;
    }
  }
}

async function main() {
  console.log('=== Iniciando Script de Atualização Total de Clientes e Localidades ===');
  
  if (!process.env.FIELD_CONTROL_API_KEY) {
    console.error('❌ Erro: FIELD_CONTROL_API_KEY não foi fornecida no ambiente.');
    console.error('Para executar este script, defina a chave.');
    console.error('Exemplo: $env:FIELD_CONTROL_API_KEY="sua_chave" ; node scripts/update_all_customers_locations.js\n');
    process.exit(1);
  }

  try {
    // 1. Busca todos os clientes da API do Field Control com retry
    console.log('Buscando todos os clientes da API do Field Control...');
    const customers = await withRetry(
      () => fieldApi.listAll('/customers'),
      'listAll /customers'
    );
    console.log(`Sucesso: ${customers.length} clientes encontrados no Field Control.`);

    let customerUpdateCount = 0;
    let locationsUpdateCount = 0;
    let errorCount = 0;

    // 2. Itera sobre cada cliente para buscar suas localidades e atualizar no BigQuery
    for (let i = 0; i < customers.length; i++) {
      const cust = customers[i];
      const percent = (((i + 1) / customers.length) * 100).toFixed(1);
      
      console.log(`[${i + 1}/${customers.length} - ${percent}%] Processando cliente: ${cust.name} (ID: ${cust.id})...`);

      // Respeita o rate limit
      await sleep(1200);

      try {
        // Upsert do cliente
        const mappedCust = mappers.mapCustomer(cust);
        await bqService.upsertRows('customers', [mappedCust]);
        customerUpdateCount++;

        // Busca e upsert das localizações do cliente com retry
        const locations = await withRetry(
          () => fieldApi.listAll(`/customers/${cust.id}/locations`),
          `locations for ${cust.name}`
        );
        
        if (locations && locations.length > 0) {
          const mappedLocations = locations.map(loc => {
            const mappedLoc = mappers.mapLocation(loc);
            mappedLoc.customer_id = cust.id; // Garante o vínculo
            return mappedLoc;
          });

          await bqService.upsertRows('locations', mappedLocations);
          locationsUpdateCount += mappedLocations.length;
          console.log(`   -> Sincronizadas ${mappedLocations.length} localizações.`);
        }
      } catch (err) {
        console.error(`   ❌ Erro ao processar cliente ${cust.name} (ID: ${cust.id}):`, err.message);
        errorCount++;
      }
    }

    console.log('\n=== Resumo da Sincronização de Clientes/Localidades ===');
    console.log(`- Clientes processados e salvos: ${customerUpdateCount}`);
    console.log(`- Localidades processadas e salvas: ${locationsUpdateCount}`);
    console.log(`- Falhas/Erros encontrados: ${errorCount}`);
    console.log('=== Sincronização Concluída! ===');

  } catch (err) {
    console.error('❌ Erro crítico no script:', err.message);
    process.exit(1);
  }
}

main();
