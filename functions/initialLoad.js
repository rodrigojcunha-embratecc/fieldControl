require('dotenv').config();
const fieldApi = require('../services/fieldControlApi');
const bqService = require('../services/bigQueryService');
const mappers = require('./mappers');

// ==================== CONFIGURAÇÕES ====================
const CONFIG = {
  MAX_RETRIES: 5,
  BASE_DELAY_MS: 2000,
  REQUESTS_PER_SECOND: 2,
  MAX_CONCURRENT_REQUESTS: 2,
  REQUEST_TIMEOUT_MS: 120000,
  LONG_TIMEOUT_MS: 300000,

  PAGE_SIZE: 50,
  BATCH_SIZE: 500,
  SPECIAL_BATCH_SIZE: 200,
  TEST_MODE: process.env.TEST_MODE === 'true',
  TEST_LIMIT: 10,
  DRY_RUN: process.env.DRY_RUN === 'true',

  DEFAULT_SEPTEMBER_START: '2026-09-01T00:00:00Z',
  DEFAULT_SEPTEMBER_END: '2026-09-30T23:59:59Z',
};

// Flags de controle de concorrência e parada
let isStopped = false;
let activeSync = null;

// ==================== UTILITÁRIOS ====================

/**
 * Normaliza parâmetros de data recebidos via HTTP ou CLI
 * Suporta formatos:
 * - month: "YYYY-MM" (ex: "2026-09")
 * - startDate / endDate: "YYYY-MM-DD" ou ISO string
 */
function normalizeDateRange(options = {}) {
  let { startDate, endDate, month } = options;

  if (month && typeof month === 'string') {
    const parts = month.split('-').map(Number);
    if (parts.length === 2 && !isNaN(parts[0]) && !isNaN(parts[1])) {
      const [year, m] = parts;
      if (m >= 1 && m <= 12) {
        const start = new Date(Date.UTC(year, m - 1, 1, 0, 0, 0));
        // O dia 0 do mês seguinte é o último dia do mês corrente
        const end = new Date(Date.UTC(year, m, 0, 23, 59, 59, 999));
        return {
          startDate: start.toISOString(),
          endDate: end.toISOString(),
          description: `Mês ${String(m).padStart(2, '0')}/${year}`
        };
      }
    }
  }

  // Normalização de startDate
  if (!startDate) {
    startDate = CONFIG.DEFAULT_SEPTEMBER_START;
  } else if (!startDate.includes('T')) {
    startDate = `${startDate}T00:00:00Z`;
  }

  // Normalização de endDate
  if (!endDate) {
    endDate = CONFIG.DEFAULT_SEPTEMBER_END;
  } else if (!endDate.includes('T')) {
    endDate = `${endDate}T23:59:59Z`;
  }

  return {
    startDate,
    endDate,
    description: `${startDate} até ${endDate}`
  };
}

class RateLimiter {
  constructor(requestsPerSecond = CONFIG.REQUESTS_PER_SECOND) {
    this.minInterval = 1000 / requestsPerSecond;
    this.lastRequestTime = 0;
  }

  async acquire() {
    const now = Date.now();
    const timeSinceLast = now - this.lastRequestTime;
    if (timeSinceLast < this.minInterval) {
      await new Promise(r => setTimeout(r, this.minInterval - timeSinceLast));
    }
    this.lastRequestTime = Date.now();
  }
}

class ConcurrencyLimiter {
  constructor(max) {
    this.max = max;
    this.active = 0;
    this.waiting = [];
  }

  acquire() {
    return new Promise((resolve) => {
      if (this.active < this.max) {
        this.active++;
        resolve();
      } else {
        this.waiting.push(resolve);
      }
    });
  }

  release() {
    this.active--;
    if (this.waiting.length > 0) {
      this.active++;
      this.waiting.shift()();
    }
  }
}

async function withRetry(operation, entityId = 'unknown', timeoutMs = CONFIG.REQUEST_TIMEOUT_MS) {
  let attempt = 1;
  while (true) {
    try {
      const timeoutPromise = new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`Timeout após ${timeoutMs}ms`)), timeoutMs)
      );
      return await Promise.race([operation(), timeoutPromise]);
    } catch (err) {
      if (attempt >= CONFIG.MAX_RETRIES) {
        console.error(`[RETRY FAILED] ${entityId} após ${attempt} tentativas: ${err.message}`);
        throw err;
      }
      const isRateLimit = err.response?.status === 429 || err.message?.includes('429');
      const baseDelay = isRateLimit ? 5000 : CONFIG.BASE_DELAY_MS;
      const wait = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 500;
      console.warn(`[RETRY ${attempt}/${CONFIG.MAX_RETRIES}] ${entityId} - aguardando ${Math.round(wait)}ms - ${err.message}`);
      await new Promise(r => setTimeout(r, wait));
      attempt++;
    }
  }
}

class ProgressTracker {
  constructor(entityName, totalItems = 0) {
    this.entityName = entityName;
    this.totalItems = totalItems;
    this.processed = 0;
    this.success = 0;
    this.errors = 0;
    this.startTime = Date.now();
    this.unknownTotal = totalItems <= 0;
  }

  logProgress(success = true, count = 1) {
    if (success) this.success += count;
    else this.errors += count;
    this.processed += count;

    if (this.processed % 50 === 0 || (!this.unknownTotal && this.processed === this.totalItems)) {
      const elapsedMin = ((Date.now() - this.startTime) / 60000).toFixed(1);
      const perMin = elapsedMin > 0 ? (this.success / elapsedMin).toFixed(1) : 'N/A';
      if (this.unknownTotal) {
        console.log(`[${this.entityName}] ${this.success} processados - ${perMin}/min - ${this.errors} erros`);
      } else {
        const pct = ((this.processed / this.totalItems) * 100).toFixed(1);
        console.log(`[${this.entityName}] ${this.success}/${this.totalItems} (${pct}%) - ${perMin}/min - ${this.errors} erros`);
      }
    }
  }

  logCompletion() {
    const elapsed = ((Date.now() - this.startTime) / 60000).toFixed(1);
    console.log(`[${this.entityName}] COMPLETO em ${elapsed}min: ${this.success} ok / ${this.errors} erros`);
    return { success: this.success, errors: this.errors, duration: elapsed };
  }
}

function addIngestionDate(row) {
  row.ingestion_date = new Date().toISOString().split('T')[0];
  row.extracted_at = new Date().toISOString();
  return row;
}

// ==================== RUNNER GENÉRICO DE ENTIDADE ====================

async function runEntityLoad(entityName, endpoint, mapper, options = {}) {
  const {
    batchSize = CONFIG.BATCH_SIZE,
    useDetailEndpoint = true,
    testLimit = CONFIG.TEST_MODE ? CONFIG.TEST_LIMIT : null,
    dateFields = ['createdAt', 'updatedAt', 'startedAt', 'completedAt', 'receivedAt', 'deadlineContract', 'stats_first_service', 'stats_last_service'],
    longTimeout = false,
    paginated = true,
    pageSize = CONFIG.PAGE_SIZE,
    resumeOffset = 0,
    extraQueryParams = {},
    enrichWithLabels = false,
    collectIds = false,
    endDate = null,
  } = options;

  const queryDesc = extraQueryParams.q ? ` (Filtro: ${extraQueryParams.q})` : '';
  console.log(`\n[Carga] Iniciando: ${entityName} (${endpoint})${queryDesc} - Paginated: ${paginated}, Offset: ${resumeOffset}`);

  const rateLimiter = new RateLimiter();
  const concurrencyLimiter = new ConcurrencyLimiter(CONFIG.MAX_CONCURRENT_REQUESTS);
  const tracker = new ProgressTracker(entityName, 0);
  tracker.unknownTotal = true;

  const collectedIds = [];
  const endTimestamp = endDate ? new Date(endDate).getTime() : null;

  try {
    const timeoutMs = longTimeout ? CONFIG.LONG_TIMEOUT_MS : CONFIG.REQUEST_TIMEOUT_MS;
    const buffer = [];
    let processedCount = 0;
    let currentOffset = resumeOffset;

    if (paginated) {
      while (true) {
        if (isStopped) {
          console.log(`[${entityName}] Interrompido pelo usuário no offset ${currentOffset}`);
          break;
        }

        const pageResponse = await withRetry(
          () => fieldApi.listPaginated(endpoint, currentOffset, pageSize, extraQueryParams),
          `${endpoint}_page_${currentOffset}`,
          timeoutMs
        );

        const pageItems = pageResponse.items || [];
        if (pageItems.length === 0) break;

        for (const item of pageItems) {
          if (isStopped) {
            console.log(`[${entityName}] Interrompido pelo usuário`);
            break;
          }

          // Validação de limite superior de data (client-side) para evitar falsos positivos
          if (endTimestamp) {
            const itemDate = item.updatedAt || item.createdAt || item.date || item.created_at;
            if (itemDate) {
              const itemTs = new Date(itemDate).getTime();
              if (!isNaN(itemTs) && itemTs > endTimestamp) {
                continue;
              }
            }
          }

          if (useDetailEndpoint || (enrichWithLabels && entityName === 'orders')) {
            await rateLimiter.acquire();
          }

          try {
            let data = item;
            if (useDetailEndpoint && item.id) {
              await concurrencyLimiter.acquire();
              try {
                data = await withRetry(
                  () => fieldApi.getDetail(endpoint, item.id),
                  `${entityName}/${item.id}`
                );
              } finally {
                concurrencyLimiter.release();
              }
            }

            let mapped;
            if (enrichWithLabels && entityName === 'orders') {
              let labels = [];
              try {
                await rateLimiter.acquire();
                labels = await withRetry(
                  () => fieldApi.getOrderLabels(data.id || item.id),
                  `labels/order/${data.id || item.id}`
                );
              } catch (labelErr) {
                console.warn(`[Labels] Falha ao buscar labels da OS ${data.id}: ${labelErr.message}`);
              }
              mapped = mapper(data, labels);
            } else {
              mapped = mapper(data);
            }

            dateFields.forEach(key => {
              if (mapped[key] && typeof mapped[key] === 'string' && !mapped[key].includes('Z')) {
                mapped[key] = new Date(mapped[key]).toISOString();
              }
            });

            mapped = addIngestionDate(mapped);
            buffer.push(mapped);

            if (collectIds && item.id) {
              collectedIds.push(item.id);
            }

            tracker.logProgress(true);

            if (buffer.length >= batchSize) {
              if (!CONFIG.DRY_RUN) {
                try {
                  await bqService.upsertRows(entityName, buffer.splice(0, batchSize));
                } catch (error) {
                  console.error(`[${entityName}] Erro no upsert:`, error.message);
                  try {
                    await bqService.insertRows(entityName, buffer.splice(0, batchSize));
                  } catch (insertError) {
                    console.error(`[${entityName}] Fallback insert falhou:`, insertError.message);
                    throw insertError;
                  }
                }
              } else {
                buffer.splice(0, batchSize);
              }
              console.log(`[RESUME] Lote salvo para ${entityName} ~ offset ${currentOffset + processedCount}`);
            }
          } catch (err) {
            tracker.logProgress(false);
            console.error(`[${entityName}] Erro ID ${item.id || 'n/a'}: ${err.message}`);
          }

          processedCount++;
          if (testLimit && processedCount >= testLimit) break;
        }

        if (testLimit && processedCount >= testLimit) break;
        currentOffset += pageItems.length;

        if (pageItems.length < pageSize) break;
      }
    } else {
      const basicItems = await withRetry(
        () => fieldApi.listAll(endpoint, extraQueryParams),
        endpoint,
        timeoutMs
      );
      const itemsToProcess = testLimit ? basicItems.slice(0, testLimit) : basicItems;
      tracker.totalItems = itemsToProcess.length;
      tracker.unknownTotal = false;

      for (const item of itemsToProcess) {
        if (isStopped) break;
        if (useDetailEndpoint) {
          await rateLimiter.acquire();
        }

        try {
          let data = item;
          if (useDetailEndpoint && item.id) {
            await concurrencyLimiter.acquire();
            try {
              data = await withRetry(
                () => fieldApi.getDetail(endpoint, item.id),
                `${entityName}/${item.id}`
              );
            } finally {
              concurrencyLimiter.release();
            }
          }

          let mapped = mapper(data);
          dateFields.forEach(key => {
            if (mapped[key] && typeof mapped[key] === 'string' && !mapped[key].includes('Z')) {
              mapped[key] = new Date(mapped[key]).toISOString();
            }
          });

          mapped = addIngestionDate(mapped);
          buffer.push(mapped);

          if (collectIds && item.id) {
            collectedIds.push(item.id);
          }

          tracker.logProgress(true);

          if (buffer.length >= batchSize) {
            if (!CONFIG.DRY_RUN) {
              await bqService.upsertRows(entityName, buffer.splice(0, batchSize));
            } else {
              buffer.splice(0, batchSize);
            }
          }
        } catch (err) {
          tracker.logProgress(false);
          console.error(`[${entityName}] Erro ID ${item.id || 'n/a'}: ${err.message}`);
        }
      }
    }

    if (buffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.upsertRows(entityName, buffer.splice(0, buffer.length));
    }

    const completion = tracker.logCompletion();
    if (collectIds) {
      completion.ids = collectedIds;
    }
    return completion;
  } catch (err) {
    console.error(`[${entityName}] Falha crítica: ${err.message}`);
    throw err;
  }
}

// ==================== CARREGADORES ESPECÍFICOS ====================

async function loadServices(options = {}) {
  return await runEntityLoad('services', '/services', mappers.mapService, {
    paginated: false,
    useDetailEndpoint: false,
    ...options
  });
}

async function loadEmployees(options = {}) {
  return await runEntityLoad('employees', '/employees', mappers.mapEmployee, {
    paginated: false,
    useDetailEndpoint: false,
    ...options
  });
}

async function loadCustomers(options = {}) {
  const { startDate = CONFIG.DEFAULT_SEPTEMBER_START, endDate = null, collectIds = true, ...rest } = options;
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `created_at>=:${startDate}` } : {});

  return await runEntityLoad('customers', '/customers', mappers.mapCustomer, {
    longTimeout: true,
    paginated: true,
    useDetailEndpoint: true,
    extraQueryParams,
    collectIds,
    endDate,
    ...rest
  });
}

async function loadLocations(options = {}) {
  const { customerIds = null } = options;
  const entityName = 'locations';
  console.log(`\n[Carga] Iniciando: ${entityName} (Sincronizando localidades de clientes)`);

  const rateLimiter = new RateLimiter();
  const tracker = new ProgressTracker(entityName, 0);

  try {
    let targetCustomerIds = customerIds;
    if (!targetCustomerIds || targetCustomerIds.length === 0) {
      console.log(`[${entityName}] Buscando IDs de clientes cadastrados no BigQuery...`);
      targetCustomerIds = await bqService.getAllCustomerIds();
    }

    tracker.totalItems = targetCustomerIds.length;
    tracker.unknownTotal = false;
    console.log(`[${entityName}] ${targetCustomerIds.length} clientes a processar para localidades.`);

    const buffer = [];

    for (const customerId of targetCustomerIds) {
      if (isStopped) break;
      await rateLimiter.acquire();

      try {
        const locations = await withRetry(
          () => fieldApi.listAll(`/customers/${customerId}/locations`),
          `locations/customer/${customerId}`
        );

        for (const loc of locations) {
          let mapped = mappers.mapLocation(loc);
          mapped.customer_id = customerId;
          mapped = addIngestionDate(mapped);

          ['createdAt', 'updatedAt'].forEach(key => {
            if (mapped[key] && !mapped[key].includes('Z')) {
              mapped[key] = new Date(mapped[key]).toISOString();
            }
          });

          buffer.push(mapped);

          if (buffer.length >= CONFIG.SPECIAL_BATCH_SIZE) {
            if (!CONFIG.DRY_RUN) {
              await bqService.upsertRows(entityName, buffer.splice(0, buffer.length));
            } else {
              buffer.splice(0, buffer.length);
            }
          }
        }

        tracker.logProgress(true);
      } catch (err) {
        tracker.logProgress(false);
        console.error(`[${entityName}] Erro ao buscar localidades do cliente ${customerId}: ${err.message}`);
      }
    }

    if (buffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.upsertRows(entityName, buffer.splice(0, buffer.length));
    }

    return tracker.logCompletion();
  } catch (err) {
    console.error(`[${entityName}] Falha crítica: ${err.message}`);
    throw err;
  }
}

async function loadOrders(options = {}) {
  const { startDate = CONFIG.DEFAULT_SEPTEMBER_START, endDate = null, collectIds = true, ...rest } = options;
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `updated_at>=:${startDate}` } : {});

  return await runEntityLoad('orders', '/orders', mappers.mapOrder, {
    longTimeout: true,
    paginated: true,
    useDetailEndpoint: true,
    enrichWithLabels: true,
    extraQueryParams,
    collectIds,
    endDate,
    ...rest
  });
}

async function loadOrderSubResources(options = {}) {
  const { orderIds = null, startDate = CONFIG.DEFAULT_SEPTEMBER_START } = options;
  const commentsEntity = 'order_comments';
  const attachmentsEntity = 'attachments';

  console.log(`\n[Carga] Iniciando sub-recursos de ordens (comentários e anexos)`);

  const rateLimiter = new RateLimiter();
  const commentsTracker = new ProgressTracker(commentsEntity, 0);
  const attachmentsTracker = new ProgressTracker(attachmentsEntity, 0);
  commentsTracker.unknownTotal = true;
  attachmentsTracker.unknownTotal = true;

  const commentsBuffer = [];
  const attachmentsBuffer = [];

  try {
    let ordersToProcess = [];

    if (orderIds && orderIds.length > 0) {
      console.log(`[Sub-recursos] Processando comentários/anexos para ${orderIds.length} ordens de serviço do período.`);
      ordersToProcess = orderIds.map(id => ({ id }));
    } else {
      console.log(`[Sub-recursos] Buscando ordens via API com filtro de data (q=updated_at>=:${startDate})...`);
      const extraQueryParams = startDate ? { q: `updated_at>=:${startDate}` } : {};
      ordersToProcess = await fieldApi.listAll('/orders', extraQueryParams);
    }

    for (const order of ordersToProcess) {
      if (isStopped) break;
      await rateLimiter.acquire();

      try {
        const comments = await withRetry(
          () => fieldApi.listAll(`/orders/${order.id}/comments`),
          `comments/order/${order.id}`
        );

        comments.forEach(c => {
          let mapped = mappers.mapOrderComment(c, order.id);
          mapped = addIngestionDate(mapped);
          commentsBuffer.push(mapped);
          commentsTracker.logProgress(true);
        });

        const attachments = await withRetry(
          () => fieldApi.listAll(`/orders/${order.id}/attachments`),
          `attachments/order/${order.id}`
        );

        attachments.forEach(a => {
          let mapped = mappers.mapOrderAttachment(a, order.id);
          mapped = addIngestionDate(mapped);
          attachmentsBuffer.push(mapped);
          attachmentsTracker.logProgress(true);
        });

        if (commentsBuffer.length >= CONFIG.SPECIAL_BATCH_SIZE) {
          if (!CONFIG.DRY_RUN) await bqService.upsertRows(commentsEntity, commentsBuffer.splice(0, CONFIG.SPECIAL_BATCH_SIZE));
          else commentsBuffer.splice(0, CONFIG.SPECIAL_BATCH_SIZE);
        }
        if (attachmentsBuffer.length >= CONFIG.SPECIAL_BATCH_SIZE) {
          if (!CONFIG.DRY_RUN) await bqService.upsertRows(attachmentsEntity, attachmentsBuffer.splice(0, CONFIG.SPECIAL_BATCH_SIZE));
          else attachmentsBuffer.splice(0, CONFIG.SPECIAL_BATCH_SIZE);
        }
      } catch (err) {
        console.error(`[Sub-recursos] Erro na OS ${order.id}: ${err.message}`);
      }
    }

    if (commentsBuffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.upsertRows(commentsEntity, commentsBuffer.splice(0, commentsBuffer.length));
    }
    if (attachmentsBuffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.upsertRows(attachmentsEntity, attachmentsBuffer.splice(0, attachmentsBuffer.length));
    }

    return {
      comments: commentsTracker.logCompletion(),
      attachments: attachmentsTracker.logCompletion()
    };
  } catch (err) {
    console.error(`[Sub-recursos] Falha crítica: ${err.message}`);
    throw err;
  }
}

async function loadTasks(options = {}) {
  const { startDate = CONFIG.DEFAULT_SEPTEMBER_START, endDate = null, ...rest } = options;
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `updated_at>=:${startDate}` } : {});

  return await runEntityLoad('tasks', '/tasks', mappers.mapTask, {
    longTimeout: true,
    paginated: true,
    useDetailEndpoint: true,
    extraQueryParams,
    endDate,
    ...rest
  });
}

async function loadFormsAnswers(options = {}) {
  const {
    startDate = CONFIG.DEFAULT_SEPTEMBER_START,
    endDate = null,
    pageSize = CONFIG.PAGE_SIZE,
    testLimit = CONFIG.TEST_MODE ? CONFIG.TEST_LIMIT : null,
  } = options;

  const entityName = 'forms_answers';
  const questionsEntity = 'forms_answers_questions';
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `created_at>=:${startDate}` } : {});
  const endTimestamp = endDate ? new Date(endDate).getTime() : null;

  console.log(`\n[Carga] Iniciando: ${entityName} e ${questionsEntity} (Filtro: ${extraQueryParams.q || 'sem filtro'})`);

  const rateLimiter = new RateLimiter();
  const concurrencyLimiter = new ConcurrencyLimiter(CONFIG.MAX_CONCURRENT_REQUESTS);
  const tracker = new ProgressTracker(entityName, 0);
  tracker.unknownTotal = true;

  try {
    let offset = 0;
    const formsBuffer = [];
    const questionsBuffer = [];
    let processedCount = 0;

    while (true) {
      if (isStopped) break;

      const pageAnswers = await withRetry(
        () => fieldApi.listPaginated('/forms-answers', offset, pageSize, extraQueryParams),
        'forms_answers_page_' + offset,
        CONFIG.LONG_TIMEOUT_MS
      );

      const items = pageAnswers?.items || [];
      if (items.length === 0) break;

      for (const item of items) {
        if (isStopped) break;

        if (endTimestamp && item.createdAt) {
          const itemTs = new Date(item.createdAt).getTime();
          if (!isNaN(itemTs) && itemTs > endTimestamp) continue;
        }

        await rateLimiter.acquire();

        try {
          let detail;
          await concurrencyLimiter.acquire();
          try {
            detail = await withRetry(() => fieldApi.getDetail('/forms-answers', item.id), item.id);
          } finally {
            concurrencyLimiter.release();
          }

          const { form, answers } = mappers.mapFormAnswer(detail);

          formsBuffer.push(form);
          if (answers?.length > 0) {
            questionsBuffer.push(...answers);
          }

          tracker.logProgress(true);

          if (formsBuffer.length >= CONFIG.SPECIAL_BATCH_SIZE) {
            if (!CONFIG.DRY_RUN) {
              await bqService.upsertRows(entityName, formsBuffer.splice(0, formsBuffer.length));
            } else {
              formsBuffer.splice(0, formsBuffer.length);
            }
          }

          if (questionsBuffer.length >= 100) {
            if (!CONFIG.DRY_RUN) {
              await bqService.insertRows(questionsEntity, questionsBuffer.splice(0, questionsBuffer.length));
            } else {
              questionsBuffer.splice(0, questionsBuffer.length);
            }
          }
        } catch (err) {
          tracker.logProgress(false);
          console.error(`[Forms] Erro ao processar ID ${item.id}: ${err.message}`);
        }

        processedCount++;
        if (testLimit && processedCount >= testLimit) break;
      }

      if (testLimit && processedCount >= testLimit) break;
      offset += items.length;
      if (items.length < pageSize) break;
    }

    if (formsBuffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.upsertRows(entityName, formsBuffer.splice(0, formsBuffer.length));
    }
    if (questionsBuffer.length > 0 && !CONFIG.DRY_RUN) {
      await bqService.insertRows(questionsEntity, questionsBuffer.splice(0, questionsBuffer.length));
    }

    return tracker.logCompletion();
  } catch (err) {
    console.error(`[Forms] Falha crítica no loop de formulários: ${err.message}`);
    throw err;
  }
}

async function loadTickets(options = {}) {
  const { startDate = CONFIG.DEFAULT_SEPTEMBER_START, endDate = null, ...rest } = options;
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `created_at>=:${startDate}` } : {});

  return await runEntityLoad('tickets', '/tickets', mappers.mapTicket, {
    longTimeout: true,
    paginated: true,
    useDetailEndpoint: true,
    extraQueryParams,
    endDate,
    ...rest
  });
}

async function loadRatings(options = {}) {
  const { startDate = CONFIG.DEFAULT_SEPTEMBER_START, endDate = null, ...rest } = options;
  const extraQueryParams = options.extraQueryParams || (startDate ? { q: `created_at>=:${startDate}` } : {});

  return await runEntityLoad('ratings', '/ratings', mappers.mapRating, {
    paginated: true,
    useDetailEndpoint: false,
    extraQueryParams,
    endDate,
    ...rest
  });
}

// ==================== CARGA CONSOLIDADA POR PERÍODO ====================

/**
 * Retorna o status da sincronização atual
 */
function getSyncStatus() {
  return {
    isRunning: !!activeSync,
    ...(activeSync || {})
  };
}

/**
 * Executa a carga/atualização completa de TODAS as entidades mapeadas para qualquer período de datas.
 * Suporta options.startDate, options.endDate, options.month (ex: "2026-09")
 */
async function loadPeriod(options = {}) {
  if (activeSync) {
    throw new Error(`[Sync Lock] Já existe uma sincronização em execução (${activeSync.description}, iniciada em ${activeSync.startedAt}).`);
  }

  isStopped = false;
  const period = normalizeDateRange(options);
  const startTime = Date.now();

  activeSync = {
    startedAt: new Date().toISOString(),
    description: period.description,
    startDate: period.startDate,
    endDate: period.endDate,
  };

  console.log('\n' + '='.repeat(85));
  console.log(`🚀 INICIANDO SINCRONIZAÇÃO TOTAL - FIELD CONTROL`);
  console.log(`📅 Período Configurado: ${period.description}`);
  console.log(`🔧 Modo: ${CONFIG.TEST_MODE ? 'TESTE (limite reduzido)' : 'PRODUÇÃO'} | Dry-Run: ${CONFIG.DRY_RUN ? 'SIM' : 'NÃO'}`);
  console.log('='.repeat(85));

  const results = {};

  try {
    const periodOpts = {
      startDate: period.startDate,
      endDate: period.endDate,
      ...options
    };

    // 1. Serviços (Catálogo completo de referência)
    console.log('\n[1/10] Sincronizando catálogo de Serviços...');
    results.services = await loadServices(periodOpts);

    // 2. Colaboradores (Catálogo completo de referência)
    console.log('\n[2/10] Sincronizando catálogo de Colaboradores...');
    results.employees = await loadEmployees(periodOpts);

    // 3. Clientes (Criados no período)
    console.log(`\n[3/10] Sincronizando Clientes (${period.description})...`);
    results.customers = await loadCustomers({ ...periodOpts, collectIds: true });

    // 4. Localidades (Vinculadas aos clientes atualizados no período)
    console.log(`\n[4/10] Sincronizando Localidades dos Clientes (${period.description})...`);
    const recentCustomerIds = results.customers?.ids || [];
    results.locations = await loadLocations({ customerIds: recentCustomerIds, ...periodOpts });

    // 5. Ordens de Serviço (Modificadas no período com Labels)
    console.log(`\n[5/10] Sincronizando Ordens de Serviço (${period.description})...`);
    results.orders = await loadOrders({ ...periodOpts, collectIds: true });

    // 6. Sub-recursos de Ordens (Comentários e Anexos das OSs do período)
    console.log(`\n[6/10] Sincronizando Comentários e Anexos das OSs (${period.description})...`);
    const recentOrderIds = results.orders?.ids || [];
    results.subresources = await loadOrderSubResources({ orderIds: recentOrderIds, startDate: period.startDate, ...periodOpts });

    // 7. Tarefas / Tasks (Modificadas no período)
    console.log(`\n[7/10] Sincronizando Tarefas (${period.description})...`);
    results.tasks = await loadTasks(periodOpts);

    // 8. Formulários e Perguntas/Respostas (Respondidos no período)
    console.log(`\n[8/10] Sincronizando Formulários e Respostas (${period.description})...`);
    results.forms = await loadFormsAnswers(periodOpts);

    // 9. Tickets / Chamados (Abertos no período)
    console.log(`\n[9/10] Sincronizando Tickets (${period.description})...`);
    results.tickets = await loadTickets(periodOpts);

    // 10. Avaliações / Ratings (Registradas no período)
    console.log(`\n[10/10] Sincronizando Avaliações (${period.description})...`);
    results.ratings = await loadRatings(periodOpts);

    // Relatório Final
    const totalMin = ((Date.now() - startTime) / 60000).toFixed(1);
    console.log('\n' + '='.repeat(85));
    console.log(`📊 RELATÓRIO CONSOLIDADO - ${period.description.toUpperCase()}`);
    console.log('='.repeat(85));

    let totalSuccess = 0;
    let totalErrors = 0;

    Object.entries(results).forEach(([key, res]) => {
      if (res && typeof res === 'object' && 'success' in res) {
        console.log(`✅ ${key.padEnd(20)}: ${res.success.toString().padStart(6)} ok | ${res.errors.toString().padStart(4)} erros | ${res.duration || '0'} min`);
        totalSuccess += res.success;
        totalErrors += res.errors;
      } else if (res && res.comments && res.attachments) {
        console.log(`✅ ${key.padEnd(20)}: comments ${res.comments.success} ok / ${res.comments.errors} err | attachments ${res.attachments.success} ok / ${res.attachments.errors} err`);
        totalSuccess += res.comments.success + res.attachments.success;
        totalErrors += res.comments.errors + res.attachments.errors;
      } else {
        console.log(`✅ ${key.padEnd(20)}: CONCLUÍDO`);
      }
    });

    console.log('='.repeat(85));
    console.log(`📈 TOTAL GERAL: ${totalSuccess} registros sincronizados | ${totalErrors} erros | Tempo Total: ${totalMin} min`);
    console.log(`🏁 SINCRONIZAÇÃO CONCLUÍDA COM SUCESSO!`);
    console.log('='.repeat(85) + '\n');

    return {
      period: period.description,
      totalSuccess,
      totalErrors,
      durationMinutes: totalMin,
      details: results
    };
  } catch (err) {
    console.error('\n❌ ERRO CRÍTICO NA SINCRONIZAÇÃO POR PERÍODO:', err.message);
    throw err;
  } finally {
    activeSync = null;
  }
}

/**
 * Carga consolidada específica para Setembro de 2026
 */
async function loadSeptember2026(options = {}) {
  return await loadPeriod({
    startDate: CONFIG.DEFAULT_SEPTEMBER_START,
    endDate: CONFIG.DEFAULT_SEPTEMBER_END,
    ...options
  });
}

/**
 * loadAll: Mantém compatibilidade com chamadas gerais, delegando para loadPeriod
 */
async function loadAll(options = {}) {
  return await loadPeriod(options);
}

/**
 * Execução em modo de teste (limite reduzido de registros)
 */
async function testLoad(options = {}) {
  process.env.TEST_MODE = 'true';
  isStopped = false;
  try {
    return await loadPeriod(options);
  } finally {
    delete process.env.TEST_MODE;
  }
}

/**
 * Interrompe qualquer carga em andamento
 */
function stopLoad() {
  isStopped = true;
  activeSync = null;
  console.log('[STOP] Sinal de interrupção enviado para os processos de carga.');
}

// ==================== SINCRONIZAÇÃO DELTA POR HORAS ====================

async function syncEntity(entityName, endpoint, mapper, hours = 24) {
  hours = Math.max(1, parseInt(hours) || 24);
  const fromDate = new Date(Date.now() - hours * 60 * 60 * 1000);
  const dateStr = fromDate.toISOString().split('.')[0] + 'Z';
  const qFilter = `updated_at>=:${dateStr}`;

  console.log(`\n🚀 [SYNC DELTA] ${entityName} → últimas ${hours}h (${qFilter})`);

  const options = {
    longTimeout: true,
    paginated: true,
    extraQueryParams: { q: qFilter },
    enrichWithLabels: entityName === 'orders',
  };

  return await runEntityLoad(entityName, endpoint, mapper, options);
}

async function syncOrders(hours = 24) {
  return await syncEntity('orders', '/orders', mappers.mapOrder, hours);
}

async function syncTasks(hours = 24) {
  return await syncEntity('tasks', '/tasks', mappers.mapTask, hours);
}

async function syncTickets(hours = 24) {
  return await syncEntity('tickets', '/tickets', mappers.mapTicket, hours);
}

// ==================== EXPORTAÇÃO ====================

module.exports = {
  // Sincronizações parametrizadas
  loadPeriod,
  syncPeriod: loadPeriod,
  loadSeptember2026,
  syncSeptember2026: loadSeptember2026,
  normalizeDateRange,

  // Status e controle
  getSyncStatus,
  loadAll,
  testLoad,
  stopLoad,

  // Sincronizações delta por horas
  syncOrders,
  syncTasks,
  syncTickets,

  // Carregadores individuais de cada entidade
  loadServices,
  loadEmployees,
  loadCustomers,
  loadLocations,
  loadOrders,
  loadOrderSubResources,
  loadTasks,
  loadFormsAnswers,
  loadTickets,
  loadRatings,
};

// ==================== EXECUÇÃO DIRETA VIA CLI ====================
// Suporta:
// node functions/initialLoad.js --month=2026-09
// node functions/initialLoad.js --startDate=2026-09-01 --endDate=2026-09-30
// node functions/initialLoad.js --test
if (require.main === module) {
  const args = process.argv.slice(2);
  const cliOptions = {};

  for (const arg of args) {
    if (arg.startsWith('--startDate=')) {
      cliOptions.startDate = arg.split('=')[1];
    } else if (arg.startsWith('--endDate=')) {
      cliOptions.endDate = arg.split('=')[1];
    } else if (arg.startsWith('--month=')) {
      cliOptions.month = arg.split('=')[1];
    } else if (arg === '--test') {
      cliOptions.test = true;
      process.env.TEST_MODE = 'true';
    } else if (arg === '--dry-run') {
      cliOptions.dryRun = true;
      process.env.DRY_RUN = 'true';
    }
  }

  console.log('Iniciando sincronização via CLI com parâmetros:', cliOptions);
  loadPeriod(cliOptions)
    .then((result) => {
      console.log('Resultado da execução:', JSON.stringify(result, null, 2));
      process.exit(0);
    })
    .catch((err) => {
      console.error('Falha na execução:', err.message);
      process.exit(1);
    });
}