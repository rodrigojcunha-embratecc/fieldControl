require('dotenv').config();
const express = require('express');
const app = express();

const { 
  loadAll, 
  loadPeriod,
  loadSeptember2026,
  testLoad,
  stopLoad,
  getSyncStatus,
  syncOrders,
  syncTasks,
  syncTickets 
} = require('./functions/initialLoad');

// Importa o handler do webhook
const { fieldControlWebhook } = require('./functions/webhookReceiver');

app.use(express.json());

// Middleware de log de requisições
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// Middleware de autenticação de segurança para endpoints de sincronização e ingestão
function authenticate(req, res, next) {
  const authToken = process.env.API_AUTH_TOKEN || process.env.LOAD_TOKEN;
  
  // Se nenhuma chave de segurança foi configurada no ambiente, permite a requisição
  if (!authToken) {
    return next();
  }

  // Permite autenticação via query param ?token=...
  if (req.query.token && req.query.token.trim() === authToken.trim()) {
    return next();
  }

  const authHeader = req.headers['authorization'] || req.headers['x-api-key'];
  if (!authHeader) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Token de autenticação obrigatório. Envie o cabeçalho Authorization: Bearer <token>, x-api-key ou query param ?token=.'
    });
  }

  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : authHeader.trim();
  if (token !== authToken.trim()) {
    return res.status(403).json({
      error: 'Forbidden',
      message: 'Token de autenticação inválido.'
    });
  }

  next();
}

// ====================== ROTAS DE SINCRONIZAÇÃO POR PERÍODO / MÊS ======================

/**
 * GET /sync/period?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&test=true
 * Sincroniza todas as 10 entidades mapeadas para qualquer intervalo de datas
 */
app.get('/sync/period', authenticate, async (req, res) => {
  const isTest = req.query.test === 'true';
  const { startDate, endDate } = req.query;

  if (!startDate) {
    return res.status(400).json({
      error: 'Bad Request',
      message: 'O parâmetro startDate é obrigatório (ex: 2026-09-01 ou 2026-09-01T00:00:00Z).'
    });
  }

  const status = getSyncStatus();
  if (status.isRunning) {
    return res.status(409).json({
      status: 'conflict',
      message: 'Uma sincronização já está em andamento. Aguarde a finalização ou consulte /sync/status.',
      currentSync: status
    });
  }

  res.status(202).json({
    status: 'accepted',
    mode: isTest ? 'test' : 'full',
    startDate,
    endDate: endDate || 'até o fim do período',
    message: `Sincronização iniciada em background para o período informado.`,
    startedAt: new Date().toISOString()
  });

  try {
    const options = { startDate, endDate };
    if (isTest) {
      await testLoad(options);
    } else {
      await loadPeriod(options);
    }
    console.log(`[SYNC PERIOD] Carga de ${startDate} a ${endDate || 'fim'} concluída com sucesso.`);
  } catch (error) {
    console.error('[SYNC PERIOD] ERRO CRÍTICO:', error.message);
  }
});

/**
 * GET /sync/month?month=YYYY-MM&test=true
 * Atalho conveniente para sincronizar um mês completo (ex: month=2026-09)
 */
app.get('/sync/month', authenticate, async (req, res) => {
  const isTest = req.query.test === 'true';
  const month = req.query.month || '2026-09';

  const status = getSyncStatus();
  if (status.isRunning) {
    return res.status(409).json({
      status: 'conflict',
      message: 'Uma sincronização já está em andamento.',
      currentSync: status
    });
  }

  res.status(202).json({
    status: 'accepted',
    month,
    mode: isTest ? 'test' : 'full',
    message: `Sincronização do mês ${month} para todas as entidades iniciada em background.`,
    startedAt: new Date().toISOString()
  });

  try {
    const options = { month };
    if (isTest) {
      await testLoad(options);
    } else {
      await loadPeriod(options);
    }
    console.log(`[SYNC MONTH] Carga do mês ${month} finalizada com sucesso.`);
  } catch (error) {
    console.error(`[SYNC MONTH ${month}] ERRO CRÍTICO:`, error.message);
  }
});

/**
 * GET /sync/september-2026
 * Atalho direto para Setembro de 2026
 */
app.get('/sync/september-2026', authenticate, async (req, res) => {
  const isTest = req.query.test === 'true';

  const status = getSyncStatus();
  if (status.isRunning) {
    return res.status(409).json({
      status: 'conflict',
      message: 'Uma sincronização já está em andamento.',
      currentSync: status
    });
  }

  res.status(202).json({
    status: 'accepted',
    period: '2026-09 (Setembro de 2026)',
    mode: isTest ? 'test' : 'full',
    message: 'Sincronização de Setembro de 2026 para todas as entidades iniciada em background.',
    startedAt: new Date().toISOString()
  });

  try {
    if (isTest) {
      await testLoad();
    } else {
      await loadSeptember2026();
    }
    console.log('[SYNC SETEMBRO-2026] Sincronização finalizada com sucesso.');
  } catch (error) {
    console.error('[SYNC SETEMBRO-2026] ERRO CRÍTICO:', error.message);
  }
});

/**
 * GET /initial-load
 * Carga inicial retrocompatível com suporte a parâmetros opcionais de data
 */
app.get('/initial-load', authenticate, async (req, res) => {
  const isTest = req.query.test === 'true';
  const { startDate, endDate, month } = req.query;

  const status = getSyncStatus();
  if (status.isRunning) {
    return res.status(409).json({
      status: 'conflict',
      message: 'Uma sincronização já está em andamento.',
      currentSync: status
    });
  }

  res.status(202).json({
    status: 'accepted',
    message: 'Carga de dados iniciada em background.',
    mode: isTest ? 'test' : 'full',
    startDate: startDate || (month ? `Mês ${month}` : '2026-09-01T00:00:00Z'),
    endDate: endDate || '2026-09-30T23:59:59Z',
    startedAt: new Date().toISOString()
  });

  try {
    const options = {};
    if (startDate) options.startDate = startDate;
    if (endDate) options.endDate = endDate;
    if (month) options.month = month;

    if (isTest) {
      await testLoad(options);
    } else {
      await loadAll(options);
    }
    console.log('[INITIAL-LOAD] Carga finalizada com sucesso.');
  } catch (error) {
    console.error('[INITIAL-LOAD] ERRO CRÍTICO:', error.message);
  }
});

// ====================== STATUS E CONTROLE DE EXECUÇÃO ======================

app.get('/sync/status', (req, res) => {
  res.status(200).json({
    timestamp: new Date().toISOString(),
    ...getSyncStatus()
  });
});

app.post('/sync/stop', authenticate, (req, res) => {
  stopLoad();
  res.status(200).json({
    status: 'stopped',
    message: 'Sinal de interrupção enviado para as rotinas de carga.',
    timestamp: new Date().toISOString()
  });
});

// ====================== ENDPOINTS DE SINCRONIZAÇÃO DELTA ======================

app.get('/sync/orders', authenticate, async (req, res) => {
  const hours = parseInt(req.query.hours) || 24;

  res.status(202).json({
    status: 'accepted',
    entity: 'orders',
    hours,
    message: `Sincronização delta de ORDERS iniciada (${hours}h)`,
    startedAt: new Date().toISOString()
  });

  try {
    const result = await syncOrders(hours);
    console.log(`[SYNC ORDERS] Concluído → ${result.success} ok | ${result.errors} erros`);
  } catch (error) {
    console.error('[SYNC ORDERS] ERRO CRÍTICO:', error.message);
  }
});

app.get('/sync/tasks', authenticate, async (req, res) => {
  const hours = parseInt(req.query.hours) || 24;

  res.status(202).json({
    status: 'accepted',
    entity: 'tasks',
    hours,
    message: `Sincronização delta de TASKS iniciada (${hours}h)`,
    startedAt: new Date().toISOString()
  });

  try {
    const result = await syncTasks(hours);
    console.log(`[SYNC TASKS] Concluído → ${result.success} ok | ${result.errors} erros`);
  } catch (error) {
    console.error('[SYNC TASKS] ERRO CRÍTICO:', error.message);
  }
});

app.get('/sync/tickets', authenticate, async (req, res) => {
  const hours = parseInt(req.query.hours) || 24;

  res.status(202).json({
    status: 'accepted',
    entity: 'tickets',
    hours,
    message: `Sincronização delta de TICKETS iniciada (${hours}h)`,
    startedAt: new Date().toISOString()
  });

  try {
    const result = await syncTickets(hours);
    console.log(`[SYNC TICKETS] Concluído → ${result.success} ok | ${result.errors} erros`);
  } catch (error) {
    console.error('[SYNC TICKETS] ERRO CRÍTICO:', error.message);
  }
});

app.get('/sync/all', authenticate, async (req, res) => {
  const hours = parseInt(req.query.hours) || 24;

  res.status(202).json({
    status: 'accepted',
    entity: 'all',
    hours,
    message: `Sincronização delta COMPLETA (orders + tasks + tickets) iniciada (${hours}h)`,
    startedAt: new Date().toISOString()
  });

  try {
    console.log(`[SYNC ALL] Iniciando sincronização completa (${hours}h)...`);
    await syncOrders(hours);
    await syncTasks(hours);
    await syncTickets(hours);
    console.log('[SYNC ALL] Sincronização completa finalizada com sucesso!');
  } catch (error) {
    console.error('[SYNC ALL] ERRO CRÍTICO:', error.message);
  }
});

// ====================== OUTRAS ROTAS ======================

app.post('/webhook', fieldControlWebhook);

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'healthy',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Servidor rodando na porta ${PORT} - ${new Date().toISOString()}`);
  if (!process.env.API_AUTH_TOKEN) {
    console.warn(`⚠️  [Segurança] Variável API_AUTH_TOKEN não configurada. Endpoints de sincronização estão abertos.`);
  } else {
    console.log(`🔒 [Segurança] Autenticação de endpoints ativada via API_AUTH_TOKEN.`);
  }
  console.log(`📡 Endpoints disponíveis:`);
  console.log(`   → GET  /sync/period?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD`);
  console.log(`   → GET  /sync/month?month=YYYY-MM`);
  console.log(`   → GET  /sync/september-2026`);
  console.log(`   → GET  /sync/status`);
  console.log(`   → POST /sync/stop`);
  console.log(`   → GET  /sync/orders?hours=6`);
  console.log(`   → GET  /sync/tasks?hours=12`);
  console.log(`   → GET  /sync/tickets?hours=24`);
  console.log(`   → GET  /sync/all?hours=24`);
  console.log(`   → POST /webhook`);
  console.log(`   → GET  /health`);
});