const bqService = require('../services/bigQueryService');
const fieldApi = require('../services/fieldControlApi');
const mappers = require('./mappers');

// Cache em memória para evitar consultas desnecessárias e chamadas de API repetidas
let knownCustomers = null;
let knownLocations = null;
let cacheInitializing = false;

async function ensureCustomerAndLocation(customerId, locationId) {
  // Inicialização do cache com bloqueio simples contra concorrência
  if (!knownCustomers && !cacheInitializing) {
    cacheInitializing = true;
    try {
      console.log('[Webhook Cache] Inicializando cache de clientes e localizações...');
      const allCusts = await bqService.getAllCustomerIds();
      knownCustomers = new Set(allCusts);
      
      const allLocs = await bqService.getAllLocationIds();
      knownLocations = new Set(allLocs);
      console.log(`[Webhook Cache] Cache carregado: ${knownCustomers.size} clientes, ${knownLocations.size} localizações.`);
    } catch (err) {
      console.error('[Webhook Cache] Falha ao carregar cache do BigQuery:', err.message);
      // fallback
      knownCustomers = new Set();
      knownLocations = new Set();
    } finally {
      cacheInitializing = false;
    }
  }

  // Espera breve caso outra chamada esteja inicializando o cache
  while (cacheInitializing) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Se o cliente não está no cache, busca na API e faz o upsert
  if (customerId && !knownCustomers.has(customerId)) {
    try {
      console.log(`[Webhook Cache] Cliente novo detectado: ${customerId}. Buscando detalhes...`);
      const custDetail = await fieldApi.getDetail('/customers', customerId);
      const mappedCust = mappers.mapCustomer(custDetail);
      await bqService.upsertRows('customers', [mappedCust]);
      knownCustomers.add(customerId);
      console.log(`[Webhook Cache] Cliente ${customerId} salvo com sucesso.`);
    } catch (err) {
      console.error(`[Webhook Cache] Erro ao sincronizar cliente novo ${customerId}:`, err.message);
    }
  }

  // Se a localização não está no cache, busca na API e faz o upsert
  if (locationId && !knownLocations.has(locationId)) {
    try {
      console.log(`[Webhook Cache] Localização nova detectada: ${locationId}. Buscando detalhes...`);
      if (customerId) {
        const locations = await fieldApi.listAll(`/customers/${customerId}/locations`);
        const targetLoc = locations.find(l => l.id === locationId);
        if (targetLoc) {
          const mappedLoc = mappers.mapLocation(targetLoc);
          mappedLoc.customer_id = customerId; // garante vinculo correto
          await bqService.upsertRows('locations', [mappedLoc]);
          knownLocations.add(locationId);
          console.log(`[Webhook Cache] Localização ${locationId} salva com sucesso.`);
        } else {
          console.warn(`[Webhook Cache] Localização ${locationId} não encontrada na API do cliente ${customerId}.`);
        }
      }
    } catch (err) {
      console.error(`[Webhook Cache] Erro ao sincronizar localização nova ${locationId}:`, err.message);
    }
  }
}

exports.fieldControlWebhook = async (req, res) => {
  const webhookSecret = process.env.FIELD_CONTROL_WEBHOOK_SECRET;
  if (webhookSecret && req.headers['x-fieldcontrol-secret'] !== webhookSecret) {
    console.warn(`[Webhook Security] Requisição rejeitada: token/secret de webhook inválido`);
    return res.status(401).json({ error: 'Unauthorized: Invalid webhook secret' });
  }
  const eventType = req.headers['x-fieldcontrol-event'];
  const deliveryId = req.headers['x-fieldcontrol-delivery'];
  const accountId = req.headers['x-fieldcontrol-account'];
  const data = req.body;

  console.log(`🚀 Webhook recebido: ${eventType} | Delivery: ${deliveryId} | Account: ${accountId} | ID: ${data.id || 'sem id'}`);

  try {
    let mappedRow = null;
    let tableId = '';

    switch (eventType) {
      // Orders
      case 'order-created':
      case 'order-updated':
      try {
            // 1. Busca o detalhe completo da ordem
            const orderDetail = await fieldApi.getDetail('/orders', data.id);

            // 1.1 Sincroniza Cliente e Localidade sob demanda
            const customerId = orderDetail.customer?.id;
            const locationId = orderDetail.location?.id;
            if (customerId || locationId) {
              await ensureCustomerAndLocation(customerId, locationId);
            }

            // 2. Busca as labels (com tratamento de erro)
            let labels = [];
            try {
              labels = await fieldApi.getOrderLabels(data.id);
            } catch (labelErr) {
              console.warn(`[Labels] Não foi possível buscar labels da OS ${data.id}: ${labelErr.message}`);
              labels = []; // garante que labels sempre exista
            }

            // 3. Mapeia com labels
            mappedRow = mappers.mapOrder(orderDetail, labels);
            tableId = 'orders';

            console.log(`✅ Order + Labels processada: ${data.id} | ${labels.length} labels`);
          } catch (err) {
            console.error(`❌ Erro ao processar order ${data.id}:`, err.message);
          }
          break;

      // Tasks
      case 'task-created':
      case 'task-updated':
      case 'task-started':
      case 'task-start-route':
      case 'task-route-started':
      case 'task-completed':
      case 'task-reported':
      case 'task-rated':
        const taskDetail = await fieldApi.getDetail('/tasks', data.id);
        mappedRow = mappers.mapTask(taskDetail);
        tableId = 'tasks';
        break;

      // Comments
      case 'comment-created':
        const orderId = data.order?.id || data.task?.order?.id;
        if (!orderId) {
          console.warn(`Comentário sem ordem associada - ignorando upsert | id: ${data.id}`);
          break;
        }
        mappedRow = mappers.mapOrderComment(data, orderId);
        tableId = 'order_comments';
        break;

            // 2. AJUSTE: Lógica específica para Formulários (Upsert Duplo) – VERSÃO CORRIGIDA
      case 'order-form-created':
      case 'order-form-updated':
          try {
              // Busca o detalhe completo (igual orders/tasks) – garante que temos o array questions
              const formDetail = await fieldApi.getDetail('/forms-answers', data.id);
              
              const { form, answers } = mappers.mapFormAnswer(formDetail);
              
              // Upsert no cabeçalho
              await bqService.upsertRows('forms_answers', [form]);
              
              // INSERT nas perguntas (evita erro de ID)
              if (answers && answers.length > 0) {
                  console.log(`[Webhook] Inserindo ${answers.length} perguntas para o form ${form.id}`);
                  await bqService.insertRows('forms_answers_questions', answers);
              }
              
              console.log(`✅ Formulário processado: ${form.id} | ${answers.length} perguntas`);
          } catch (err) {
              console.error(`❌ Erro ao processar formulário ${data.id || 'sem id'}: ${err.message}`);
          }
          break;

      // Anexos
      case 'order-attachment-created':
      case 'order-attachment-updated':
        const relatedOrderId = data.order?.id || data.task?.order?.id;
        if (!relatedOrderId) {
          console.warn(`Anexo sem ordem associada - ignorando upsert | id: ${data.id}`);
          break;
        }
        mappedRow = mappers.mapOrderAttachment(data, relatedOrderId);
        tableId = 'attachments';
        break;

      // Colaboradores
      case 'employee-created':
        const empDetail = await fieldApi.getDetail('/employees', data.id);
        mappedRow = mappers.mapEmployee(empDetail);
        tableId = 'employees';
        break;

      // Tickets
      case 'ticket-created':
      case 'ticket-accepted':
      case 'ticket-canceled':
        const ticketDetail = await fieldApi.getDetail('/tickets', data.id);
        mappedRow = mappers.mapTicket(ticketDetail);
        tableId = 'tickets';
        break;

      default:
        console.log(`⚠️ Evento não tratado: ${eventType}`);
        return res.status(200).send('Evento não processado');
    }

    // Upsert padrão para os casos de tabela única (Orders, Tasks, etc)
    if (tableId && mappedRow) {
      await bqService.upsertRows(tableId, [mappedRow]);
      console.log(`✅ Upsert realizado: ${tableId} | id: ${mappedRow.id || 'sem id'} | event: ${eventType}`);
    }

    res.status(200).send('Webhook Processed');
  } catch (err) {
    console.error(`❌ Erro processando webhook ${eventType} | Delivery ${deliveryId}:`, {
      message: err.message,
      stack: err.stack,
      response: err.response?.data || err.response
    });
    res.status(200).send('Erro interno registrado');
  }
};