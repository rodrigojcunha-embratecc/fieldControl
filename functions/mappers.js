// Helper function para formatar respostas (Necessário estar aqui para ser usado no mapFormAnswer)
const formatAnswer = (answer) => {
  if (answer === null || answer === undefined) return null;
  // Se for objeto ou array (ex: multipla escolha, fotos), converte para string JSON
  if (typeof answer === 'object') return JSON.stringify(answer);
  return String(answer);
};

module.exports = {
  // Mapeamento de Clientes
  mapCustomer: (c) => {
    const row = {
      id: c.id,
      external_id: c.external?.id,
      primary_location_id: c.primaryLocation?.id,
      name: c.name,
      code: c.code,
      documentNumber: c.documentNumber,
      notes: c.notes,
      archived: !!c.archived,
      createdAt: c.createdAt,
      email: c.contact?.email,
      phone: c.contact?.phone,
      zipCode: c.address?.zipCode,
      street: c.address?.street,
      number: c.address?.number,
      neighborhood: c.address?.neighborhood,
      city: c.address?.city,
      state: c.address?.state,
      formattedAddress: c.address?.formattedAddress,
      lat: c.address?.coords?.latitude,
      lng: c.address?.coords?.longitude,
      stats_first_service: c.statistics?.service?.firstAt,
      stats_last_service: c.statistics?.service?.lastAt,
      stats_total_services: c.statistics?.service?.total || 0,
      stats_average_stars: c.statistics?.rating?.averageStars || 0,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  // Mapeamento de Tasks
  mapTask: (t) => {
    const row = {
      id: t.id,
      duration: t.duration,
      status: t.status,
      startedAt: t.startedAt,
      completedAt: t.completedAt,
      receivedAt: t.receivedAt,
      createdAt: t.createdAt,
      updatedAt: t.updatedAt,
      statusDescription: t.statusDescription,
      description: t.description,
      employee_id: t.employee?.id,
      order_id: t.order?.id,
      customer_id: t.customer?.id,
      scheduling_type: t.scheduling?.type,
      scheduling_date: t.scheduling?.date,
      scheduling_time: t.scheduling?.time,
      lat_start: t.startCoords?.latitude,
      lng_start: t.startCoords?.longitude,
      location_id: t.location?.id,
      lat_complete: t.completeCoords?.latitude,
      lng_complete: t.completeCoords?.longitude,
      ratingLink: t.ratingLink,
      archived: t.archived || false
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  // Mapeamento de Tickets
  mapTicket: (t) => {
    const row = {
      id: t.id,
      name: t.name,
      subject: t.subject,
      message: t.message,
      status: t.status,
      createdAt: t.createdAt,
      customer_id: t.customer?.id,
      service_id: t.service?.id,
      location_id: t.location?.id,
      contact_email: t.contact?.email,
      contact_phone: t.contact?.phone
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  // Mapeador de Locais
  mapLocation: (l) => {
    const row = {
      id: l.id,
      name: l.name,
      customer_id: l.customer?.id,
      archived: l.archived,
      zipCode: l.address?.postalCode || l.address?.zipCode,
      street: l.address?.street,
      number: l.address?.number,
      neighborhood: l.address?.neighborhood,
      city: l.address?.city,
      state: l.address?.state,
      formattedAddress: l.address?.formattedAddress,
      latitude: l.address?.coords?.latitude,
      longitude: l.address?.coords?.longitude,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  // Mapeador de Ordens de Serviço
  mapOrder: (o, labels = []) => {
    const row = {
      id: o.id,
      identifier: o.identifier,
      description: o.description,
      status_archived: o.archived,
      link_report: o.link,
      products_value: o.productsTotalValue || 0,
      services_value: o.servicesTotalValue || 0,
      total_value: o.totalValue || 0,
      customer_id: o.customer?.id,
      service_id: o.service?.id,
      location_id: o.location?.id,
      ticket_id: o.ticket?.id,
      createdAt: o.createdAt,
      updatedAt: o.updatedAt,
      deadlineContract: o.deadlineContract,
      exec_city: o.address?.city,
      exec_state: o.address?.state,
      exec_lat: o.address?.coords?.latitude,
      exec_lng: o.address?.coords?.longitude,
      external_id: o.external?.id,
      labels: labels && labels.length > 0 ? labels.map(l => l.name).join(', ') : null,
      labels_array: labels && labels.length > 0 ? labels.map(l => l.name) : [],
      labels_count: labels ? labels.length : 0,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  mapOrderComment: (c, orderId) => {
    const row = {
      id: c.id,
      order_id: orderId,
      message: c.message,
      createdAt: c.createdAt,
      type: c.type,
      user_id: c.user?.id,
      postedBy_name: c.postedBy?.name,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  mapOrderAttachment: (a, orderId) => {
    const row = {
      id: a.id,
      title: a.title,
      order_id: orderId,
      task_id: a.task?.id,
      path: a.path,
      createdAt: a.createdAt,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  mapService: (s) => {
    const row = {
      id: s.id,
      name: s.name,
      duration_planned: s.duration,
      archived: s.archived,
      createdAt: s.createdAt,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  mapEmployee: (e) => {
    const row = {
      id: e.id,
      name: e.name,
      email: e.email,
      isActive: e.isActive,
      avatarUrl: e.avatarUrl,
      createdAt: e.createdAt,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  mapRating: (r) => {
    const row = {
      task_id: r.task?.id,
      stars: r.stars,
      comment: r.comment,
      createdAt: r.createdAt,
      extracted_at: new Date().toISOString()
    };
    row.ingestion_date = new Date().toISOString().split('T')[0];
    return row;
  },

  // Mapeia o cabeçalho do formulário e suas perguntas
  mapFormAnswer: (f) => {
    const ingestionDate = new Date().toISOString().split('T')[0];
    const extractedAt   = new Date().toISOString();

    const form = {
      id:               f.id,
      form_name:        f.name,
      form_id:          f.form?.id ?? null,
      order_id:         f.order?.id ?? null,
      archived:         !!f.archived,
      created_at:       f.createdAt ? new Date(f.createdAt).toISOString() : null,
      answered_by_id:   f.answeredBy?.updatedById ?? f.answeredBy?.id ?? null,
      answered_by_type: f.answeredBy?.updatedByType ?? f.answeredBy?.type ?? null,
      score:            f.score ?? null,
      ingestion_date:   ingestionDate,
      extracted_at:     extractedAt
    };

    const answers = (f.questions || []).map(q => ({
      form_answer_id:   f.id,
      position:         q.position ?? null,
      title:            q.title    ?? null,
      type:             q.type     ?? null,
      required:         !!q.required,
      order_id:         f.order?.id ?? null,
      answer:           formatAnswer(q.answer), // Usa a função definida no topo
      ingestion_date:   ingestionDate,
      extracted_at:     extractedAt
    }));

    return { form, answers };
  }
};