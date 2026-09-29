require('dotenv').config();
const axios = require('axios');
const CancelToken = axios.CancelToken;

const API_KEY = process.env.FIELD_CONTROL_API_KEY;
const BASE_URL = 'https://carchost.fieldcontrol.com.br';

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 180000,          // Aumentado para 3 minutos (para páginas grandes + rede lenta)
  headers: {
    'X-Api-Key': API_KEY,
    'Content-Type': 'application/json;charset=UTF-8',
    'User-Agent': 'BI-Integration-NodeJS'
  }
});

// Helper para delay entre requisições (ajuda a respeitar rate limit)
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Lista uma página específica de itens
 * @param {string} endpoint 
 * @param {number} offset 
 * @param {number} limit 
 * @param {object} extraParams 
 * @returns {Promise<{items: Array, totalCount: number}>}
 */
async function listPaginated(endpoint, offset = 0, limit = 50, extraParams = {}) {
  try {
    const params = { ...extraParams, limit, offset };
    const response = await api.get(endpoint, { params });
    
    return {
      items: response.data.items || [],
      totalCount: response.data.totalCount || 0
    };
  } catch (err) {
    console.error(`Erro ao listar página ${endpoint} offset=${offset}:`, err.message);
    throw err;
  }
}

/**
 * Lista TODOS os itens (mantido para compatibilidade, mas evite usar em entidades grandes)
 * @param {string} endpoint 
 * @param {object} params 
 * @returns {Promise<Array>}
 */
async function listAll(endpoint, params = {}) {
  let allItems = [];
  let offset = 0;
  const limit = 50;
  let totalCount = Infinity; // para entrar no loop na primeira vez

  while (offset < totalCount) {
    const { items, totalCount: newTotal } = await listPaginated(endpoint, offset, limit, params);
    totalCount = newTotal;
    
    allItems = allItems.concat(items);
    offset += limit;
    
    console.log(`[listAll] ${endpoint}: ${allItems.length}/${totalCount}`);
    
    if (items.length < limit) break; // segurança extra
    
    await sleep(1800); // ~2s entre páginas
  }

  return allItems;
}

/**
 * Busca detalhe de um item específico
 * @param {string} endpoint 
 * @param {string} id 
 * @returns {Promise<object>}
 */
async function getDetail(endpoint, id) {
  // Removido sleep fixo de 2s aqui → rate limit é mais relevante entre páginas/listagens
  // Se precisar, adicione sleep condicional no caller
  const response = await api.get(`${endpoint}/${id}`);
  return response.data;
}


/**
 * Busca as labels de uma ordem específica
 */
async function getOrderLabels(orderId) {
  try {
    const response = await api.get(`/orders/${orderId}/labels`);
    return response.data.items || [];
  } catch (err) {
    if (err.response?.status === 404 || err.response?.status === 400) {
      return [];
    }
    console.error(`Erro ao buscar labels da ordem ${orderId}:`, err.message);
    return []; // não quebra o fluxo
  }
}

module.exports = {
  listAll,       // mantido para compatibilidade
  listPaginated, // novo método recomendado para evitar timeout e memória alta
  getDetail,
  getOrderLabels
};