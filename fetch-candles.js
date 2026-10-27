/**
 * HybriOx-AI - Professional Data Ingestion Pipeline
 * Script: fetch-candles.js
 * Description: Rolling queue data fetcher with multi-provider fallback (Estafet System)
 * Updated: Flipped and restricted to TARGET_PAIRS: XAUUSD, GBPJPY, EURUSD
 */

import admin from 'firebase-admin';

// 1. Inisialisasi Firebase Admin menggunakan secret environment
if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
  console.error('❌ Error: FIREBASE_SERVICE_ACCOUNT environment variable is missing.');
  process.exit(1);
}

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);

if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount)
  });
}

const db = admin.firestore();

// 2. Konfigurasi Utama (Disesuaikan hanya untuk 3 pair pilihan utama)
const TARGET_PAIRS = ['XAUUSD', 'GBPJPY', 'EURUSD'];
const CANDLE_COUNT = 400;
const TIMEFRAME = '30min';

// Daftar Provider berjenjang (Estafet)
const PROVIDERS = [
  { name: 'Twelve Data', keyEnv: 'TWELVE_DATA_API_KEY' },
  { name: 'Alpha Vantage', keyEnv: 'ALPHA_VANTAGE_API_KEY' },
  { name: 'Finnhub', keyEnv: 'FINNHUB_API_KEY' }
];

/**
 * Fungsi Fetcher untuk Twelve Data (Mendukung XAUUSD & Forex secara native)
 */
async function fetchFromTwelveData(symbol, apiKey) {
  let t12Symbol = symbol;
  if (symbol === 'XAUUSD') t12Symbol = 'XAU/USD';
  else if (symbol === 'XAGUSD') t12Symbol = 'XAG/USD';
  else if (symbol.length === 6 && !symbol.includes('/')) {
    t12Symbol = `${symbol.slice(0, 3)}/${symbol.slice(3, 6)}`;
  }

  const url = `https://api.twelvedata.com/time_series?symbol=${t12Symbol}&interval=${TIMEFRAME}&outputsize=${CANDLE_COUNT}&apikey=${apiKey}`;
  const response = await fetch(url);
  const data = await response.json();

  if (data.status === 'error' || !data.values) {
    throw new Error(data.message || 'Twelve Data limit or error');
  }

  return data.values.map(item => ({
    time: item.datetime,
    open: parseFloat(item.open),
    high: parseFloat(item.high),
    low: parseFloat(item.low),
    close: parseFloat(item.close),
    volume: parseFloat(item.volume || 0)
  })).reverse();
}

/**
 * Fungsi Fetcher untuk Alpha Vantage (Menggunakan endpoint FX_INTRADAY khusus Forex & Gold)
 */
async function fetchFromAlphaVantage(symbol, apiKey) {
  let fromSymbol = 'EUR';
  let toSymbol = 'USD';

  if (symbol === 'XAUUSD') {
    fromSymbol = 'XAU';
    toSymbol = 'USD';
  } else if (symbol === 'XAGUSD') {
    fromSymbol = 'XAG';
    toSymbol = 'USD';
  } else if (symbol.length === 6) {
    fromSymbol = symbol.slice(0, 3);
    toSymbol = symbol.slice(3, 6);
  }

  const url = `https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol=${fromSymbol}&to_symbol=${toSymbol}&interval=30min&outputsize=full&apikey=${apiKey}`;
  
  const response = await fetch(url);
  const data = await response.json();
  
  // Mencari key time series secara dinamis
  const timeSeriesKey = Object.keys(data).find(k => k.includes('Time Series') || k.includes('FX Intraday'));
  if (!data[timeSeriesKey]) {
    throw new Error(data['Note'] || data['Error Message'] || 'Alpha Vantage limit or invalid response');
  }

  const entries = Object.entries(data[timeSeriesKey]).slice(0, CANDLE_COUNT);
  return entries.map(([time, values]) => ({
    time: time,
    open: parseFloat(values['1. open']),
    high: parseFloat(values['2. high']),
    low: parseFloat(values['3. low']),
    close: parseFloat(values['4. close']),
    volume: parseFloat(values['5. volume'] || 0)
  })).reverse();
}

/**
 * Fungsi Fetcher untuk Finnhub (Menggunakan endpoint forex/candle untuk XAUUSD & Forex, bukan crypto)
 */
async function fetchFromFinnhub(symbol, apiKey) {
  let fhSymbol = 'EUR_USD';
  if (symbol === 'XAUUSD') fhSymbol = 'XAU_USD';
  else if (symbol === 'XAGUSD') fhSymbol = 'XAG_USD';
  else if (symbol.length === 6) {
    fhSymbol = `${symbol.slice(0, 3)}_${symbol.slice(3, 6)}`;
  }

  const now = Math.floor(Date.now() / 1000);
  const from = now - (CANDLE_COUNT * 30 * 60); // 30 menit per candle
  const url = `https://finnhub.io/api/v1/forex/candle?symbol=OANDA:${fhSymbol}&resolution=30&from=${from}&to=${now}&token=${apiKey}`;
  
  const response = await fetch(url);
  const data = await response.json();

  if (data.s !== 'ok' || !data.t) {
    throw new Error('Finnhub data error or rate limit');
  }

  return data.t.map((timestamp, index) => ({
    time: new Date(timestamp * 1000).toISOString(),
    open: data.o[index],
    high: data.h[index],
    low: data.l[index],
    close: data.c[index],
    volume: parseFloat(data.v ? data.v[index] : 0)
  }));
}

/**
 * Router utama eksekusi estafet per pair
 */
async function fetchCandlesWithEstafet(symbol) {
  for (const provider of PROVIDERS) {
    const apiKey = process.env[provider.keyEnv];
    if (!apiKey) {
      console.warn(`⚠️ API Key untuk ${provider.name} (${provider.keyEnv}) tidak ditemukan. Lewati.`);
      continue;
    }

    try {
      console.log(`🔄 Mengambil ${symbol} via ${provider.name}...`);
      let candles = [];

      if (provider.name === 'Twelve Data') {
        candles = await fetchFromTwelveData(symbol, apiKey);
      } else if (provider.name === 'Alpha Vantage') {
        candles = await fetchFromAlphaVantage(symbol, apiKey);
      } else if (provider.name === 'Finnhub') {
        candles = await fetchFromFinnhub(symbol, apiKey);
      }

      if (candles && candles.length > 0) {
        console.log(`✅ Sukses mengambil ${candles.length} candle untuk ${symbol} dari ${provider.name}`);
        return {
          success: true,
          provider: provider.name,
          candles: candles.slice(-CANDLE_COUNT)
        };
      }
    } catch (error) {
      console.warn(`⚠️ Gagal pada ${provider.name} untuk ${symbol}: ${error.message}. Melanjutkan ke provider berikutnya...`);
    }
  }

  throw new Error(`❌ Semua provider gagal mengambil data untuk pair: ${symbol}`);
}

/**
 * Eksekusi Utama Pipeline
 */
async function main() {
  console.log('🚀 Memulai siklus pengambilan data makro HybriOx-AI M30 (Fokus: XAUUSD, GBPJPY, EURUSD)...');
  const payloadData = {
    lastUpdated: new Date().toISOString(),
    status: 'success',
    providersUsed: {},
    candles: {}
  };

  let hasError = false;

  for (const symbol of TARGET_PAIRS) {
    try {
      const result = await fetchCandlesWithEstafet(symbol);
      payloadData.candles[symbol] = result.candles;
      payloadData.providersUsed[symbol] = result.provider;
    } catch (err) {
      console.error(err.message);
      hasError = true;
      payloadData.candles[symbol] = [];
      payloadData.providersUsed[symbol] = 'FAILED';
    }
  }

  if (hasError) {
    payloadData.status = 'partial_success';
  }

  const docRef = db.collection('macro_candles').doc('latest_m30');
  await docRef.set(payloadData, { merge: true });

  console.log('✨ Data berhasil disimpan secara rolling ke Firestore (macro_candles/latest_m30). Selesai!');
}

main().catch(error => {
  console.error('❌ Fatal Error pada skrip utama:', error);
  process.exit(1);
});
