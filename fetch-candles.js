/**
 * HybriOx-AI - Professional Data Ingestion Pipeline
 * Script: fetch-candles.js
 * Description: Rolling queue data fetcher with multi-provider fallback (Estafet System)
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

// 2. Konfigurasi Utama
const TARGET_PAIRS = ['XAUUSD', 'BTCUSD', 'HSI', 'GBPJPY', 'EURUSD', 'USDJPY', 'AUDUSD'];
const CANDLE_COUNT = 400;
const TIMEFRAME = '30min';

// Daftar Provider berjenjang (Estafet)
const PROVIDERS = [
  { name: 'Twelve Data', keyEnv: 'TWELVE_DATA_API_KEY' },
  { name: 'Alpha Vantage', keyEnv: 'ALPHA_VANTAGE_API_KEY' },
  { name: 'Finnhub', keyEnv: 'FINNHUB_API_KEY' }
];

/**
 * Fungsi Fetcher untuk Twelve Data
 */
async function fetchFromTwelveData(symbol, apiKey) {
  const url = `https://api.twelvedata.com/time_series?symbol=${symbol}&interval=${TIMEFRAME}&outputsize=${CANDLE_COUNT}&apikey=${apiKey}`;
  const response = await fetch(url);
  const data = await response.json();

  if (data.status === 'error' || !data.values) {
    throw new Error(data.message || 'Twelve Data limit or error');
  }

  // Format data standar HybriOx
  return data.values.map(item => ({
    time: item.datetime,
    open: parseFloat(item.open),
    high: parseFloat(item.high),
    low: parseFloat(item.low),
    close: parseFloat(item.close),
    volume: parseFloat(item.volume || 0)
  })).reverse(); // Urutkan dari terlama ke terbaru
}

/**
 * Fungsi Fetcher untuk Alpha Vantage
 */
async function fetchFromAlphaVantage(symbol, apiKey) {
  // Mapping simbol khusus jika diperlukan
  const avSymbol = symbol === 'XAUUSD' ? 'GOLD' : symbol; 
  const url = `https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=${avSymbol}&interval=30min&outputsize=full&apikey=${apiKey}`;
  
  const response = await fetch(url);
  const data = await response.json();
  
  const timeSeriesKey = 'Time Series (30min)';
  if (!data[timeSeriesKey]) {
    throw new Error(data['Note'] || 'Alpha Vantage limit or invalid response');
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
 * Fungsi Fetcher untuk Finnhub
 */
async function fetchFromFinnhub(symbol, apiKey) {
  // Finnhub menggunakan resolusi dalam satuan menit ('30')
  const now = Math.floor(Date.now() / 1000);
  const from = now - (CANDLE_COUNT * 30 * 60); // Estimasi rentang waktu ke belakang
  const url = `https://finnhub.io/api/v1/crypto/candle?symbol=BINANCE:${symbol}&resolution=30&from=${from}&to=${now}&token=${apiKey}`;
  
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
    volume: data.v[index]
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
          candles: candles.slice(-CANDLE_COUNT) // Pastikan pas maksimal 400
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
  console.log('🚀 Memulai siklus pengambilan data HybriOx-AI M30...');
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

  // Simpan secara rolling ke Firestore pada dokumen tunggal 'macro_candles/latest_m30'
  const docRef = db.collection('macro_candles').doc('latest_m30');
  await docRef.set(payloadData, { merge: true });

  console.log('✨ Data berhasil disimpan secara rolling ke Firestore (macro_candles/latest_m30). Selesai!');
}

main().catch(error => {
  console.error('❌ Fatal Error pada skrip utama:', error);
  process.exit(1);
});
