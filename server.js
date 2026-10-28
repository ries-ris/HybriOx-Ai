/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Full Multi-Provider Support (Twelve Data, Finnhub, Alpha Vantage / Fallbacks)
 * - Strict Master Registry Whitelist (3 Authorized Instruments: XAUUSD, GBPJPY, EURUSD)
 * - XAUUSDInstitutionalEngine (Top-Down M30 -> M15 -> M5 & TP-First / 180° Mirror SL)
 * - Autonomous Forex Factory News Guard (High-Impact USD Filter)
 * - Tier Distribution Pipeline & WebSocket Integration
 * - ES Module Syntax (Compatible with package.json "type": "module")
 */

import express from 'express';
import cors from 'cors';
import axios from 'axios';
import http from 'http';
import { WebSocketServer } from 'ws';
import admin from 'firebase-admin';

// Inisialisasi Firebase Admin untuk Backend
if (!admin.apps.length) {
    try {
        if (process.env.FIREBASE_SERVICE_ACCOUNT) {
            const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
            admin.initializeApp({
                credential: admin.credential.cert(serviceAccount)
            });
        } else {
            admin.initializeApp();
        }
    } catch (e) {
        console.log("Firebase Admin init notice:", e.message);
    }
}
const db = admin.apps.length ? admin.firestore() : null;

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(cors());
app.use(express.json());

// 1. MASTER REGISTRY WHITELIST (3 Authorized Instruments)
const MASTER_WHITELIST = [
    "XAUUSD", "GBPJPY", "EURUSD"
];

// 2. RUNTIME CONFIGURATION FOR API KEYS & PROVIDERS
let runtimeConfig = {
    twelveApiKey: process.env.TWELVE_DATA_API_KEY || '',
    finnhubKey: process.env.FINNHUB_KEY || '',
    alphaVantageKey: process.env.ALPHA_VANTAGE_KEY || '',
    geminiApiKey: process.env.GEMINI_API_KEY || '',
    tier: 'free'
};

// State Otonom Backend (Session, Emergency Stop, Scheduler, News Guard Cache)
let BackendState = {
    isRunning: true,
    econCalendarFilterActive: true,
    activeSessions: {
        asia: { start: 5, end: 14 },     // 05:00 - 14:00
        london: { start: 14, end: 22 },  // 14:00 - 22:00
        newYork: { start: 20, end: 4 }   // 20:00 - 04:00
    },
    cachedNewsEvents: [],
    lastNewsFetchTime: 0
};

// Penyimpanan State Cooldown di Sisi Server (Mencegah Spam Sinyal WebSocket)
const backendLastSignalRecord = {};

// Fungsi memuat ulang konfigurasi Global API Keys dari Firebase saat server menyala
async function loadGlobalConfigFromFirebase() {
    try {
        if (db) {
            let docSnap = await db.collection('settings').doc('global_config').get();
            if (docSnap.exists) {
                let data = docSnap.data();
                runtimeConfig.twelveApiKey = data.twelveApiKey !== undefined ? data.twelveApiKey : (process.env.TWELVE_DATA_API_KEY || '');
                runtimeConfig.finnhubKey = data.finnhubKey !== undefined ? data.finnhubKey : (process.env.FINNHUB_KEY || '');
                runtimeConfig.alphaVantageKey = data.alphaVantageKey !== undefined ? data.alphaVantageKey : (process.env.ALPHA_VANTAGE_KEY || '');
                runtimeConfig.geminiApiKey = data.geminiApiKey !== undefined ? data.geminiApiKey : (process.env.GEMINI_API_KEY || '');
                runtimeConfig.tier = data.tier || 'free';
                console.log('[Server Startup] Global API Keys berhasil disinkronkan dari Firebase Firestore.');
            }
        }
    } catch (error) {
        console.log('[Server Startup] Menggunakan konfigurasi default Environment Variables.');
    }
}

loadGlobalConfigFromFirebase();

const PROVIDERS = {
    twelveData: { baseUrl: "https://api.twelvedata.com" },
    binance: { baseUrl: "https://api.binance.com/api/v3" },
    alphaVantage: { baseUrl: "https://www.alphavantage.co/query" },
    finnhub: { baseUrl: "https://finnhub.io/api/v1" }
};

const marketCache = new Map();
const CACHE_TTL = 15000;

function formatSymbolForProvider(cleanSymbol, provider) {
    if (provider === 'twelveData') {
        if (cleanSymbol.length === 6 && !cleanSymbol.includes('/') && !['BTCUSD', 'ETHUSD'].includes(cleanSymbol)) {
            return `${cleanSymbol.slice(0, 3)}/${cleanSymbol.slice(3)}`;
        }
    } else if (provider === 'binance') {
        return cleanSymbol.replace('USD', 'USDT');
    }
    return cleanSymbol;
}

// ============================================================================
// MODUL PENGAMAN: FOREX FACTORY PUBLIC ECONOMIC CALENDAR (FREE & NO API KEY)
// ============================================================================
async function fetchAndCacheEconomicCalendar() {
    let now = Date.now();
    if (now - BackendState.lastNewsFetchTime < 14400000 && BackendState.cachedNewsEvents.length > 0) {
        return BackendState.cachedNewsEvents;
    }

    try {
        const response = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.json', { timeout: 10000 });
        if (Array.isArray(response.data)) {
            BackendState.cachedNewsEvents = response.data;
            BackendState.lastNewsFetchTime = now;
            console.log(`[News Guard] Berhasil menyinkronkan ${response.data.length} jadwal berita ekonomi global.`);
        }
    } catch (e) {
        console.log('[News Guard Warning] Gagal menarik data kalender ekonomi publik:', e.message);
    }
    return BackendState.cachedNewsEvents;
}

async function isHighImpactNewsBlackout() {
    if (!BackendState.econCalendarFilterActive) return false;

    let events = await fetchAndCacheEconomicCalendar();
    if (!events || events.length === 0) return false;

    let currentTime = Date.now();
    let blackoutWindowMs = 30 * 60 * 1000; // 30 Menit sebelum & sesudah berita

    for (let ev of events) {
        if (ev.country === 'USD' && ev.impact && ev.impact.toLowerCase() === 'high') {
            let eventTime = new Date(ev.date).getTime();
            if (!isNaN(eventTime)) {
                let timeDiff = eventTime - currentTime;
                if (timeDiff >= -blackoutWindowMs && timeDiff <= blackoutWindowMs) {
                    console.log(`[News Blackout Active] Berita High-Impact USD terdeteksi: "${ev.title}" pada pukul ${ev.date}. Sinyal ditahan.`);
                    return true;
                }
            }
        }
    }
    return false;
}

// ============================================================================
// HELPER: Mengambil Cache Makro M30 (400 Candle) dari Firestore 
// (Disesuaikan selaras dengan struktur penyimpanan fetch-candles.js)
// ============================================================================
async function getMacroCache400(pairSymbol) {
    try {
        if (db) {
            // Mengambil dari dokumen terpusat 'latest_m30' pada koleksi 'macro_candles'
            let docSnap = await db.collection('macro_candles').doc('latest_m30').get();
            if (docSnap.exists) {
                let data = docSnap.data();
                let cleanSym = (pairSymbol || "").toUpperCase();
                
                // Mengambil array candle dari objek candles berdasarkan key pair (XAUUSD, GBPJPY, EURUSD)
                if (data.candles && Array.isArray(data.candles[cleanSym]) && data.candles[cleanSym].length > 0) {
                    return data.candles[cleanSym].slice(-400);
                }
            }
        }
    } catch (e) {
        console.log(`[Macro Cache Warning] Gagal memuat cache Firestore untuk ${pairSymbol}:`, e.message);
    }
    return [];
}

// ============================================================================
// 3. MESIN INSTITUSIONAL: XAUUSDInstitutionalEngine (TOP-DOWN M30 -> M15 -> M5)
// ============================================================================
async function runInstitutionalEnginePipeline(pairSymbol, timeframe) {
    if (!BackendState.isRunning) {
        console.log('[Engine] Sistem dalam status Emergency Stop (Kill Switch Aktif).');
        return null;
    }

    console.log(`[Engine WSM-200] Menjalankan analisis Hierarki M30 -> M15 -> M5 untuk ${pairSymbol}...`);

    try {
        let activeKey = runtimeConfig.twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo";
        let formattedSym = formatSymbolForProvider(pairSymbol, 'twelveData');

        // Ambil data taktis M15 (200 candle untuk SNR) & M5 (30 candle untuk eksekusi presisi) secara pararel
        let urlM15 = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${formattedSym}&interval=15min&outputsize=200&apikey=${activeKey}`;
        let urlM5 = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${formattedSym}&interval=5min&outputsize=30&apikey=${activeKey}`;

        let [resM15, resM5] = await Promise.all([axios.get(urlM15), axios.get(urlM5)]);
        
        let m15Candles = [];
        if (resM15.data && resM15.data.values && Array.isArray(resM15.data.values)) {
            m15Candles = resM15.data.values.reverse().map(item => [
                new Date(item.datetime).getTime(),
                parseFloat(item.open), parseFloat(item.high), parseFloat(item.low), parseFloat(item.close), parseFloat(item.volume || 100)
            ]);
        }

        let m5Candles = [];
        if (resM5.data && resM5.data.values && Array.isArray(resM5.data.values)) {
            m5Candles = resM5.data.values.reverse().map(item => [
                new Date(item.datetime).getTime(),
                parseFloat(item.open), parseFloat(item.high), parseFloat(item.low), parseFloat(item.close), parseFloat(item.volume || 100)
            ]);
        }

        // Ambil 400 candle M30 Makro langsung dari Local Storage Firebase
        let m30MacroCandles = await getMacroCache400(pairSymbol);

        if (m15Candles.length < 50 || m30MacroCandles.length < 50 || m5Candles.length < 10) {
            return { status: 'NO_SIGNAL', reason: 'Data multi-timeframe belum lengkap dari provider / Firebase.' };
        }

        // Masukkan ke Mesin Evaluasi Hierarki Utama (Top-Down + TP-First + 180° Mirror SL)
        let signalResult = evaluateWSM200InstitutionalStrategy(m30MacroCandles, m15Candles, m5Candles, pairSymbol);

        if (signalResult && signalResult.status === 'TRIGGERED') {
            let currentCandleTime = m15Candles[m15Candles.length - 1][0];
            let currentPrice = m15Candles[m15Candles.length - 1][4];
            let stratContextKey = `${pairSymbol}_wsm200_sniper_m5`;
            let lastStratRecord = backendLastSignalRecord[stratContextKey];

            if (lastStratRecord && currentCandleTime <= lastStratRecord.candleTime) {
                return { status: 'NO_SIGNAL', reason: 'Sinyal pada candle ini sudah pernah dieksekusi.' };
            }

            backendLastSignalRecord[stratContextKey] = {
                candleTime: currentCandleTime,
                price: currentPrice
            };
            return signalResult;
        }

        return signalResult;

    } catch (e) {
        console.error('[Engine Pipeline Error]:', e.message);
        return null;
    }
}

// ============================================================================
// MESIN WSM-200: M30 BIAS -> M15 SNR ANCHOR -> M5 ENTRY & 180° MIRROR SL
// ============================================================================
function evaluateWSM200InstitutionalStrategy(candlesM30, candlesM15, candlesM5, pairSymbol) {
    let cleanSym = (pairSymbol || "").toUpperCase();
    let isGold = cleanSym.includes("XAU") || cleanSym.includes("GOLD");
    if (!isGold) return { status: 'NO_SIGNAL', reason: 'Modul khusus instrumen XAUUSD.' };

    let currentPrice = candlesM5[candlesM5.length - 1][4];

    // Lapis 1: M30 Macro Bias & Dealing Range (Discount vs Premium)
    let m30Sample = candlesM30.slice(-400);
    let m30High = Math.max(...m30Sample.map(c => c[2]));
    let m30Low = Math.min(...m30Sample.map(c => c[3]));
    let m30Equilibrium = (m30High + m30Low) / 2;

    let macroBias = currentPrice < m30Equilibrium ? 'DISCOUNT_BUY' : 'PREMIUM_SELL';

    // Lapis 2: M15 Structural Anchor (SNR & Dealing Range)
    let m15Sample = candlesM15.slice(-200);
    let m15Supports = [];
    let m15Resistances = [];

    for (let i = 2; i < m15Sample.length - 2; i++) {
        let h = m15Sample[i][2];
        let l = m15Sample[i][3];
        if (h > m15Sample[i-1][2] && h > m15Sample[i-2][2] && h > m15Sample[i+1][2] && h > m15Sample[i+2][2]) {
            m15Resistances.push(h);
        }
        if (l < m15Sample[i-1][3] && l < m15Sample[i-2][3] && l < m15Sample[i+1][3] && l < m15Sample[i+2][3]) {
            m15Supports.push(l);
        }
    }

    // Lapis 3: M5 Precision Execution & Zone Validation
    let actionType = null;
    let targetSupport = null;
    let targetResistance = null;
    let zoneThreshold = 4.0;

    if (macroBias === 'DISCOUNT_BUY') {
        let validSupports = m15Supports.filter(s => s < currentPrice);
        if (validSupports.length > 0) {
            targetSupport = Math.max(...validSupports);
            let m5RecentLows = Math.min(...candlesM5.slice(-6).map(c => c[3]));
            if (Math.abs(m5RecentLows - targetSupport) <= zoneThreshold || (currentPrice >= targetSupport && currentPrice <= targetSupport + (zoneThreshold * 1.5))) {
                actionType = 'BUY_LIMIT';
            }
        }
    } else {
        let validResistances = m15Resistances.filter(r => r > currentPrice);
        if (validResistances.length > 0) {
            targetResistance = Math.min(...validResistances);
            let m5RecentHighs = Math.max(...candlesM5.slice(-6).map(c => c[2]));
            if (Math.abs(m5RecentHighs - targetResistance) <= zoneThreshold || (currentPrice <= targetResistance && currentPrice >= targetResistance - (zoneThreshold * 1.5))) {
                actionType = 'SELL_LIMIT';
            }
        }
    }

    if (!actionType) {
        return { status: 'NO_SIGNAL', reason: 'Menunggu harga masuk zona struktur SNR M15 terkonfirmasi M5.' };
    }

    // Kalkulasi TP First & 180° Mirror SL (Anti-Stop-Hunt)
    let entryPrice = currentPrice;
    let minNetPipDistance = 1.8; // 18 pips x 0.1 = 1.8 USD bersih
    let spreadAndBuffer = 0.5;   
    let sl = 0;
    let tp1 = 0, tp2 = null, tp3 = null;
    let tpCount = 1;

    if (actionType === 'BUY_LIMIT') {
        let upperResistances = m15Resistances.filter(r => r > entryPrice).sort((a, b) => a - b);
        let rawTp1 = entryPrice + minNetPipDistance + spreadAndBuffer;

        if (upperResistances.length > 0) {
            tp1 = Number(Math.max(rawTp1, upperResistances[0] * 0.98).toFixed(2));
            if (upperResistances.length > 1) { tp2 = Number(upperResistances[1].toFixed(2)); tpCount = 2; }
            if (upperResistances.length > 2) { tp3 = Number(upperResistances[2].toFixed(2)); tpCount = 3; }
        } else {
            tp1 = Number(rawTp1.toFixed(2));
        }

        let netPipCheck = Math.abs(tp1 - entryPrice);
        if (netPipCheck < minNetPipDistance) {
            return { status: 'NO_SIGNAL', reason: 'Ditolak: Jarak TP1 kurang dari minimum 18 pip bersih.' };
        }

        // Proyeksi 180 Derajat ke Bawah untuk SL (Anti-Stop-Hunt)
        let upwardSpan = Math.abs(tp1 - entryPrice);
        sl = Number((entryPrice - upwardSpan).toFixed(2));

    } else {
        let lowerSupports = m15Supports.filter(s => s < entryPrice).sort((a, b) => b - a);
        let rawTp1 = entryPrice - minNetPipDistance - spreadAndBuffer;

        if (lowerSupports.length > 0) {
            tp1 = Number(Math.min(rawTp1, lowerSupports[0] * 1.02).toFixed(2));
            if (lowerSupports.length > 1) { tp2 = Number(lowerSupports[1].toFixed(2)); tpCount = 2; }
            if (lowerSupports.length > 2) { tp3 = Number(lowerSupports[2].toFixed(2)); tpCount = 3; }
        } else {
            tp1 = Number(rawTp1.toFixed(2));
        }

        let netPipCheck = Math.abs(entryPrice - tp1);
        if (netPipCheck < minNetPipDistance) {
            return { status: 'NO_SIGNAL', reason: 'Ditolak: Jarak TP1 kurang dari minimum 18 pip bersih.' };
        }

        // Proyeksi 180 Derajat ke Atas untuk SL (Anti-Stop-Hunt)
        let downwardSpan = Math.abs(entryPrice - tp1);
        sl = Number((entryPrice + downwardSpan).toFixed(2));
    }

    return {
        signalId: 'WSM200_GOLD_' + Date.now(),
        pair: pairSymbol,
        timeframe: 'M30->M15->M5',
        status: 'TRIGGERED',
        strategyName: 'WSM-200 Gold Sniper (Top-Down M30/M15/M5 + 180° Mirror SL)',
        actionType: actionType,
        price: Number(entryPrice.toFixed(2)),
        entryZone: Number(entryPrice.toFixed(2)),
        sl: sl,
        targets: [tp1, tp2, tp3].filter(Boolean),
        tpCount: tpCount,
        tp1: tp1,
        tp2: tp2,
        tp3: tp3,
        bepTrigger: tp1,
        bepActive: false,
        timestamp: Date.now()
    };
}

// ============================================================================
// 4. REST API ENDPOINTS
// ============================================================================
app.get('/api/macro/latest', async (req, res) => {
    try {
        if (!db) return res.status(500).json({ success: false, message: 'Firestore belum diinisialisasi.' });
        let docSnap = await db.collection('macro_candles').doc('latest_m30').get();
        if (!docSnap.exists) return res.status(404).json({ success: false, message: 'Data M30 belum tersedia.' });
        return res.json(docSnap.data());
    } catch (error) {
        return res.status(500).json({ success: false, error: error.message });
    }
});

app.post('/api/verify-dev', (req, res) => {
    const { password } = req.body;
    const devPassword = process.env.DEV_PASSWORD;
    if (!devPassword) return res.status(500).json({ success: false, message: "DEV_PASSWORD belum diatur di Render." });
    if (password && password.trim() === devPassword) {
        return res.json({ success: true, message: "Akses diverifikasi." });
    }
    return res.status(401).json({ success: false, message: "Sandi pengembang salah." });
});

app.post('/api/developer/config', async (req, res) => {
    try {
        const { twelveApiKey, finnhubKey, alphaVantageKey, geminiApiKey, tier } = req.body;
        let firestoreUpdate = { updatedAt: new Date().toISOString() };

        if (twelveApiKey !== undefined && twelveApiKey.trim() !== '') {
            runtimeConfig.twelveApiKey = twelveApiKey.trim();
            firestoreUpdate.twelveApiKey = twelveApiKey.trim();
        }
        if (finnhubKey !== undefined && finnhubKey.trim() !== '') {
            runtimeConfig.finnhubKey = finnhubKey.trim();
            firestoreUpdate.finnhubKey = finnhubKey.trim();
        }
        if (alphaVantageKey !== undefined && alphaVantageKey.trim() !== '') {
            runtimeConfig.alphaVantageKey = alphaVantageKey.trim();
            firestoreUpdate.alphaVantageKey = alphaVantageKey.trim();
        }
        if (geminiApiKey !== undefined && geminiApiKey.trim() !== '') {
            runtimeConfig.geminiApiKey = geminiApiKey.trim();
            firestoreUpdate.geminiApiKey = geminiApiKey.trim();
        }
        if (tier !== undefined) {
            runtimeConfig.tier = tier;
            firestoreUpdate.tier = tier;
        }

        if (db) {
            await db.collection('settings').doc('global_config').set(firestoreUpdate, { merge: true });
        }

        return res.json({ success: true, message: "Konfigurasi diperbarui dengan aman!" });
    } catch (error) {
        return res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/symbols/search', (req, res) => {
    const q = (req.query.q || '').trim().toUpperCase();
    if (!q || q.length < 2) return res.json({ success: true, symbols: [] });
    const filtered = MASTER_WHITELIST.filter(symbol => symbol.includes(q));
    res.json({ success: true, symbols: filtered });
});

app.post('/api/validate-symbol', (req, res) => {
    const { symbol } = req.body;
    const cleanSymbol = (symbol || "").trim().toUpperCase();
    if (MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.json({ valid: true, symbol: cleanSymbol });
    }
    return res.status(400).json({ valid: false, message: `Instrumen '${cleanSymbol}' ditolak whitelist.` });
});

app.get('/api/proxy/candles', async (req, res) => {
    try {
        const { provider = 'twelvedata', symbol, interval, finnhubSymbol, resolution, from, to, isForex, isDaily, base, quote } = req.query;
        const cleanSymbol = (symbol || "").trim().toUpperCase();

        if (provider === 'twelvedata') {
            const apiKey = runtimeConfig.twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo";
            const t12Interval = interval || '15min';
            const formattedSym = formatSymbolForProvider(cleanSymbol, 'twelveData');
            const targetUrl = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${encodeURIComponent(formattedSym)}&interval=${t12Interval}&outputsize=200&apikey=${apiKey}`;
            const response = await axios.get(targetUrl);
            return res.json(response.data);
        } else if (provider === 'finnhub') {
            const apiKey = runtimeConfig.finnhubKey || process.env.FINNHUB_KEY || "c8888888888888888888";
            const isFx = isForex === 'true';
            let targetUrl = isFx 
                ? `${PROVIDERS.finnhub.baseUrl}/forex/candle?symbol=OANDA:${finnhubSymbol || cleanSymbol}&resolution=${resolution || '15'}&from=${from}&to=${to}&token=${apiKey}`
                : `${PROVIDERS.finnhub.baseUrl}/stock/candle?symbol=${cleanSymbol}&resolution=${resolution || '15'}&from=${from}&to=${to}&token=${apiKey}`;
            const response = await axios.get(targetUrl);
            return res.json(response.data);
        } else if (provider === 'alphavantage') {
            const apiKey = runtimeConfig.alphaVantageKey || process.env.ALPHA_VANTAGE_KEY || "demo";
            const isFx = isForex === 'true';
            const daily = isDaily === 'true';
            let targetUrl = isFx
                ? (daily ? `${PROVIDERS.alphaVantage.baseUrl}?function=FX_DAILY&from_symbol=${base}&to_symbol=${quote}&outputsize=full&apikey=${apiKey}` : `${PROVIDERS.alphaVantage.baseUrl}?function=FX_INTRADAY&from_symbol=${base}&to_symbol=${quote}&interval=${interval || '15min'}&outputsize=full&apikey=${apiKey}`)
                : (daily ? `${PROVIDERS.alphaVantage.baseUrl}?function=TIME_SERIES_DAILY&symbol=${cleanSymbol}&outputsize=full&apikey=${apiKey}` : `${PROVIDERS.alphaVantage.baseUrl}?function=TIME_SERIES_INTRADAY&symbol=${cleanSymbol}&interval=${interval || '15min'}&outputsize=full&apikey=${apiKey}`);
            const response = await axios.get(targetUrl);
            return res.json(response.data);
        } else {
            return res.status(400).json({ success: false, message: "Provider proxy tidak dikenal" });
        }
    } catch (error) {
        return res.status(500).json({ success: false, isInvalidSymbol: false, message: error.message });
    }
});

app.get('/api/v1/market', async (req, res) => {
    const { symbol, interval = '1h', provider = 'twelveData' } = req.query;
    const cleanSymbol = (symbol || "").trim().toUpperCase();

    if (!MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.status(403).json({ error: "Access Denied", message: "Simbol tidak ada di Katalog Master Whitelist." });
    }

    const cacheKey = `${provider}_${cleanSymbol}_${interval}`;
    const cachedData = marketCache.get(cacheKey);
    if (cachedData && (Date.now() - cachedData.timestamp < CACHE_TTL)) {
        return res.json({ source: 'cache', data: cachedData.data });
    }

    try {
        let responseData;
        const formattedSymbol = formatSymbolForProvider(cleanSymbol, provider);

        if (provider === 'twelveData') {
            const activeKey = runtimeConfig.twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo";
            const url = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${formattedSymbol}&interval=${interval}&apikey=${activeKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } else if (provider === 'binance') {
            const url = `${PROVIDERS.binance.baseUrl}/klines?symbol=${formattedSymbol}&interval=${interval}`;
            const response = await axios.get(url);
            responseData = response.data;
        } else if (provider === 'alphaVantage') {
            const activeKey = runtimeConfig.alphaVantageKey || process.env.ALPHA_VANTAGE_KEY || "demo";
            const isDaily = ['240', 'D', 'W', 'M'].includes(interval);
            const functionType = isDaily ? 'TIME_SERIES_DAILY' : 'TIME_SERISS_INTRADAY';
            const intervalParam = isDaily ? '' : `&interval=${interval}`;
            const url = `${PROVIDERS.alphaVantage.baseUrl}?function=${functionType}&symbol=${formattedSymbol}${intervalParam}&apikey=${activeKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } else {
            throw new Error("Provider tidak dikenal.");
        }

        marketCache.set(cacheKey, { timestamp: Date.now(), data: responseData });
        return res.json({ source: provider, data: responseData });
    } catch (error) {
        return res.status(500).json({ error: "Provider Error", details: error.message });
    }
});

app.post('/api/sync', (req, res) => {
    const tradeRecord = req.body;
    return res.json({ success: true, message: 'Disinkronkan ke server.', data: tradeRecord });
});

// ============================================================================
// 5. JALUR DISTRIBUSI SINYAL BERBASIS TIER (PIPELINE)
// ============================================================================
function routeSignalThroughTierPipeline(signalData, clientTier) {
    if (clientTier === 'free' || !clientTier) {
        return { ...signalData, tierTarget: 'STANDARD', allowed: true, note: 'Dialirkan ke jalur Standar Tier' };
    } 
    else if (clientTier === 'premium' || clientTier === 'vip') {
        return { ...signalData, tierTarget: 'PREMIUM', allowed: true, note: 'Dialirkan ke jalur Premium Tier (Full Access)' };
    }
    return null;
}

// ============================================================================
// 6. WEBSOCKET SERVER
// ============================================================================
wss.on('connection', (ws) => {
    console.log('[WebSocket Backend] Klien Panel Developer terhubung secara aktif.');
    ws.tier = 'free';

    ws.on('message', async (message) => {
        try {
            const parsed = JSON.parse(message);
            console.log(`[WebSocket Backend] Menerima aksi kendali: ${parsed.action || parsed.type}`);

            if (parsed.tier) {
                ws.tier = parsed.tier;
            }

            if (parsed.action === 'FORCE_TRIGGER_SCAN' || parsed.type === 'FORCE_TRIGGER_SCAN') {
                let signal = await runInstitutionalEnginePipeline("XAUUSD", "15");
                if (signal && signal.status === 'TRIGGERED') {
                    let routedSignal = routeSignalThroughTierPipeline(signal, ws.tier);
                    if (routedSignal && routedSignal.allowed) {
                        ws.send(JSON.stringify({ type: 'DECENTRALIZED_SIGNAL_BROADCAST', signal: routedSignal }));
                    }
                }
            } 
            else if (parsed.action === 'EMERGENCY_STOP' || parsed.type === 'EMERGENCY_STOP') {
                BackendState.isRunning = false;
                ws.send(JSON.stringify({ type: 'SYSTEM_STATUS', message: 'Emergency Stop berhasil diterapkan di server.' }));
            } 
            else {
                ws.send(JSON.stringify({ status: 'ack', payload: parsed }));
            }
        } catch (e) {
            ws.send(JSON.stringify({ error: 'Format pesan WebSocket tidak valid.' }));
        }
    });

    ws.on('close', () => {
        console.log('[WebSocket Backend] Klien Panel Developer terputus.');
    });
});

// ============================================================================
// 7. AUTONOMOUS BACKGROUND SCHEDULER & WORKER (INTERVAL 3.75 MENIT / 225000MS)
// ============================================================================
function isWeekend() {
    const now = new Date();
    const day = now.getDay();
    return (day === 0 || day === 6);
}

function isWithinSessionRange() {
    const now = new Date();
    const currentHour = now.getHours();
    
    for (let sessionName in BackendState.activeSessions) {
        let sess = BackendState.activeSessions[sessionName];
        if (sess.start < sess.end) {
            if (currentHour >= sess.start && currentHour < sess.end) return true;
        } else {
            if (currentHour >= sess.start || currentHour < sess.end) return true;
        }
    }
    return false;
}

const AUTONOMOUS_INTERVAL_MS = 225000; // 3.75 Menit

setInterval(async () => {
    if (!BackendState.isRunning) return;
    if (isWeekend()) return;
    if (!isWithinSessionRange()) return;

    try {
        let inNewsBlackout = await isHighImpactNewsBlackout();
        if (inNewsBlackout) {
            console.log('[News Guard] Sinyal XAUUSD ditahan sementara karena ada rilis High-Impact News USD.');
            return;
        }

        let targetSymbol = "XAUUSD";
        let signalResult = await runInstitutionalEnginePipeline(targetSymbol, "15");

        if (signalResult && signalResult.status === 'TRIGGERED') {
            console.log(`[Autonomous Engine] Sinyal WSM-200 & 180° Mirror SL Valid! Menyiarkan sinyal ${targetSymbol} via WebSocket.`);
            
            wss.clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) {
                    let userTier = client.tier || 'free';
                    let routedSignal = routeSignalThroughTierPipeline(signalResult, userTier);

                    if (routedSignal && routedSignal.allowed) {
                        client.send(JSON.stringify({
                            type: 'DECENTRALIZED_SIGNAL_BROADCAST',
                            signal: routedSignal
                        }));
                    }
                }
            });
        }
    } catch (error) {
        console.error('[Autonomous Worker Error]:', error.message);
    }
}, AUTONOMOUS_INTERVAL_MS);

// ============================================================================
// SERVER LISTEN
// ============================================================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`HybriOX Autonomous Backend & Proxy aktif dan berjalan pada port ${PORT}`);
});
