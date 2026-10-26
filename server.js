/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Full Multi-Provider Support (Twelve Data, Finnhub, Alpha Vantage / Fallbacks)
 * - Strict Master Registry Whitelist (25 Authorized Instruments)
 * - XAUUSDInstitutionalEngine (ISLMS-200) & Session-Aware Autonomous Scheduler
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

// 1. MASTER REGISTRY WHITELIST (25 Authorized Instruments)
const MASTER_WHITELIST = [
    // Forex Majors & Crosses
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD",
    "EURGBP", "EURJPY", "GBPJPY", "AUDJPY", "EURAUD",
    // Metals & Commodities
    "XAUUSD", "XAGUSD", "WTI", "BRENT", "USOIL", "UKOIL",
    // Indices (Including HSI)
    "US30", "NAS100", "SPX500", "DAX", "HSI",
    // Cryptocurrencies
    "BTCUSD", "ETHUSD"
];

// 2. RUNTIME CONFIGURATION FOR API KEYS & PROVIDERS
let runtimeConfig = {
    twelveApiKey: process.env.TWELVE_DATA_API_KEY || '',
    finnhubKey: process.env.FINNHUB_KEY || '',
    alphaVantageKey: process.env.ALPHA_VANTAGE_KEY || '',
    geminiApiKey: process.env.GEMINI_API_KEY || '',
    tier: 'free'
};

// State Otonom Backend (Session, Emergency Stop, Scheduler)
let BackendState = {
    isRunning: true,
    econCalendarFilterActive: true,
    activeSessions: {
        asia: { start: 5, end: 14 },     // 05:00 - 14:00
        london: { start: 14, end: 22 },  // 14:00 - 22:00
        newYork: { start: 20, end: 4 }   // 20:00 - 04:00
    }
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

// Helper untuk Mengambil Cache Makro M30 dari Firestore atau Memory Cache Lokal
async function getMacroCache400(pairSymbol) {
    try {
        if (db) {
            let docSnap = await db.collection('macro_cache').doc(pairSymbol.toUpperCase()).get();
            if (docSnap.exists) {
                let data = docSnap.data();
                if (Array.isArray(data.candles) && data.candles.length > 0) {
                    return data.candles.slice(-400);
                }
            }
        }
    } catch (e) {
        console.log(`[Macro Cache Warning] Gagal memuat cache Firestore untuk ${pairSymbol}:`, e.message);
    }
    return [];
}

// ============================================================================
// 3. MESIN INSTITUSIONAL: XAUUSDInstitutionalEngine (ISLMS-200) & PIPELINE
// ============================================================================
async function runInstitutionalEnginePipeline(pairSymbol, timeframe) {
    if (!BackendState.isRunning) {
        console.log('[Engine] Sistem dalam status Emergency Stop (Kill Switch Aktif).');
        return null;
    }

    console.log(`[Engine ISLMS-200] Menjalankan analisis Smart Money Concepts (200 M15 + 400 M30) untuk ${pairSymbol}...`);

    try {
        let activeKey = runtimeConfig.twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo";
        let formattedSym = formatSymbolForProvider(pairSymbol, 'twelveData');

        // 1. Tarik 200 candle M15 untuk data taktis
        let urlM15 = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${formattedSym}&interval=15min&outputsize=200&apikey=${activeKey}`;
        let resM15 = await axios.get(urlM15);
        
        let m15Candles = [];
        if (resM15.data && resM15.data.values && Array.isArray(resM15.data.values)) {
            m15Candles = resM15.data.values.reverse().map(item => [
                new Date(item.datetime).getTime(),
                parseFloat(item.open), parseFloat(item.high), parseFloat(item.low), parseFloat(item.close), parseFloat(item.volume || 100)
            ]);
        }

        // 2. Ambil 400 candle M30 makro secara serentak
        let m30MacroCandles = await getMacroCache400(pairSymbol);

        if (m15Candles.length < 50) {
            // Fallback darurat jika API terbatas
            m15Candles = [
                [Date.now() - 900000, 2650.0, 2655.0, 2648.0, 2652.5, 1000],
                [Date.now(), 2652.5, 2658.0, 2651.0, 2657.0, 1200]
            ];
        }

        // 3. Masukkan ke Mesin ISLMS-200 dengan struktur SNR Ganda
        let signalResult = evaluateSmartMoneyConcepts(m15Candles, m30MacroCandles, pairSymbol, timeframe);

        if (signalResult && signalResult.status === 'TRIGGERED') {
            let currentCandleTime = m15Candles[m15Candles.length - 1][0];
            let currentPrice = m15Candles[m15Candles.length - 1][4];
            let stratContextKey = `${pairSymbol}_${timeframe}_strat_islms_200`;
            let lastStratRecord = backendLastSignalRecord[stratContextKey];

            let trList = [];
            for (let i = 1; i < m15Candles.length; i++) {
                let high = m15Candles[i][2], low = m15Candles[i][3], prevClose = m15Candles[i - 1][4];
                trList.push(Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose)));
            }
            let currentAtr = trList.slice(-14).reduce((a, b) => a + b, 0) / 14 || (currentPrice * 0.001);

            if (lastStratRecord && currentCandleTime <= lastStratRecord.candleTime) {
                let baselineAtr = lastStratRecord.snapshotAtr > 0 ? lastStratRecord.snapshotAtr : (currentPrice * 0.001);
                let isReleased = Math.abs(currentPrice - lastStratRecord.price) >= (baselineAtr * 0.5);
                
                if (!isReleased) {
                    return { status: 'NO_SIGNAL' };
                }
            }

            backendLastSignalRecord[stratContextKey] = {
                candleTime: currentCandleTime,
                price: currentPrice,
                snapshotAtr: currentAtr
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
// MESIN INSTITUSIONAL ISLMS-200 & GATE 4 RISK MANAGEMENT (SNR + ATURAN 18 PIP)
// ============================================================================
function evaluateSmartMoneyConcepts(m15Candles, m30MacroCandles, pair, tf) {
    if (!m15Candles || m15Candles.length < 50) return { status: 'NO_SIGNAL' };

    let currentPrice = m15Candles[m15Candles.length - 1][4];

    // Gabungkan data makro dan taktis untuk pemetaan SNR horizontal yang mendalam
    let combinedCandles = m30MacroCandles.length >= 50 ? [...m30MacroCandles, ...m15Candles] : m15Candles;

    let sampleRange = combinedCandles.slice(-200);
    let rangeHigh = Math.max(...sampleRange.map(c => c[2]));
    let rangeLow = Math.min(...sampleRange.map(c => c[3]));
    let equilibrium = (rangeHigh + rangeLow) / 2;

    // Deteksi FVG (Fair Value Gap)
    let fvgs = [];
    for (let i = 2; i < m15Candles.length; i++) {
        let prevCandle = m15Candles[i - 2];
        let currCandle = m15Candles[i];

        if (currCandle[3] > prevCandle[2]) {
            fvgs.push({ type: 'BULLISH_FVG', zoneLow: prevCandle[2], zoneHigh: currCandle[3] });
        }
        if (currCandle[2] < prevCandle[3]) {
            fvgs.push({ type: 'BEARISH_FVG', zoneLow: currCandle[2], zoneHigh: currCandle[3] });
        }
    }

    let actionType = null;
    let strategyReason = "";

    if (fvgs.length > 0) {
        let latestFVG = fvgs[fvgs.length - 1];

        if (currentPrice < equilibrium && latestFVG.type === 'BULLISH_FVG') {
            if (currentPrice >= latestFVG.zoneLow && currentPrice <= latestFVG.zoneHigh * 1.005) {
                actionType = 'BUY_LIMIT';
                strategyReason = 'ISlMS-200: 200 M15 + 400 M30 Discount Zone + Bullish FVG';
            }
        }

        if (currentPrice > equilibrium && latestFVG.type === 'BEARISH_FVG') {
            if (currentPrice <= latestFVG.zoneHigh && currentPrice >= latestFVG.zoneLow * 0.995) {
                actionType = 'SELL_LIMIT';
                strategyReason = 'ISLMS-200: 200 M15 + 400 M30 Premium Zone + Bearish FVG';
            }
        }
    }

    if (!actionType) {
        let lastC = m15Candles[m15Candles.length - 1];
        let isBullishMomentum = lastC[4] > lastC[1];
        if (currentPrice < equilibrium && isBullishMomentum) {
            actionType = 'BUY_MARKET';
            strategyReason = 'ISLMS-200: Institutional Discount Momentum Fallback';
        } else if (currentPrice > equilibrium && !isBullishMomentum) {
            actionType = 'SELL_MARKET';
            strategyReason = 'ISLMS-200: Institutional Premium Momentum Fallback';
        } else {
            return { status: 'NO_SIGNAL' };
        }
    }

    let isBuy = actionType.includes('BUY');

    // ========================================================================
    // GATE 4: RISK MANAGEMENT & TPSL BERBASIS SNR MAYOR + ATURAN 18 PIP
    // ========================================================================
    let swingHighs = [];
    let swingLows = [];
    for (let i = 2; i < combinedCandles.length - 2; i++) {
        let h = combinedCandles[i][2];
        let l = combinedCandles[i][3];
        if (h > combinedCandles[i-1][2] && h > combinedCandles[i+1][2]) swingHighs.push(h);
        if (l < combinedCandles[i-1][3] && l < combinedCandles[i+1][3]) swingLows.push(l);
    }

    let trList = [];
    for (let i = 1; i < m15Candles.length; i++) {
        let high = m15Candles[i][2], low = m15Candles[i][3], prevClose = m15Candles[i - 1][4];
        trList.push(Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose)));
    }
    let atr = trList.slice(-14).reduce((a, b) => a + b, 0) / 14 || (currentPrice * 0.001);
    
    // Buffer pengaman agar lolos dari jilatan liquidity sweep
    let buffer = isBuy ? (atr * 0.35) : (atr * 0.35);
    let sl = 0;
    let targetPrices = [];

    if (isBuy) {
        let validSupports = swingLows.filter(l => l < currentPrice);
        let nearestSupport = validSupports.length > 0 ? Math.min(...validSupports) : (currentPrice - (atr * 1.5));
        sl = Number((nearestSupport - buffer).toFixed(4)); // SL di luar Support mayor + buffer

        let validResistances = swingHighs.filter(h => h > currentPrice).sort((a, b) => a - b);
        if (validResistances.length > 0) {
            targetPrices = validResistances.map(r => Number((r - buffer).toFixed(4)));
        } else {
            targetPrices = [Number((currentPrice + (atr * 2.0)).toFixed(4))];
        }
    } else {
        let validResistances = swingHighs.filter(h => h > currentPrice);
        let nearestResistance = validResistances.length > 0 ? Math.max(...validResistances) : (currentPrice + (atr * 1.5));
        sl = Number((nearestResistance + buffer).toFixed(4)); // SL di luar Resisten mayor + buffer

        let validSupports = swingLows.filter(l => l < currentPrice).sort((a, b) => b - a);
        if (validSupports.length > 0) {
            targetPrices = validSupports.map(s => Number((s + buffer).toFixed(4)));
        } else {
            targetPrices = [Number((currentPrice - (atr * 2.0)).toFixed(4))];
        }
    }

    // ATURAN KAKU: MINIMUM 18 PIP BERSIH (Khusus XAUUSD / Gold)
    let isGold = pair.toUpperCase().includes("XAU") || pair.toUpperCase().includes("GOLD");
    let minTpPipsRequired = isGold ? 18 : 10;
    let pipMultiplier = isGold ? 10 : 10000; // Skala pip Gold (0.1) vs Forex (0.0001)

    let finalTargets = [];
    for (let tpPrice of targetPrices) {
        let distancePips = Math.abs(tpPrice - currentPrice) * pipMultiplier;
        if (distancePips >= minTpPipsRequired) {
            finalTargets.push(tpPrice);
        }
    }

    // Jika jarak TP kurang dari 18 pip bersih, tolak sinyal agar tidak habis karena spread/noise
    if (finalTargets.length === 0) {
        return { status: 'NO_SIGNAL', reason: `Gate 4 Rejected: Potensi TP kurang dari batas minimum (${minTpPipsRequired} pip bersih).` };
    }

    let tp1 = finalTargets[0] || null;
    let tp2 = finalTargets[1] || null;
    let tp3 = finalTargets[2] || null;
    let tpCount = finalTargets.length;

    return {
        signalId: 'INST_ISLMS200_' + pair + '_TF' + tf + '_' + Date.now(),
        pair: pair,
        timeframe: tf,
        status: 'TRIGGERED',
        strategyName: strategyReason,
        actionType: actionType,
        price: Number(currentPrice.toFixed(4)),
        entryZone: Number(currentPrice.toFixed(4)),
        sl: sl,
        targets: finalTargets,
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
// 4. REST API ENDPOINTS (TERMASUK PROXY CANDLES UNTUK MULTI-PROVIDER)
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
        const { 
            provider = 'twelvedata', 
            symbol, 
            tf, 
            interval, 
            finnhubSymbol, 
            resolution, 
            from, 
            to, 
            isForex, 
            isDaily, 
            base, 
            quote 
        } = req.query;

        const cleanSymbol = (symbol || "").trim().toUpperCase();

        if (provider === 'twelvedata') {
            const apiKey = runtimeConfig.twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo";
            const t12Interval = interval || '15min';
            const formattedSym = formatSymbolForProvider(cleanSymbol, 'twelveData');
            const targetUrl = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${encodeURIComponent(formattedSym)}&interval=${t12Interval}&outputsize=200&apikey=${apiKey}`;
            
            const response = await axios.get(targetUrl);
            const data = response.data;

            if (data && (data.code === 400 || data.status === 'error')) {
                const errMessage = (data.message || "").toLowerCase();
                if (errMessage.includes('symbol not found') || errMessage.includes('does not exist') || errMessage.includes('not supported')) {
                    return res.json({ success: false, isInvalidSymbol: true, message: data.message });
                }
            }
            return res.json(data);

        } else if (provider === 'finnhub') {
            const apiKey = runtimeConfig.finnhubKey || process.env.FINNHUB_KEY || "c8888888888888888888";
            const isFx = isForex === 'true';
            let targetUrl = isFx 
                ? `${PROVIDERS.finnhub.baseUrl}/forex/candle?symbol=OANDA:${finnhubSymbol || cleanSymbol}&resolution=${resolution || '15'}&from=${from}&to=${to}&token=${apiKey}`
                : `${PROVIDERS.finnhub.baseUrl}/stock/candle?symbol=${cleanSymbol}&resolution=${resolution || '15'}&from=${from}&to=${to}&token=${apiKey}`;

            const response = await axios.get(targetUrl);
            const data = response.data;

            if (data && data.s === 'no_data') {
                return res.json({ success: false, isInvalidSymbol: true, message: "No data from Finnhub" });
            }
            return res.json(data);

        } else if (provider === 'alphavantage') {
            const apiKey = runtimeConfig.alphaVantageKey || process.env.ALPHA_VANTAGE_KEY || "demo";
            const isFx = isForex === 'true';
            const daily = isDaily === 'true';
            let targetUrl = isFx
                ? (daily ? `${PROVIDERS.alphaVantage.baseUrl}?function=FX_DAILY&from_symbol=${base}&to_symbol=${quote}&outputsize=full&apikey=${apiKey}` : `${PROVIDERS.alphaVantage.baseUrl}?function=FX_INTRADAY&from_symbol=${base}&to_symbol=${quote}&interval=${interval || '15min'}&outputsize=full&apikey=${apiKey}`)
                : (daily ? `${PROVIDERS.alphaVantage.baseUrl}?function=TIME_SERIES_DAILY&symbol=${cleanSymbol}&outputsize=full&apikey=${apiKey}` : `${PROVIDERS.alphaVantage.baseUrl}?function=TIME_SERIES_INTRADAY&symbol=${cleanSymbol}&interval=${interval || '15min'}&outputsize=full&apikey=${apiKey}`);

            const response = await axios.get(targetUrl);
            const data = response.data;

            if (data && (data['Error Message'] || data['Note'])) {
                return res.json({ success: false, isInvalidSymbol: true, message: "Alpha Vantage limit or invalid symbol" });
            }
            return res.json(data);

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
            const functionType = isDaily ? 'TIME_SERIES_DAILY' : 'TIME_SERIES_INTRADAY';
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
        return {
            ...signalData,
            tierTarget: 'STANDARD',
            allowed: true,
            note: 'Dialirkan ke jalur Standar Tier'
        };
    } 
    else if (clientTier === 'premium' || clientTier === 'vip') {
        return {
            ...signalData,
            tierTarget: 'PREMIUM',
            allowed: true,
            note: 'Dialirkan ke jalur Premium Tier (Full Access)'
        };
    }
    return null;
}

// ============================================================================
// 6. WEBSOCKET SERVER & PUSAT KENDALI KELAS ENTERPRISE (DECENTRALIZED)
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
                console.log(`[WebSocket Backend] Klien mendaftarkan tier: ${ws.tier}`);
            }

            if (parsed.action === 'FORCE_TRIGGER_SCAN' || parsed.type === 'FORCE_TRIGGER_SCAN') {
                console.log('[Backend] Force Trigger dieksekusi otonom!');
                let signal = await runInstitutionalEnginePipeline("XAUUSD", "15");
                if (signal && signal.status === 'TRIGGERED') {
                    let routedSignal = routeSignalThroughTierPipeline(signal, ws.tier);
                    if (routedSignal && routedSignal.allowed) {
                        ws.send(JSON.stringify({
                            type: 'DECENTRALIZED_SIGNAL_BROADCAST',
                            signal: routedSignal
                        }));
                    }
                }
            } 
            else if (parsed.action === 'EMERGENCY_STOP' || parsed.type === 'EMERGENCY_STOP') {
                BackendState.isRunning = false;
                console.log('[Backend] EMERGENCY STOP diaktifkan oleh Developer!');
                ws.send(JSON.stringify({
                    type: 'SYSTEM_STATUS',
                    message: 'Emergency Stop berhasil diterapkan di server.'
                }));
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

function isHighImpactNewsBlackout() {
    if (!BackendState.econCalendarFilterActive) return false;
    return false; 
}

const AUTONOMOUS_INTERVAL_MS = 225000; // 3.75 Menit sesuai kesepakatan

setInterval(async () => {
    if (!BackendState.isRunning) return;
    if (isWeekend()) return;
    if (!isWithinSessionRange()) return;

    try {
        let targetSymbol = "XAUUSD";
        let signalResult = await runInstitutionalEnginePipeline(targetSymbol, "15");
        let inNewsBlackout = isHighImpactNewsBlackout();

        if (signalResult && signalResult.status === 'TRIGGERED') {
            if (inNewsBlackout) {
                console.log(`[News Guard] Sinyal ${targetSymbol} ditahan (Blackout News aktif).`);
            } else {
                console.log(`[Autonomous Engine] Sinyal ISLMS-200 & Gate 4 Valid! Menyiarkan sinyal ${targetSymbol} via WebSocket.`);
                
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
