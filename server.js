/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Full Multi-Provider Support (Twelve Data, Binance, Alpha Vantage / Fallbacks)
 * - Strict Master Registry Whitelist (25 Authorized Instruments)
 * - Zero Hardcoded Passwords & Zero Hardcoded API Keys (Strictly process.env / Dynamic Runtime)
 * - ES Module Syntax (Compatible with package.json "type": "module")
 */

import express from 'express';
import cors from 'cors';
import axios from 'axios';
import http from 'http';
import { WebSocketServer } from 'ws';

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

const PROVIDERS = {
    twelveData: {
        baseUrl: "https://api.twelvedata.com"
    },
    binance: {
        baseUrl: "https://api.binance.com/api/v3"
    },
    alphaVantage: {
        baseUrl: "https://www.alphavantage.co/query"
    }
};

// Caching layer to protect provider rate limits (15 seconds TTL)
const marketCache = new Map();
const CACHE_TTL = 15000;

// Helper function: Format penulisan simbol sesuai provider
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

// ENDPOINT: Mengambil bundel data makro global (latest_m30) dari Firestore untuk Frontend
app.get('/api/macro/latest', async (req, res) => {
    try {
        // Pastikan variabel 'db' (Firestore) sudah diinisialisasi di server.js
        if (typeof db === 'undefined' || !db) {
            return res.status(500).json({ 
                success: false, 
                message: 'Database Firestore belum diinisialisasi di server.js' 
            });
        }

        let docRef = db.collection('macro_candles').doc('latest_m30');
        let docSnap = await docRef.get();

        if (!docSnap.exists) {
            return res.status(404).json({ 
                success: false, 
                message: 'Bundel data makro latest_m30 belum tersedia.' 
            });
        }

        // Mengirimkan data ke frontend dalam format JSON
        return res.json(docSnap.data());

    } catch (error) {
        console.error('Error mengambil latest_m30:', error);
        return res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// 3. DEVELOPER AUTHENTICATION BRIDGE (Strictly via Environment Variable Render)
app.post('/api/verify-dev', (req, res) => {
    const { password } = req.body;
    const devPassword = process.env.DEV_PASSWORD;

    if (!devPassword) {
        return res.status(500).json({ success: false, message: "Konfigurasi server belum lengkap: DEV_PASSWORD belum diatur di Render." });
    }

    if (password && password.trim() === devPassword) {
        return res.json({ success: true, message: "Akses pengembang diverifikasi." });
    } else {
        return res.status(401).json({ success: false, message: "Sandi pengembang salah." });
    }
});

// 3.1 ENDPOINT PENERIMAAN KUNCI-KUNCI API DARI PANEL FRONTEND
app.post('/api/developer/config', (req, res) => {
    const { twelveApiKey, finnhubKey, alphaVantageKey, geminiApiKey, tier } = req.body;

    if (twelveApiKey !== undefined) runtimeConfig.twelveApiKey = twelveApiKey;
    if (finnhubKey !== undefined) runtimeConfig.finnhubKey = finnhubKey;
    if (alphaVantageKey !== undefined) runtimeConfig.alphaVantageKey = alphaVantageKey;
    if (geminiApiKey !== undefined) runtimeConfig.geminiApiKey = geminiApiKey;
    if (tier !== undefined) runtimeConfig.tier = tier;

    return res.json({ 
        success: true, 
        message: "Konfigurasi kunci API berhasil diperbarui di server.",
        tier: runtimeConfig.tier
    });
});

// 3.2 ENDPOINT AUTO-COMPLETE PENCARIAN INSTRUMEN (Minimal 2 Huruf)
app.get('/api/symbols/search', (req, res) => {
    const q = (req.query.q || '').trim().toUpperCase();
    if (!q || q.length < 2) {
        return res.json({ success: true, symbols: [] });
    }
    const filtered = MASTER_WHITELIST.filter(symbol => symbol.includes(q));
    res.json({ success: true, symbols: filtered });
});

// 4. STRICT VALIDATION & MARKET DATA ENDPOINT
app.post('/api/validate-symbol', (req, res) => {
    const { symbol } = req.body;
    const cleanSymbol = (symbol || "").trim().toUpperCase();
    
    if (MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.json({ valid: true, symbol: cleanSymbol });
    } else {
        return res.status(400).json({ 
            valid: false, 
            message: `Instrumen '${cleanSymbol}' tidak terdaftar di dalam Katalog Master Whitelist.` 
        });
    }
});

app.get('/api/v1/market', async (req, res) => {
    const { symbol, interval = '1h', provider = 'twelveData' } = req.query;
    const cleanSymbol = (symbol || "").trim().toUpperCase();

    if (!MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.status(403).json({ 
            error: "Access Denied", 
            message: "Simbol ditolak oleh Gatekeeper Backend. Tidak ada di Katalog Master." 
        });
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
            throw new Error("Provider tidak dikenal atau tidak didukung.");
        }

        marketCache.set(cacheKey, { timestamp: Date.now(), data: responseData });
        return res.json({ source: provider, data: responseData });

    } catch (error) {
        return res.status(500).json({ 
            error: "Provider Error", 
            message: "Gagal mengambil data dari provider eksternal.",
            details: error.message 
        });
    }
});

app.post('/api/sync', (req, res) => {
    const tradeRecord = req.body;
    return res.json({ success: true, message: 'Rekam jejak sinyal berhasil disinkronkan ke server.', data: tradeRecord });
});

// 5. WEBSOCKET PROXY FOR REAL-TIME STREAMING
wss.on('connection', (ws) => {
    console.log('[WebSocket] Klien frontend terhubung.');

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            ws.send(JSON.stringify({ status: 'subscribed', payload: parsed }));
        } catch (e) {
            ws.send(JSON.stringify({ error: 'Format pesan tidak valid' }));
        }
    });

    ws.on('close', () => {
        console.log('[WebSocket] Klien terputus.');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`HybriOX Backend aktif dan berjalan pada port ${PORT}`);
});
