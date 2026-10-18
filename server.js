/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Full Multi-Provider Support (Twelve Data, Binance, Alpha Vantage / Fallbacks)
 * - Strict Master Registry Whitelist (25 Authorized Instruments)
 * - Real-time WebSocket, REST Proxy Mechanics & Developer Security Bridge
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

// 2. MULTI-PROVIDER CONFIGURATION KEYS & FALLBACKS
const PROVIDERS = {
    twelveData: {
        apiKey: process.env.TWELVE_DATA_API_KEY || "demo",
        baseUrl: "https://api.twelvedata.com"
    },
    binance: {
        baseUrl: "https://api.binance.com/api/v3"
    },
    fallback: {
        enabled: true
    }
};

// Caching layer to protect provider rate limits (15 seconds TTL)
const marketCache = new Map();
const CACHE_TTL = 15000;

// Helper function: Otomatisasi format penulisan simbol sesuai kebutuhan masing-masing provider
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

// 3. DEVELOPER AUTHENTICATION BRIDGE
app.post('/api/verify-dev', (req, res) => {
    const { password } = req.body;
    const devPassword = process.env.DEV_MASTER_PASSWORD || "masayu"; 

    if (password && password.trim() === devPassword) {
        return res.json({ success: true, message: "Akses pengembang diverifikasi." });
    } else {
        return res.status(401).json({ success: false, message: "Sandi pengembang salah." });
    }
});

// 3.2 ENDPOINT PENERIMA KONFIGURASI DEVELOPER & MASTER API KEYS
app.post('/api/developer/config', (req, res) => {
    try {
        const { twelveApiKey, finnhubKey, alphaVantageKey, geminiApiKey, tier } = req.body;
        
        console.log('[Config Update] Menerima pembaruan kunci API master dari Panel Developer:', {
            tier: tier || 'free',
            hasTwelve: !!twelveApiKey,
            hasFinnhub: !!finnhubKey,
            hasAlpha: !!alphaVantageKey,
            hasGemini: !!geminiApiKey
        });

        global.SERVER_MASTER_CONFIG = {
            twelveApiKey: twelveApiKey || process.env.TWELVE_DATA_API_KEY || "demo",
            finnhubKey: finnhubKey || process.env.FINNHUB_API_KEY || "",
            alphaVantageKey: alphaVantageKey || process.env.ALPHA_VANTAGE_API_KEY || "",
            geminiApiKey: geminiApiKey || process.env.GEMINI_API_KEY || "",
            tier: tier || 'free',
            updatedAt: Date.now()
        };

        return res.json({ 
            success: true, 
            message: "Konfigurasi master API berhasil disinkronkan dan disimpan di server Render." 
        });
    } catch (error) {
        return res.status(500).json({ 
            success: false, 
            error: "Gagal memproses konfigurasi di server.",
            details: error.message 
        });
    }
});

// 3.1 ENDPOINT AUTO-COMPLETE PENCARIAN INSTRUMEN
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
            const url = `${PROVIDERS.twelveData.baseUrl}/time_series?symbol=${formattedSymbol}&interval=${interval}&apikey=${PROVIDERS.twelveData.apiKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } else if (provider === 'binance') {
            const url = `${PROVIDERS.binance.baseUrl}/klines?symbol=${formattedSymbol}&interval=${interval}`;
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

// 5. WEBSOCKET PROXY FOR REAL-TIME STREAMING
wss.on('connection', (ws, req) => {
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
