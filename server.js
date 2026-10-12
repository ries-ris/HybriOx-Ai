/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Full Multi-Provider Support (Twelve Data, Alpha Vantage, Finnhub) + Google Gemini Proxy
 * - Strict Master Registry Whitelist (25 Authorized Instruments)
 * - Developer Panel Authentication & Master Control Sync
 * - Dynamic On-Demand API Key & VIP Tier Routing Mechanics
 */

const express = require('express');
const cors = require('cors');
const axios = require('axios');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

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

// Provider Base URLs
const PROVIDER_URLS = {
    twelveData: "https://api.twelvedata.com",
    alphaVantage: "https://www.alphavantage.co/query",
    finnhub: "https://finnhub.io/api/v1",
    gemini: "https://generativelanguage.googleapis.com/v1beta"
};

// Caching Layer (15 Seconds TTL for Market Data)
const marketCache = new Map();
const CACHE_TTL = 15000;

// Health Check Route
app.get('/', (req, res) => {
    res.status(200).send('HybriOX Master Dynamic Proxy Backend is Online & Fully Operational.');
});

// 2. DEVELOPER PANEL AUTHENTICATION ENDPOINT
app.post('/api/verify-dev', (req, res) => {
    const { password } = req.body;
    const devSecret = process.env.DEV_SECRET_PASSWORD || "admin123";
    
    if (password === devSecret) {
        return res.status(200).json({ success: true, message: "Akses Developer Panel diberikan." });
    } else {
        return res.status(401).json({ success: false, message: "Sandi salah." });
    }
});

// 3. STRICT SYMBOL VALIDATION ENDPOINT
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

// 4. DYNAMIC MULTI-PROVIDER MARKET DATA PROXY (Supports On-Demand Keys)
app.get('/api/v1/market', async (req, res) => {
    const { symbol, interval = '1h', provider = 'twelveData', apiKey, tier = 'free' } = req.query;
    const cleanSymbol = (symbol || "").trim().toUpperCase();

    // Early Exit: Strict Whitelist Check
    if (!MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.status(403).json({ 
            error: "Access Denied", 
            message: "Simbol ditolak oleh Gatekeeper Backend. Tidak ada di Katalog Master." 
        });
    }

    // Resolusi API Key: Prioritaskan On-Demand dari Frontend -> Fallback ke Environment Server Render
    const activeApiKey = apiKey || process.env[`${provider.toUpperCase()}_API_KEY`] || "demo";

    const cacheKey = `${provider}_${cleanSymbol}_${interval}_${tier}`;
    const cachedData = marketCache.get(cacheKey);
    if (cachedData && (Date.now() - cachedData.timestamp < CACHE_TTL)) {
        return res.json({ source: 'cache', data: cachedData.data });
    }

    try {
        let responseData;
        
        if (provider === 'twelveData') {
            const url = `${PROVIDER_URLS.twelveData}/time_series?symbol=${cleanSymbol}&interval=${interval}&apikey=${activeApiKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } 
        else if (provider === 'alphaVantage') {
            const url = `${PROVIDER_URLS.alphaVantage}?function=TIME_SERIES_INTRADAY&symbol=${cleanSymbol}&interval=${interval}&apikey=${activeApiKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } 
        else if (provider === 'finnhub') {
            const url = `${PROVIDER_URLS.finnhub}/quote?symbol=${cleanSymbol}&token=${activeApiKey}`;
            const response = await axios.get(url);
            responseData = response.data;
        } 
        else {
            throw new Error("Provider pasar tidak dikenal.");
        }

        marketCache.set(cacheKey, { timestamp: Date.now(), data: responseData });
        return res.json({ source: provider, tier: tier, data: responseData });

    } catch (error) {
        return res.status(500).json({ 
            error: "Provider Error", 
            message: "Gagal mengambil data dari provider eksternal.",
            details: error.message 
        });
    }
});

// 5. GOOGLE GEMINI AI PROXY ENDPOINT (Supports Master/On-Demand Sync)
app.post('/api/v1/gemini', async (req, res) => {
    const { prompt, apiKey } = req.body;
    const activeGeminiKey = apiKey || process.env.GEMINI_API_KEY;

    if (!activeGeminiKey) {
        return res.status(400).json({ error: "API Key Gemini tidak ditemukan (baik dari panel developer maupun on-demand)." });
    }

    try {
        const url = `${PROVIDER_URLS.gemini}/models/gemini-1.5-flash:generateContent?key=${activeGeminiKey}`;
        const response = await axios.post(url, {
            contents: [{ parts: [{ text: prompt }] }]
        });
        return res.json({ success: true, data: response.data });
    } catch (error) {
        return res.status(500).json({ 
            error: "Gemini AI Error", 
            details: error.response?.data || error.message 
        });
    }
});

// 6. WEBSOCKET PROXY REAL-TIME STREAMING
wss.on('connection', (ws) => {
    console.log('[WebSocket] Klien terhubung ke server proxy.');

    ws.on('message', (message) => {
        try {
            const parsed = JSON.parse(message);
            ws.send(JSON.stringify({ status: 'subscribed', payload: parsed }));
        } catch (e) {
            ws.send(JSON.stringify({ error: 'Format pesan WebSocket tidak valid' }));
        }
    });

    ws.on('close', () => {
        console.log('[WebSocket] Klien terputus.');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`HybriOX Master Backend aktif dan berjalan pada port ${PORT}`);
});
