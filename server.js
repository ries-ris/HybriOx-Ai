/**
 * HybriOX Professional-Grade Backend Proxy & Asset Management Server
 * - Transparent Multi-Provider Dynamic Proxy (Twelve Data, Alpha Vantage, Finnhub)
 * - Strict Master Registry Whitelist (25 Authorized Instruments)
 * - Supports On-Demand API Keys & Developer Panel Integration from Frontend
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

// Base URLs for Providers
const PROVIDER_URLS = {
    twelveData: "https://api.twelvedata.com",
    alphaVantage: "https://www.alphavantage.co/query",
    finnhub: "https://finnhub.io/api/v1"
};

// Caching layer
const marketCache = new Map();
const CACHE_TTL = 15000;

// Health Check Route
app.get('/', (req, res) => {
    res.status(200).send('HybriOX Dynamic Proxy Backend is Online.');
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

// 3. STRICT VALIDATION ENDPOINT
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

// 4. DYNAMIC MULTI-PROVIDER MARKET DATA PROXY (Supports On-Demand API Keys)
app.get('/api/v1/market', async (req, res) => {
    const { symbol, interval = '1h', provider = 'twelveData', apiKey } = req.query;
    const cleanSymbol = (symbol || "").trim().toUpperCase();

    // Early Exit: Strict Whitelist Check
    if (!MASTER_WHITELIST.includes(cleanSymbol)) {
        return res.status(403).json({ 
            error: "Access Denied", 
            message: "Simbol ditolak oleh Gatekeeper Backend. Tidak ada di Katalog Master." 
        });
    }

    // Gunakan API Key yang dikirim on-demand dari frontend, atau fallback ke environment server jika ada
    const activeApiKey = apiKey || process.env[`${provider.toUpperCase()}_API_KEY`] || "demo";

    const cacheKey = `${provider}_${cleanSymbol}_${interval}`;
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
            throw new Error("Provider tidak dikenal.");
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

// 5. WEBSOCKET PROXY
wss.on('connection', (ws) => {
    console.log('[WebSocket] Klien terhubung.');

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
    console.log(`HybriOX Backend Proxy aktif pada port ${PORT}`);
});
