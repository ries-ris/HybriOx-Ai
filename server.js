const express = require('express');
const axios = require('axios');
const cors = require('cors');
const http = require('http');
const WebSocket = require('ws');

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// API Key Utama Anda disimpan aman di server (Environment Variable / Hardcoded privat)
// Pengguna umum tidak akan pernah bisa melihat kunci ini dari HP mereka.
const MASTER_TWELVE_DATA_KEY = process.env.TWELVE_DATA_API_KEY || "demo";

// Memori Caching sementara untuk Twelve Data (Mencegah Rate Limit / Kuota Habis)
let marketCache = {};

// --- ENDPOINT 1: PROXY REST API (Forex, Metal, Indeks via Twelve Data) ---
app.get('/api/v1/market', async (req, res) => {
    const { symbol, interval, mode } = req.query;

    // Validasi Mode Premium (Contoh sederhana token verifikasi)
    if (mode === 'premium') {
        // Di sini Anda bisa menambahkan logika cek database/token langganan aktif
        console.log("⚡ Akses Premium Terverifikasi di Server.");
    }

    if (!symbol || !interval) {
        return res.status(400).json({ status: "error", message: "Parameter symbol dan interval wajib diisi." });
    }

    const cacheKey = `${symbol}_${interval}`;
    const now = Date.now();

    // Jika data ada di cache dan umurnya belum 15 detik, berikan data cache (Hemat Kuota)
    if (marketCache[cacheKey] && (now - marketCache[cacheKey].timestamp < 15000)) {
        return res.json({ status: "ok", source: "cache", values: marketCache[cacheKey].data });
    }

    try {
        const response = await axios.get(`https://api.twelvedata.com/time_series`, {
            params: {
                symbol: symbol,
                interval: interval,
                outputsize: 200,
                apikey: MASTER_TWELVE_DATA_KEY
            }
        });

        if (response.data.status === "ok" && response.data.values) {
            // Simpan ke cache
            marketCache[cacheKey] = {
                timestamp: now,
                data: response.data.values
            };
            return res.json({ status: "ok", source: "live_server", values: response.data.values });
        } else {
            // TOLAK TEGAS: Jika gagal dari Twelve Data, jangan buat data palsu! Kirim error transparan.
            return res.status(402).json({ 
                status: "error", 
                message: response.data.message || "Gagal menarik data riil dari bursa. Kuota/Koneksi terbatas." 
            });
        }
    } catch (error) {
        return res.status(500).json({ status: "error", message: "Koneksi Backend Gagal: " + error.message });
    }
});

// --- ENDPOINT 2: WEBSOCKET PROXY (Binance Crypto - Bypass Blokir ISP Indonesia) ---
wss.on('connection', (ws, req) => {
    // Klien frontend terhubung dengan menyertakan symbol, cth: ws://server-anda/ws?symbol=btcusdt
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const symbol = urlParams.get('symbol') || 'btcusdt';

    console.log(`🔗 Klien terhubung ke Binance Proxy untuk instrumen: ${symbol}`);

    // Server kita membuka koneksi langsung ke Binance (server cloud tidak diblokir)
    const binanceWs = new WebSocket(`wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@ticker`);

    binanceWs.on('message', (message) => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message); // Teruskan data real-time ke HP pengguna
        }
    });

    binanceWs.on('error', (err) => {
        console.error("Binance WS Error:", err.message);
    });

    ws.on('close', () => {
        binanceWs.close();
        console.log(`🔌 Klien terputus dari instrumen: ${symbol}`);
    });
});

// Jalankan Server di Port dinamis (Cloud ready)
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 HybriOX Backend Proxy aktif dan berjalan di port ${PORT}`);
});
app.post('/api/verify-dev', (req, res) => {
    const { password } = req.body;
    if (password === process.env.DEV_SECRET_PASSWORD) {
        return res.status(200).json({ success: true, message: "Sandi pengembang benar" });
    } else {
        return res.status(401).json({ success: false, message: "Sandi salah" });
    }
});
