const admin = require('firebase-admin');
const axios = require('axios');

// ==========================================
// 1. LOGIKA ISTIRAHAT MALAM (00.00 - 05.30 WIB)
// ==========================================
const now = new Date();
const utcHours = now.getUTCHours();
const utcMinutes = now.getUTCMinutes();
// Konversi ke Waktu Indonesia Barat (WIB = UTC + 7)
let wibHours = (utcHours + 7) % 24;
let totalMinutesWIB = wibHours * 60 + utcMinutes;
// 00.00 WIB = 0 menit, 05.30 WIB = (5 * 60) + 30 = 330 menit
if (totalMinutesWIB >= 0 && totalMinutesWIB < 330) {
    console.log("Waktu istirahat malam (00.00 - 05.30 WIB). Skrip melewati eksekusi untuk menghemat kuota API.");
    process.exit(0);
}

// ==========================================
// 2. INISIALISASI FIREBASE FIRESTORE
// ==========================================
if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
    console.error("Error: FIREBASE_SERVICE_ACCOUNT belum diset di GitHub Secrets.");
    process.exit(1);
}

try {
    const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount)
    });
} catch (error) {
    console.error("Gagal memparsing FIREBASE_SERVICE_ACCOUNT:", error.message);
    process.exit(1);
}

const db = admin.firestore();

// ==========================================
// 3. DAFTAR 5 PAIR & KONFIGURASI 3 PROVIDER
// ==========================================
const TARGET_PAIRS = ['XAUUSD', 'EURUSD', 'GBPUSD', 'GBPJPY', 'AUDUSD'];

// Provider 1: Twelve Data
async function fetchTwelveData(symbol, apiKey) {
    if (!apiKey) return null;
    const formattedSymbol = symbol.length === 6 ? `${symbol.slice(0, 3)}/${symbol.slice(3)}` : symbol;
    try {
        const res = await axios.get(`https://api.twelvedata.com/time_series`, {
            params: {
                symbol: formattedSymbol,
                interval: '30min',
                outputsize: 400,
                apikey: apiKey
            },
            timeout: 10000
        });
        if (res.data && res.data.values && res.data.values.length > 0) {
            return res.data.values.slice(0, 400);
        }
    } catch (err) {
        console.warn(`[Twelve Data] Gagal untuk ${symbol}: ${err.message}`);
    }
    return null;
}

// Provider 2: Alpha Vantage (Fallback 1)
async function fetchAlphaVantage(symbol, apiKey) {
    if (!apiKey) return null;
    const fromSymbol = symbol.slice(0, 3);
    const toSymbol = symbol.slice(3);
    try {
        const res = await axios.get(`https://www.alphavantage.co/query`, {
            params: {
                function: 'FX_INTRADAY',
                from_symbol: fromSymbol,
                to_symbol: toSymbol,
                interval: '30min',
                outputsize: 'full',
                apikey: apiKey
            },
            timeout: 10000
        });
        const key = 'Time Series FX (30min)';
        if (res.data && res.data[key]) {
            const raw = res.data[key];
            return Object.keys(raw).slice(0, 400).map(time => ({
                datetime: time,
                open: raw[time]['1. open'],
                high: raw[time]['2. high'],
                low: raw[time]['3. low'],
                close: raw[time]['4. close'],
                volume: raw[time]['5. volume'] || '0'
            }));
        }
    } catch (err) {
        console.warn(`[Alpha Vantage] Gagal untuk ${symbol}: ${err.message}`);
    }
    return null;
}

// Provider 3: Pin Hub (Fallback 2)
async function fetchPinHub(symbol, apiKey) {
    if (!apiKey) return null;
    try {
        const res = await axios.get(`https://api.pinhub.com/v1/candles`, {
            params: {
                symbol: symbol,
                interval: '30min',
                limit: 400,
                apiKey: apiKey
            },
            timeout: 10000
        });
        if (res.data && res.data.candles) {
            return res.data.candles.slice(0, 400);
        }
    } catch (err) {
        console.warn(`[Pin Hub] Gagal untuk ${symbol}: ${err.message}`);
    }
    return null;
}

// ==========================================
// 4. EKSKUSI UTAMA (FAILOVER & ROLLING OVERWRITE)
// ==========================================
async function main() {
    console.log(`Memulai siklus fetcher M30 untuk ${TARGET_PAIRS.length} pair...`);

    const twelveKey = process.env.TWELVE_DATA_API_KEY;
    const alphaKey = process.env.ALPHA_VANTAGE_API_KEY;
    const pinhubKey = process.env.PINHUB_API_KEY;

    const payloadData = {};

    for (const pair of TARGET_PAIRS) {
        console.log(`\n--- Memproses Pair: ${pair} ---`);
        let candles = null;
        let activeProvider = '';

        // Urutan Failover: Twelve Data -> Alpha Vantage -> Pin Hub
        candles = await fetchTwelveData(pair, twelveKey);
        if (candles) {
            activeProvider = 'Twelve Data';
        } else {
            console.log(`Mencoba Fallback ke Alpha Vantage untuk ${pair}...`);
            candles = await fetchAlphaVantage(pair, alphaKey);
            if (candles) {
                activeProvider = 'Alpha Vantage';
            } else {
                console.log(`Mencoba Fallback ke Pin Hub untuk ${pair}...`);
                candles = await fetchPinHub(pair, pinhubKey);
                if (candles) {
                    activeProvider = 'Pin Hub';
                }
            }
        }

        if (candles && candles.length > 0) {
            payloadData[pair] = {
                provider: activeProvider,
                count: candles.length,
                candles: candles
            };
            console.log(`[SUKSES] ${pair}: Berhasil mengambil ${candles.length} candle via ${activeProvider}`);
        } else {
            console.error(`[GAGAL TOTAL] Semua provider gagal mengambil data untuk ${pair}`);
        }
    }

    try {
        // ROLLING WINDOW: Overwrite dokumen 'latest_m30' di Firestore agar konsisten 400 candle
        await db.collection('macro_candles').doc('latest_m30').set({
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            status: 'Rolling Window Active (400 Candles Overwrite)',
            pairs: TARGET_PAIRS,
            data: payloadData
        }, { merge: false }); // merge: false memastikan penimpaan bersih (overwrite total)

        console.log("\n====================================================");
        console.log("SUKSES: Firestore (latest_m30) berhasil diperbarui secara rolling!");
        console.log("====================================================");
    } catch (error) {
        console.error("Gagal memperbarui Firestore:", error.message);
        process.exit(1);
    }
}

main();
