const admin = require('firebase-admin');
const axios = require('axios');

// ==========================================
// 1. LOGIKA ISTIRAHAT MALAM (00.00 - 06.30 WIB)
// ==========================================
const now = new Date();
const utcHours = now.getUTCHours();
const utcMinutes = now.getUTCMinutes();
const wibHours = (utcHours + 7) % 24;
const totalMinutesWIB = wibHours * 60 + utcMinutes;

if (totalMinutesWIB >= 0 && totalMinutesWIB < 390) {
    console.log("Waktu istirahat malam (00.00 - 06.30 WIB). Skrip melewati eksekusi untuk menghemat kuota API.");
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
// 3. FUNGSI UTAMA PENGAMBILAN DATA (3 PROVIDER - 400 CANDLE)
// ==========================================
async function main() {
    console.log("Memulai siklus fetcher M30 dari Twelve Data, Alpha Vantage, dan Pin Hub...");

    const twelveApiKey = process.env.TWELVE_DATA_API_KEY;
    const alphaApiKey = process.env.ALPHA_VANTAGE_API_KEY;
    const pinhubApiKey = process.env.PINHUB_API_KEY;

    let twelveDataCandles = [];
    let alphaVantageCandles = [];
    let pinHubCandles = [];

    try {
        // 1. Fetch Twelve Data (Target 400 candles via outputsize=400)
        if (twelveApiKey) {
            console.log("Mengambil data dari Twelve Data...");
            const twelveRes = await axios.get(`https://api.twelvedata.com/time_series`, {
                params: {
                    symbol: 'EUR/USD',
                    interval: '30min',
                    outputsize: 400,
                    apikey: twelveApiKey
                }
            });
            if (twelveRes.data && twelveRes.data.values) {
                twelveDataCandles = twelveRes.data.values;
                console.log(`Berhasil mengambil ${twelveDataCandles.length} candles dari Twelve Data.`);
            }
        }

        // 2. Fetch Alpha Vantage (Target 400 candles)
        if (alphaApiKey) {
            console.log("Mengambil data dari Alpha Vantage...");
            const alphaRes = await axios.get(`https://www.alphavantage.co/query`, {
                params: {
                    function: 'FX_INTRADAY',
                    from_symbol: 'EUR',
                    to_symbol: 'USD',
                    interval: '30min',
                    outputsize: 'full',
                    apikey: alphaApiKey
                }
            });
            const timeSeriesKey = 'Time Series FX (30min)';
            if (alphaRes.data && alphaRes.data[timeSeriesKey]) {
                const rawData = alphaRes.data[timeSeriesKey];
                alphaVantageCandles = Object.keys(rawData).slice(0, 400).map(time => ({
                    datetime: time,
                    ...rawData[time]
                }));
                console.log(`Berhasil memproses ${alphaVantageCandles.length} candles dari Alpha Vantage.`);
            }
        }

        // 3. Fetch Pin Hub
        if (pinhubApiKey) {
            console.log("Mengambil data dari Pin Hub...");
            const pinHubRes = await axios.get(`https://api.pinhub.com/v1/candles`, {
                params: {
                    symbol: 'EURUSD',
                    interval: '30min',
                    limit: 400,
                    apiKey: pinhubApiKey
                }
            });
            if (pinHubRes.data && pinHubRes.data.candles) {
                pinHubCandles = pinHubRes.data.candles.slice(0, 400);
                console.log(`Berhasil mengambil ${pinHubCandles.length} candles dari Pin Hub.`);
            }
        }

        // Simpan semua data hasil fetch ke Firestore
        await db.collection('macro_candles').add({
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
            status: 'Success fetch 400 candles',
            providers: ['Twelve Data', 'Alpha Vantage', 'Pin Hub'],
            data: {
                twelveData: twelveDataCandles,
                alphaVantage: alphaVantageCandles,
                pinHub: pinHubCandles
            }
        });
        
        console.log("Data 400 candles dari ketiga provider berhasil disimpan ke Firebase Firestore!");
    } catch (error) {
        console.error("Terjadi kesalahan saat mengambil atau menyimpan data:", error.message);
        process.exit(1);
    }
}

main();
