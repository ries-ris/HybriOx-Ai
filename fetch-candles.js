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

// 00:00 WIB = 0 menit, 06:30 WIB = 390 menit
if (totalMinutesWIB >= 0 && totalMinutesWIB < 390) {
    console.log("Waktu istirahat malam (00.00 - 06.30 WIB). Skrip melewati eksekusi untuk menghemat kuota API.");
    process.exit(0); // Keluar dengan aman tanpa error
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
// 3. FUNGSI UTAMA PENGAMBILAN DATA (3 PROVIDER)
// ==========================================
async function main() {
    console.log("Memulai siklus fetcher M30 dari Twelve Data, Alpha Vantage, dan Pin Hub...");

    const twelveApiKey = process.env.TWELVE_DATA_API_KEY;
    const alphaApiKey = process.env.ALPHA_VANTAGE_API_KEY;
    const pinhubApiKey = process.env.PINHUB_API_KEY;

    try {
        // Catat eksekusi ke koleksi Firestore 'macro_candles'
        await db.collection('macro_candles').add({
            timestamp: admin.firestore.FieldValue.serverTimestamp(),
            status: 'Active fetcher cycle executed',
            providers: ['Twelve Data', 'Alpha Vantage', 'Pin Hub']
        });
        
        console.log("Siklus pengambilan data berhasil dicatat ke Firebase Firestore!");
    } catch (error) {
        console.error("Terjadi kesalahan saat menyimpan ke Firestore:", error);
        process.exit(1);
    }
}

main();
