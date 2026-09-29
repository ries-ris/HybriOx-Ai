// ui-components.js - Modul Dropdown, Modal & Tampilan (Terintegrasi Backend Render)

let soundEnabled = true;
let vibrateEnabled = true;
let devTapCount = 0;
let DEFAULT_BACKEND_URL = "https://hybriox-ai.onrender.com";

function toggleTheme() {
    document.body.classList.toggle('light-mode');
    let isLight = document.body.classList.contains('light-mode');
    let btnAuth = document.getElementById('themeToggleBtnAuth');
    let btnTerm = document.getElementById('themeToggleBtn');
    let labelText = isLight ? "🌙" : "☀️";
    if (btnAuth) btnAuth.innerText = labelText;
    if (btnTerm) btnTerm.innerText = labelText;
    if (typeof initTradingView === 'function') {
        initTradingView(currentSymbol, currentTf);
    }
}

function toggleLoginDropdown() {
    const dropdown = document.getElementById('login-dropdown');
    const arrow = document.getElementById('login-arrow');
    dropdown.classList.toggle('hidden');
    arrow.style.transform = dropdown.classList.contains('hidden') ? 'rotate(0deg)' : 'rotate(180deg)';
}

function togglePasswordVisibility(inputId, btnEl) {
    let input = document.getElementById(inputId);
    if (input.type === "password") { input.type = "text"; btnEl.innerText = "🙈"; }
    else { input.type = "password"; btnEl.innerText = "👁️"; }
}

function openForgotModal() { document.getElementById('forgot-modal').classList.remove('hidden'); }
function closeForgotModal() { document.getElementById('forgot-modal').classList.add('hidden'); }
function handleForgotSubmit(e) {
    e.preventDefault();
    alert(`Tautan pemulihan sandi berhasil dikirim ke: ${document.getElementById('forgot-email').value}`);
    closeForgotModal();
}

function openRegisterModal() { document.getElementById('register-modal').classList.remove('hidden'); }
function closeRegisterModal() { document.getElementById('register-modal').classList.add('hidden'); }

function openSettingsModal() {
    document.getElementById('setting-email').value = userProfile.email;
    document.getElementById('setting-account-tier').value = userProfile.tier || 'free';
    document.getElementById('setting-twelve-key').value = userProfile.twelveApiKey || '';
    document.getElementById('setting-finnhub-key').value = userProfile.finnhubKey || '';
    document.getElementById('setting-alphavantage-key').value = userProfile.alphaVantageKey || '';
    document.getElementById('setting-gemini-key').value = userProfile.geminiApiKey || '';
    document.getElementById('setting-backend-url').value = userProfile.backendUrl || DEFAULT_BACKEND_URL;
    
    handleTierChange(userProfile.tier || 'free');
    document.getElementById('devSettingsContainer').classList.add('hidden');
    document.getElementById('devPasswordPrompt').classList.remove('hidden');
    document.getElementById('devUrlConfigBox').classList.add('hidden');
    document.getElementById('devSecretPassInput').value = '';
    devTapCount = 0;
    document.getElementById('settings-modal').classList.remove('hidden');
}

function closeSettingsModal() { document.getElementById('settings-modal').classList.add('hidden'); }

function handleTierChange(tierVal) {
    let freeGroup = document.getElementById('freeTierSettingsGroup');
    let premiumGroup = document.getElementById('premiumTierInfoGroup');
    if (tierVal === 'premium') { freeGroup.classList.add('hidden'); premiumGroup.classList.remove('hidden'); }
    else { freeGroup.classList.remove('hidden'); premiumGroup.classList.add('hidden'); }
}

function handleDevSecretTap() {
    devTapCount++;
    if (devTapCount >= 5) {
        document.getElementById('devSettingsContainer').classList.remove('hidden');
        alert("🔓 Panel Pengembang Aktif.");
        devTapCount = 0;
    }
}

// Verifikasi sandi pengembang secara aman melalui Backend Render
async function verifyDevPassword() {
    let inputPassword = document.getElementById('devSecretPassInput').value.trim();
    
    if (!inputPassword) {
        alert("❌ Silakan masukkan sandi terlebih dahulu!");
        return;
    }

    try {
        let backendUrl = userProfile.backendUrl || DEFAULT_BACKEND_URL;
        
        let response = await fetch(`${backendUrl}/api/verify-dev`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ password: inputPassword })
        });

        let data = await response.json();

        if (response.ok && data.success) {
            alert("✅ Sandi Pengembang Benar!");
            document.getElementById('devPasswordPrompt').classList.add('hidden');
            document.getElementById('devUrlConfigBox').classList.remove('hidden');
        } else {
            alert("❌ Sandi Salah!");
        }
    } catch (error) {
        console.error("Gagal menghubungi server:", error);
        alert("❌ Terjadi kesalahan koneksi ke server backend.");
    }
}

function saveSettings() {
    userProfile.tier = document.getElementById('setting-account-tier').value;
    userProfile.twelveApiKey = document.getElementById('setting-twelve-key').value.trim();
    userProfile.finnhubKey = document.getElementById('setting-finnhub-key').value.trim();
    userProfile.alphaVantageKey = document.getElementById('setting-alphavantage-key').value.trim();
    userProfile.geminiApiKey = document.getElementById('setting-gemini-key').value.trim();
    
    let customBackendUrl = document.getElementById('setting-backend-url').value.trim();
    if (customBackendUrl) userProfile.backendUrl = customBackendUrl;

    localStorage.setItem('ai_terminal_account', JSON.stringify(userProfile));
    document.getElementById('user-info-badge').innerText = `Akun: ${userProfile.email} (${userProfile.tier.toUpperCase()})`;
    closeSettingsModal();
    alert('Pengaturan berhasil disimpan!');
}

function openTermsScreen() { document.getElementById('terms-screen').classList.remove('hidden'); }
function closeTermsScreen() { document.getElementById('terms-screen').classList.add('hidden'); }

function switchMode(mode) {
    activeMode = mode;
    let btnStandalone = document.getElementById('modeStandalone');
    let btnWebBridge = document.getElementById('modeWebBridge');
    let execPanel = document.getElementById('executionPanel');

    if (mode === 'standalone') {
        btnStandalone.className = "flex-1 p-2.5 text-xs font-bold border border-green-600 bg-green-600 text-black rounded-xl transition-all shadow-md";
        btnWebBridge.className = "flex-1 p-2.5 text-xs font-bold border border-neutral-800 bg-neutral-900 text-neutral-400 rounded-xl transition-all hover:text-white";
        execPanel.classList.add('hidden');
    } else {
        btnWebBridge.className = "flex-1 p-2.5 text-xs font-bold border border-yellow-600 bg-yellow-500 text-black rounded-xl transition-all shadow-md";
        btnStandalone.className = "flex-1 p-2.5 text-xs font-bold border border-neutral-800 bg-neutral-900 text-neutral-400 rounded-xl transition-all hover:text-white";
        if (isRunning) execPanel.classList.remove('hidden');
    }
}

function handleBridgeOrderTypeChange(val) {
    let gridPanel = document.getElementById('gridConfigSubPanel');
    let trailPanel = document.getElementById('trailingConfigSubPanel');
    let stdBtns = document.getElementById('standardOrderButtons');

    gridPanel.classList.add('hidden');
    trailPanel.classList.add('hidden');
    stdBtns.classList.add('hidden');

    if (val === 'grid') gridPanel.classList.remove('hidden');
    else if (val === 'trailing') trailPanel.classList.remove('hidden');
    else stdBtns.classList.add('hidden');
}

function executeOrder(type) { alert(`✅ Eksekusi ${type} Berhasil dikirim melalui Web Bridge untuk instrumen ${currentSymbol}`); }
function executeSpotGrid() { alert(`🤖 Spot Grid Bot diaktifkan untuk ${currentSymbol}`); }
function executeTrailingStop() { alert(`📈 Trailing Stop diaktifkan untuk ${currentSymbol}`); }

function setTimeframe(tf, btnElement) {
    currentTf = tf;
    document.querySelectorAll('.tf-btn').forEach(b => {
        b.className = "tf-btn bg-neutral-950 border border-neutral-800 text-neutral-400 text-[10px] font-bold py-1.5 rounded-lg hover:bg-neutral-800 hover:text-white transition-all";
    });
    btnElement.className = "tf-btn active bg-green-600 text-black border border-green-500 text-[10px] font-bold py-1.5 rounded-lg transition-all shadow";
    initTradingView(currentSymbol, tf);
}

function toggleAudio() {
    soundEnabled = !soundEnabled;
    document.getElementById('audioToggleBtn').innerText = soundEnabled ? "🔊" : "🔇";
}

function toggleVibration() {
    vibrateEnabled = !vibrateEnabled;
    document.getElementById('vibrateToggleBtn').innerText = vibrateEnabled ? "📳" : "📴";
}

function triggerNotificationFeedback(type) {
    if (soundEnabled) {
        try {
            let ctx = new (window.AudioContext || window.webkitAudioContext)();
            let osc = ctx.createOscillator();
            let gain = ctx.createGain();
            osc.type = type === 'buy' ? 'sine' : 'triangle';
            osc.frequency.setValueAtTime(type === 'buy' ? 587.33 : 440, ctx.currentTime);
            gain.gain.setValueAtTime(0.1, ctx.currentTime);
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start();
            osc.stop(ctx.currentTime + 0.2);
        } catch(e) {}
    }
    if (vibrateEnabled && navigator.vibrate) navigator.vibrate(type === 'buy' ? [100, 50, 100] : [200]);
}
