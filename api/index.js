const express = require('express');
const app = express();

// Memakai file storage & mikrotik asli milik kamu
const storage = require('../config/storage');
const mikrotik = require('../config/mikrotik');
const { RouterOSClient } = require('routeros-client');

app.use(express.json());

// Helper koneksi MikroTik via Tunnel.id untuk ambil traffic
async function getMikrotikTraffic() {
    // TAMBAHAN: Mendukung variabel Vercel MIKROTIK_IP & MIKROTIK_HOST sekaligus
    const host = process.env.MIKROTIK_IP || process.env.MIKROTIK_HOST;
    const port = process.env.MIKROTIK_API_PORT || process.env.MIKROTIK_PORT || '8728';
    const user = process.env.MIKROTIK_API_USER || process.env.MIKROTIK_USER;
    const password = process.env.MIKROTIK_API_PASSWORD || process.env.MIKROTIK_PASSWORD;

    if (!host) {
        console.error("Host/IP MikroTik tidak ditemukan di Environment Variables");
        return { mtUsers: [], mtActive: [] };
    }

    const client = new RouterOSClient({
        host: host,
        port: parseInt(port),
        user: user,
        password: password,
        timeout: 5
    });

    try {
        const api = await client.connect();
        const mtUsers = await api.menu('/ip/hotspot/user').get();
        const mtActive = await api.menu('/ip/hotspot/active').get();
        await client.close();
        return { mtUsers, mtActive };
    } catch (err) {
        console.error("Gagal terhubung ke MikroTik API:", err.message);
        return { mtUsers: [], mtActive: [] };
    }
}

// 1. ENDPOINT GET USERS + TRAFFIC MIKROTIK
app.get('/api/users', async (req, res) => {
    try {
        // Ambil data user dari users.json lewat storage.js kamu
        const users = await storage.getUsers(); 

        // Ambil data live traffic dari MikroTik
        const { mtUsers, mtActive } = await getMikrotikTraffic();

        // Gabungkan traffic ke data user kamu
        const responseUsers = users.map(user => {
            const mtUser = mtUsers.find(u => u.name && u.name.toLowerCase() === user.username.toLowerCase());
            const activeUser = mtActive.find(u => u.user && u.user.toLowerCase() === user.username.toLowerCase());

            return {
                ...user,
                bytesIn: mtUser ? parseInt(mtUser['bytes-in'] || 0) : 0,   // Upload
                bytesOut: mtUser ? parseInt(mtUser['bytes-out'] || 0) : 0, // Download
                uptime: activeUser ? `${activeUser.uptime} (Online)` : (mtUser ? (mtUser.uptime || 'Off') : 'Off')
            };
        });

        res.json({ success: true, users: responseUsers });
    } catch (error) {
        res.status(500).json({ success: false, message: 'Gagal mengambil data user' });
    }
});

// 2. ENDPOINT APPROVE (Pakai logika storage asli kamu)
app.post('/api/approve', async (req, res) => {
    try {
        const { username } = req.body;
        if (storage.approveUser) {
            await storage.approveUser(username);
        }
        res.json({ success: true, message: `User ${username} berhasil diapprove` });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// 3. ENDPOINT REJECT (Pakai logika storage asli kamu)
app.post('/api/reject', async (req, res) => {
    try {
        const { username } = req.body;
        if (storage.rejectUser) {
            await storage.rejectUser(username);
        }
        res.json({ success: true, message: `User ${username} berhasil direject` });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

// 4. ENDPOINT DELETE (Pakai logika storage asli kamu agar users.json terupdate)
app.delete('/api/users/:username', async (req, res) => {
    try {
        const { username } = req.params;
        if (storage.deleteUser) {
            await storage.deleteUser(username);
        }
        res.json({ success: true, message: `User ${username} berhasil dihapus` });
    } catch (err) {
        res.status(500).json({ success: false, message: err.message });
    }
});

module.exports = app;